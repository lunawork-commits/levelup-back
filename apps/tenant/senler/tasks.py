"""
Celery tasks for automatic VK broadcasts.

Schedule (main/celery.py beat_schedule):
  send-birthday-broadcasts  — daily at 10:00 Moscow time
  send-after-game-broadcast — every 15 minutes (09:00–21:00 window enforced in task)
  send-after-game-morning   — daily at 09:00, handles yesterday-evening games
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

import pytz
from celery import shared_task
from django.utils import timezone

logger = logging.getLogger(__name__)

_MSK = pytz.timezone('Europe/Moscow')


# ── Birthday broadcasts ───────────────────────────────────────────────────────

@shared_task(name='apps.tenant.senler.tasks.send_birthday_broadcasts_task')
def send_birthday_broadcasts_task() -> dict:
    """
    Daily task at 10:00: sends birthday VK messages for 3 triggers per tenant.

    Trigger  → send date relative to today
    birthday_7d → birthday is in 7 days
    birthday_1d → birthday is in 1 day
    birthday    → birthday is today

    Deduplication: each (trigger_type, vk_id) is sent at most once per calendar year.
    """
    from django_tenants.utils import get_tenant_model, schema_context
    from apps.tenant.branch.models import ClientBranch
    from apps.tenant.senler.models import (
        AutoBroadcastLog, AutoBroadcastTemplate, AutoBroadcastType,
        BroadcastSend, SendStatus, TriggerType,
    )
    from apps.tenant.senler.services import send_vk_message, upload_vk_photo

    today = timezone.now().astimezone(_MSK).date()

    triggers = [
        (AutoBroadcastType.BIRTHDAY_7_DAYS, today + timedelta(days=7)),
        (AutoBroadcastType.BIRTHDAY_1_DAY,  today + timedelta(days=1)),
        (AutoBroadcastType.BIRTHDAY,        today),
    ]

    TenantModel = get_tenant_model()
    total_sent = 0

    for tenant in TenantModel.objects.exclude(schema_name='public'):
        try:
            with schema_context(tenant.schema_name):
                # Load active templates for this tenant
                templates = {
                    t.type: t
                    for t in AutoBroadcastTemplate.objects.filter(
                        type__in=[tt for tt, _ in triggers],
                        is_active=True,
                    )
                }
                if not templates:
                    continue

                for trigger_type, target_date in triggers:
                    template = templates.get(trigger_type)
                    if not template:
                        continue

                    # Guests whose birthday month/day matches target_date
                    # birth_date_set_at__lte: дата ДР должна быть установлена
                    # не менее 30 дней назад (защита от злоупотреблений)
                    candidates = (
                        ClientBranch.objects
                        .filter(
                            is_employee=False,
                            client__is_active=True,
                            client__vk_id__isnull=False,
                            birth_date__month=target_date.month,
                            birth_date__day=target_date.day,
                            birth_date_set_at__lte=today - timedelta(days=30),
                        )
                        .select_related('client', 'branch')
                    )

                    if not candidates.exists():
                        continue

                    # Already sent this trigger this calendar year
                    already_sent = set(
                        AutoBroadcastLog.objects
                        .filter(trigger_type=trigger_type, sent_at__year=today.year)
                        .values_list('vk_id', flat=True)
                    )

                    # Create BroadcastSend record for this run
                    bs = BroadcastSend.objects.create(
                        auto_broadcast_template=template,
                        status=SendStatus.RUNNING,
                        trigger_type=TriggerType.AUTO,
                        triggered_by=trigger_type,
                        started_at=timezone.now(),
                    )

                    # Cache image uploads per VK community (senler_cfg.pk).
                    # Each community stores photos in its own album —
                    # an attachment uploaded via community A cannot be sent
                    # from community B's token.
                    attachment_cache: dict[int, str | None] = {}
                    sent_count = 0
                    failed_count = 0

                    for cb in candidates:
                        vk_id = cb.client.vk_id
                        if vk_id in already_sent:
                            continue

                        try:
                            senler_cfg = cb.branch.senler_config
                        except Exception:
                            continue

                        if not senler_cfg.is_active:
                            continue

                        if template.image:
                            if senler_cfg.pk not in attachment_cache:
                                att, _ = upload_vk_photo(senler_cfg, template.image)
                                attachment_cache[senler_cfg.pk] = att
                            attachment = attachment_cache[senler_cfg.pk]
                        else:
                            attachment = None

                        ok, err = send_vk_message(
                            senler_cfg, vk_id, template.message_text, attachment
                        )
                        if ok:
                            AutoBroadcastLog.objects.create(
                                trigger_type=trigger_type, vk_id=vk_id
                            )
                            already_sent.add(vk_id)
                            total_sent += 1
                            sent_count += 1
                        else:
                            failed_count += 1
                            logger.warning(
                                'Birthday broadcast failed vk_id=%s trigger=%s: %s',
                                vk_id, trigger_type, err,
                            )
                        time.sleep(0.05)  # VK rate limit: ≤ 20 messages/second

                    bs.status = SendStatus.DONE
                    bs.sent_count = sent_count
                    bs.failed_count = failed_count
                    bs.recipients_count = sent_count + failed_count
                    bs.finished_at = timezone.now()
                    bs.save(update_fields=[
                        'status', 'sent_count', 'failed_count',
                        'recipients_count', 'finished_at',
                    ])

        except Exception as exc:
            logger.exception(
                'send_birthday_broadcasts_task failed tenant=%s: %s',
                tenant.schema_name, exc,
            )

    return {'sent': total_sent}


# ── After-game (3h) broadcast ─────────────────────────────────────────────────

@shared_task(name='apps.tenant.senler.tasks.send_after_game_broadcast_task')
def send_after_game_broadcast_task(process_evening: bool = False) -> dict:
    """
    Sends 'after_game_3h' message to guests who played ~3 hours ago.

    Normal mode (process_evening=False, runs every 15 min):
      - Window: games played between (now-3h-20min) and (now-3h)
      - Only executes if current Moscow time is 09:00–21:00
      - Games where 3h mark falls after 21:00 are skipped (morning task handles them)

    Evening mode (process_evening=True, runs at 09:00):
      - Window: yesterday's games played between 18:01 and 23:59 Moscow time
        (their 3h mark was 21:01–26:59, i.e., after 21:00 or next-day morning)
      - Sends now (at 09:00)

    Deduplication: at most one send per (vk_id) per calendar day.
    """
    from django_tenants.utils import get_tenant_model, schema_context
    from apps.tenant.game.models import ClientAttempt
    from apps.tenant.senler.models import (
        AutoBroadcastLog, AutoBroadcastTemplate, AutoBroadcastType,
        BroadcastSend, SendStatus, TriggerType,
    )
    from apps.tenant.senler.services import send_vk_message, upload_vk_photo

    now = timezone.now()
    now_local = now.astimezone(_MSK)
    today = now_local.date()

    if process_evening:
        # Yesterday's games 18:01–23:59 whose 3h mark was after 21:00
        yesterday = today - timedelta(days=1)
        window_start = _MSK.localize(
            datetime(yesterday.year, yesterday.month, yesterday.day, 18, 1, 0)
        )
        window_end = _MSK.localize(
            datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
        )
    else:
        # Only send between 09:00 and 21:00 Moscow time
        if not (9 <= now_local.hour < 21):
            return {'sent': 0, 'reason': 'outside_send_window'}

        # Games played ~3h ago (20-min overlap prevents missed windows)
        window_end   = now - timedelta(hours=3)
        window_start = now - timedelta(hours=3, minutes=20)

    TenantModel = get_tenant_model()
    total_sent = 0

    for tenant in TenantModel.objects.exclude(schema_name='public'):
        try:
            with schema_context(tenant.schema_name):
                try:
                    template = AutoBroadcastTemplate.objects.get(
                        type=AutoBroadcastType.AFTER_GAME_3H,
                        is_active=True,
                    )
                except AutoBroadcastTemplate.DoesNotExist:
                    continue

                # Already sent today
                already_sent_today = set(
                    AutoBroadcastLog.objects
                    .filter(
                        trigger_type=AutoBroadcastType.AFTER_GAME_3H,
                        sent_at__date=today,
                    )
                    .values_list('vk_id', flat=True)
                )

                attempts = (
                    ClientAttempt.objects
                    .filter(
                        created_at__gte=window_start,
                        created_at__lte=window_end,
                        client__is_employee=False,
                        client__client__vk_id__isnull=False,
                    )
                    .select_related('client', 'client__client', 'client__branch')
                    .distinct()
                )

                if not attempts.exists():
                    continue

                # Create BroadcastSend record for this run
                bs = BroadcastSend.objects.create(
                    auto_broadcast_template=template,
                    status=SendStatus.RUNNING,
                    trigger_type=TriggerType.AUTO,
                    triggered_by=AutoBroadcastType.AFTER_GAME_3H,
                    started_at=timezone.now(),
                )

                attachment_cache: dict[int, str | None] = {}
                sent_count = 0
                failed_count = 0

                for attempt in attempts:
                    cb = attempt.client
                    vk_id = cb.client.vk_id

                    if vk_id in already_sent_today:
                        continue

                    try:
                        senler_cfg = cb.branch.senler_config
                    except Exception:
                        continue

                    if not senler_cfg.is_active:
                        continue

                    if template.image:
                        if senler_cfg.pk not in attachment_cache:
                            att, _ = upload_vk_photo(senler_cfg, template.image)
                            attachment_cache[senler_cfg.pk] = att
                        attachment = attachment_cache[senler_cfg.pk]
                    else:
                        attachment = None

                    ok, err = send_vk_message(
                        senler_cfg, vk_id, template.message_text, attachment
                    )
                    if ok:
                        AutoBroadcastLog.objects.create(
                            trigger_type=AutoBroadcastType.AFTER_GAME_3H,
                            vk_id=vk_id,
                        )
                        already_sent_today.add(vk_id)
                        total_sent += 1
                        sent_count += 1
                    else:
                        failed_count += 1
                        logger.warning(
                            'After-game broadcast failed vk_id=%s: %s', vk_id, err
                        )
                    time.sleep(0.05)  # VK rate limit: ≤ 20 messages/second

                bs.status = SendStatus.DONE
                bs.sent_count = sent_count
                bs.failed_count = failed_count
                bs.recipients_count = sent_count + failed_count
                bs.finished_at = timezone.now()
                bs.save(update_fields=[
                    'status', 'sent_count', 'failed_count',
                    'recipients_count', 'finished_at',
                ])

        except Exception as exc:
            logger.exception(
                'send_after_game_broadcast_task failed tenant=%s: %s',
                tenant.schema_name, exc,
            )

    return {'sent': total_sent}
