"""
Analytics services — atomic functions for each dashboard metric.

All functions accept:
  branch_ids: list[int] | None  — None means all branches in this tenant
  start_date / end_date: datetime.date  — inclusive range

Convention: every function does exactly one thing and is independently testable.
"""
from __future__ import annotations

from datetime import date
from collections import defaultdict

from django.db.models import Count, Min, Q
from django.db.models.functions import TruncDate
from apps.tenant.analytics.models import POSGuestCache


# ── Helpers ───────────────────────────────────────────────────────────────────

def _branch_filter(qs, branch_ids: list[int] | None, field: str = 'branch__in'):
    if branch_ids:
        return qs.filter(**{field: branch_ids})
    return qs


# ── Metric 1: QR scans ────────────────────────────────────────────────────────

def get_qr_scan_count(branch_ids: list[int] | None, start_date: date, end_date: date) -> int:
    """Count unique guests who scanned QR and interacted with the app in any way."""
    from django.db.models import Exists, OuterRef
    from apps.tenant.branch.models import (
        ClientBranch, ClientBranchVisit, CoinTransaction,
        TransactionType, TransactionSource, TestimonialMessage,
    )
    from apps.tenant.game.models import ClientAttempt
    from apps.tenant.delivery.models import Delivery

    visited_ids = ClientBranchVisit.objects.filter(
        visited_at__date__gte=start_date,
        visited_at__date__lte=end_date,
    )
    visited_ids = _branch_filter(visited_ids, branch_ids, 'client__branch__in').values('client_id')
    return ClientBranch.objects.filter(pk__in=visited_ids).filter(
        Q(vk_status__community_via_app=True)                        # Подписались на сообщество
        | Q(vk_status__newsletter_via_app=True)                     # Подписались на рассылку
        | Q(vk_status__is_story_uploaded=True)                      # Опубликовали сторис
        | Exists(ClientAttempt.objects.filter(                       # Сыграли в игру
            client=OuterRef('pk'),
            created_at__date__gte=start_date,
            created_at__date__lte=end_date,
        ))
        | Exists(TestimonialMessage.objects.filter(                  # Оставили отзыв
            conversation__client=OuterRef('pk'),
            source=TestimonialMessage.Source.APP,
            created_at__date__gte=start_date,
            created_at__date__lte=end_date,
        ))
        | Exists(Delivery.objects.filter(                            # Активировали код доставки
            activated_by=OuterRef('pk'),
            activated_at__date__gte=start_date,
            activated_at__date__lte=end_date,
        ))
        | Exists(CoinTransaction.objects.filter(                     # Потратили монеты в магазине
            client=OuterRef('pk'),
            type=TransactionType.EXPENSE,
            source=TransactionSource.SHOP,
            created_at__date__gte=start_date,
            created_at__date__lte=end_date,
        ))
    ).count()


# ── Metric 2: Total VK subscribers (all-time, no period) ─────────────────────

def get_total_vk_subscribers(branch_ids: list[int] | None) -> int:
    """Unique guests who subscribed to community OR newsletter via app (ever)."""
    from apps.tenant.branch.models import ClientVKStatus

    qs = ClientVKStatus.objects.filter(
        Q(community_via_app=True) | Q(newsletter_via_app=True)
    )
    return _branch_filter(qs, branch_ids, 'client__branch__in').count()


# ── Metric 3: New group+newsletter members who got their first gift ───────────

def get_new_group_with_first_gift(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    Guests who in the period:
      1. Subscribed to community OR newsletter via app
      2. Received their very first SuperPrize (acquired_from=GAME)
    """
    from apps.tenant.branch.models import ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry, SuperPrizeTrigger

    # Step 1: guests who subscribed via app in the period
    vk_qs = ClientVKStatus.objects.filter(
        Q(community_via_app=True, community_joined_at__date__gte=start_date,
          community_joined_at__date__lte=end_date) |
        Q(newsletter_via_app=True, newsletter_joined_at__date__gte=start_date,
          newsletter_joined_at__date__lte=end_date)
    )
    vk_qs = _branch_filter(vk_qs, branch_ids, 'client__branch__in')
    subscribed_cb_ids = set(vk_qs.values_list('client_id', flat=True))

    if not subscribed_cb_ids:
        return 0

    # Step 2: from those, who got their FIRST game prize in the period
    first_prizes = (
        SuperPrizeEntry.objects
        .filter(
            acquired_from=SuperPrizeTrigger.GAME,
            client_branch__in=subscribed_cb_ids,
        )
        .values('client_branch')
        .annotate(first_at=Min('created_at'))
        .filter(
            first_at__date__gte=start_date,
            first_at__date__lte=end_date,
        )
    )
    return first_prizes.count()


# ── Metric 4: Repeat game players ─────────────────────────────────────────────

def get_repeat_game_players(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """Guests who played on at least 2 distinct calendar days in the period."""
    from apps.tenant.game.models import ClientAttempt

    qs = ClientAttempt.objects.filter(
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    qs = _branch_filter(qs, branch_ids, 'client__branch__in')

    # Collect distinct (client_id, date) pairs
    pairs = (
        qs.annotate(play_date=TruncDate('created_at'))
        .values_list('client_id', 'play_date')
        .distinct()
    )

    client_days: dict[int, set] = defaultdict(set)
    for client_id, play_date in pairs:
        client_days[client_id].add(play_date)

    return sum(1 for days in client_days.values() if len(days) >= 2)


# ── Metric 5: Coin purchasers ─────────────────────────────────────────────────

def get_coin_purchasers(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """Unique guests who spent coins in the shop at least once in the period."""
    from apps.tenant.branch.models import CoinTransaction, TransactionType, TransactionSource

    qs = CoinTransaction.objects.filter(
        type=TransactionType.EXPENSE,
        source=TransactionSource.SHOP,
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    qs = _branch_filter(qs, branch_ids, 'client__branch__in')
    return qs.values('client').distinct().count()


# ── Metric 6: New VK community subscribers via app ───────────────────────────

def get_new_community_subscribers(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """Guests who subscribed to VK community via app in the period (by community_joined_at)."""
    from apps.tenant.branch.models import ClientVKStatus

    qs = ClientVKStatus.objects.filter(
        community_via_app=True,
        community_joined_at__date__gte=start_date,
        community_joined_at__date__lte=end_date,
    )
    return _branch_filter(qs, branch_ids, 'client__branch__in').count()


# ── Metric 7: New VK newsletter subscribers via app ──────────────────────────

def get_new_newsletter_subscribers(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """Guests who subscribed to VK newsletter via app in the period (by newsletter_joined_at)."""
    from apps.tenant.branch.models import ClientVKStatus

    qs = ClientVKStatus.objects.filter(
        newsletter_via_app=True,
        newsletter_joined_at__date__gte=start_date,
        newsletter_joined_at__date__lte=end_date,
    )
    return _branch_filter(qs, branch_ids, 'client__branch__in').count()


# ── Metric 7b: First gift receivers ──────────────────────────────────────────

def get_first_gift_receivers(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    Unique guests whose very first InventoryItem (any acquisition source) was
    created within the period — i.e. they got their first-ever gift in this range.
    """
    from apps.tenant.inventory.models import InventoryItem

    qs = InventoryItem.objects.all()
    qs = _branch_filter(qs, branch_ids, 'client_branch__branch__in')

    first_items = (
        qs.values('client_branch')
        .annotate(first_at=Min('created_at'))
        .filter(
            first_at__date__gte=start_date,
            first_at__date__lte=end_date,
        )
    )
    return first_items.count()


# ── Metric 7c: Gift activators ───────────────────────────────────────────────

def get_gift_activators(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """Unique guests who activated at least one InventoryItem in the period."""
    from apps.tenant.inventory.models import InventoryItem

    qs = InventoryItem.objects.filter(
        activated_at__date__gte=start_date,
        activated_at__date__lte=end_date,
    )
    qs = _branch_filter(qs, branch_ids, 'client_branch__branch__in')
    return qs.values('client_branch').distinct().count()


# ── Metric 8: Birthday greetings sent ────────────────────────────────────────

def get_birthday_greetings_sent(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    Unique guests who received at least one birthday auto-broadcast in the period.
    Counts by distinct vk_id from AutoBroadcastLog (birthday triggers: 7d, 1d, day-of).
    Note: birthday sends bypass BroadcastRecipient and log directly to AutoBroadcastLog.
    """
    from apps.tenant.senler.models import AutoBroadcastLog, AutoBroadcastType

    BIRTHDAY_TRIGGERS = [
        AutoBroadcastType.BIRTHDAY_7_DAYS,
        AutoBroadcastType.BIRTHDAY_1_DAY,
        AutoBroadcastType.BIRTHDAY,
    ]
    qs = AutoBroadcastLog.objects.filter(
        trigger_type__in=BIRTHDAY_TRIGGERS,
        sent_at__date__gte=start_date,
        sent_at__date__lte=end_date,
    )
    if branch_ids:
        from apps.tenant.branch.models import ClientBranch
        vk_ids = ClientBranch.objects.filter(
            branch__in=branch_ids,
            client__vk_id__isnull=False,
        ).values_list('client__vk_id', flat=True)
        qs = qs.filter(vk_id__in=vk_ids)
    return qs.values('vk_id').distinct().count()


# ── Metric 9: Birthday celebrants (came to redeem birthday prize) ─────────────

def get_birthday_celebrants(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    Unique guests who visited the cafe on their birthday (month+day of visit matches birth_date).
    Counts distinct guests from ClientBranchVisit where visit date matches their birthday.
    """
    from django.db.models import F
    from django.db.models.functions import ExtractMonth, ExtractDay
    from apps.tenant.branch.models import ClientBranchVisit

    qs = ClientBranchVisit.objects.filter(
        visited_at__date__gte=start_date,
        visited_at__date__lte=end_date,
        client__birth_date__isnull=False,
    ).annotate(
        visit_month=ExtractMonth('visited_at'),
        visit_day=ExtractDay('visited_at'),
        birth_month=ExtractMonth('client__birth_date'),
        birth_day=ExtractDay('client__birth_date'),
    ).filter(
        visit_month=F('birth_month'),
        visit_day=F('birth_day'),
    )
    qs = _branch_filter(qs, branch_ids, 'client__branch__in')
    return qs.values('client').distinct().count()


# ── Metric 10: Message open rate ─────────────────────────────────────────────

def get_message_open_rate(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> float:
    """
    % of sent VK messages that were read in the period.

    Counts messages from all three outgoing channels:
    - BroadcastRecipient (рассылки и авторассылки)
    - TestimonialMessage(source=ADMIN_REPLY) (ответы на отзывы)

    read_at is populated by the hourly Celery task `check_read_status_task`
    which polls VK API `messages.getConversationsById`.

    Returns 0.0 if no messages were sent in the period.
    """
    from apps.tenant.senler.models import BroadcastRecipient, RecipientStatus
    from apps.tenant.branch.models import TestimonialMessage

    # ── Broadcasts & auto-broadcasts ─────────────────────────────────────────
    bc_qs = BroadcastRecipient.objects.filter(
        status=RecipientStatus.SENT,
        sent_at__date__gte=start_date,
        sent_at__date__lte=end_date,
    )
    if branch_ids:
        bc_qs = bc_qs.filter(client_branch__branch__in=branch_ids)

    bc_sent = bc_qs.count()
    bc_read = bc_qs.filter(read_at__isnull=False).count()

    # ── Admin replies (ответы на отзывы) ──────────────────────────────────────
    reply_qs = TestimonialMessage.objects.filter(
        source=TestimonialMessage.Source.ADMIN_REPLY,
        vk_message_id__gt='',
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    if branch_ids:
        reply_qs = reply_qs.filter(conversation__branch__in=branch_ids)

    reply_sent = reply_qs.count()
    reply_read = reply_qs.filter(read_at__isnull=False).count()

    total_sent = bc_sent + reply_sent
    total_read = bc_read + reply_read
    if total_sent == 0:
        return 0.0, 0, 0

    return round(total_read / total_sent * 100, 1), total_sent, total_read


# ── Metric 11 & 12: VK stories (not yet implemented) ─────────────────────────

def get_vk_stories_publishers(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    Unique guests who published a VK story via the app in the period.
    Filtered by story_uploaded_at date range (first upload date).
    """
    from apps.tenant.branch.models import ClientVKStatus

    qs = ClientVKStatus.objects.filter(
        is_story_uploaded=True,
        story_uploaded_at__date__gte=start_date,
        story_uploaded_at__date__lte=end_date,
    )
    return _branch_filter(qs, branch_ids, 'client__branch__in').count()


def get_stories_referrals(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    New guests who registered via a referral link from someone's VK story.
    Counts ClientBranch records where invited_by is set and created_at is in period.
    """
    from apps.tenant.branch.models import ClientBranch

    qs = ClientBranch.objects.filter(
        invited_by__isnull=False,
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    return _branch_filter(qs, branch_ids, 'branch__in').count()


# ── Metric 13: POS guests ────────────────────────────────────────────────────

def get_pos_guests_count(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> int:
    """
    Guest count from POS systems (IIKO/Dooglys) for the period.
    1. Reads from POSGuestCache (populated by hourly Celery task for today,
       daily at 02:00 for yesterday).
    2. If today is in range but not yet cached, supplements cached total with
       a live POS API call for today only.
    3. If the cache is completely empty, falls back to a live POS API call for
       the full range.
    Returns 0 if POS is not configured or fetch fails.
    """
    import logging
    from datetime import date as _date
    from django.db.models import Sum

    today = _date.today()

    qs = POSGuestCache.objects.filter(date__gte=start_date, date__lte=end_date)
    if branch_ids:
        qs = qs.filter(branch__in=branch_ids)
    result = qs.aggregate(total=Sum('guest_count'))
    cached_total = result['total'] or 0

    # Check whether today is in the requested range but missing from the cache
    today_in_range = start_date <= today <= end_date
    today_cached = today_in_range and qs.filter(date=today).exists()

    if cached_total and (not today_in_range or today_cached):
        # Cache is complete for the requested range
        return cached_total

    # Need live data: either the cache is empty, or today is missing
    try:
        from django.db import connection
        from apps.shared.config.models import POSType
        from apps.tenant.analytics.pos_service import sync_get_guests_for_period
        from apps.tenant.branch.models import Branch

        config = getattr(connection.tenant, 'config', None)
        if not config or getattr(config, 'pos_type', POSType.NONE) == POSType.NONE:
            return cached_total

        branches_qs = Branch.objects.filter(is_active=True)
        if branch_ids:
            branches_qs = branches_qs.filter(id__in=branch_ids)
        branches = list(branches_qs)
        if not branches:
            return cached_total

        if cached_total and today_in_range and not today_cached:
            # Cache has past days; only fetch today live
            live_results = sync_get_guests_for_period(config, today, today, branches=branches)
        else:
            # Cache is completely empty — fetch the full range
            live_results = sync_get_guests_for_period(config, start_date, end_date, branches=branches)

        live_total = sum(live_results.values()) if live_results else 0
        return cached_total + live_total

    except Exception:
        logging.getLogger(__name__).exception('get_pos_guests_count: live POS fetch failed')
        return cached_total


# ── Metric 14: Scan index ────────────────────────────────────────────────────

def get_scan_index(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> float:
    """QR scans ÷ POS guests × 100%. Returns 0.0 if no POS data."""
    scans = get_qr_scan_count(branch_ids, start_date, end_date)
    pos = get_pos_guests_count(branch_ids, start_date, end_date)
    if not pos:
        return 0.0
    return round(scans / pos * 100, 1)


# ── Main aggregate ────────────────────────────────────────────────────────────

def get_general_stats(
    branch_ids: list[int] | None, start_date: date, end_date: date,
    skip_slow: bool = False,
) -> dict:
    """All general-stats metrics in a single dict for the API response.

    Pass skip_slow=True to omit POS-dependent metrics (pos_guests, scan_index)
    that may require a live external API call. Use the /api/v1/analytics/stats/slow/
    endpoint to load those asynchronously after the page renders.
    """
    scans = get_qr_scan_count(branch_ids, start_date, end_date)

    if skip_slow:
        pos        = None
        scan_index = None
    else:
        pos        = get_pos_guests_count(branch_ids, start_date, end_date)
        scan_index = round(scans / pos * 100, 1) if pos else 0.0

    return {
        'qr_scans':                  scans,
        'total_vk_subscribers':      get_total_vk_subscribers(branch_ids),
        'new_group_with_gift':       get_new_group_with_first_gift(branch_ids, start_date, end_date),
        'repeat_game_players':       get_repeat_game_players(branch_ids, start_date, end_date),
        'coin_purchasers':           get_coin_purchasers(branch_ids, start_date, end_date),
        'new_community_subscribers': get_new_community_subscribers(branch_ids, start_date, end_date),
        'new_newsletter_subscribers': get_new_newsletter_subscribers(branch_ids, start_date, end_date),
        'first_gift_receivers':      get_first_gift_receivers(branch_ids, start_date, end_date),
        'gift_activators':           get_gift_activators(branch_ids, start_date, end_date),
        'birthday_greetings_sent':   get_birthday_greetings_sent(branch_ids, start_date, end_date),
        'birthday_celebrants':       get_birthday_celebrants(branch_ids, start_date, end_date),
        **dict(zip(
            ('message_open_rate', 'message_total_sent', 'message_total_read'),
            get_message_open_rate(branch_ids, start_date, end_date),
        )),
        'vk_stories_publishers':     get_vk_stories_publishers(branch_ids, start_date, end_date),
        'stories_referrals':         get_stories_referrals(branch_ids, start_date, end_date),
        'pos_guests':                pos,
        'scan_index':                scan_index,
    }


# ── Chart data ────────────────────────────────────────────────────────────────

def get_chart_data(
    branch_ids: list[int] | None, start_date: date, end_date: date
) -> dict:
    """Returns data for all dashboard donut charts."""
    from apps.tenant.branch.models import ClientBranchVisit, CoinTransaction, TransactionType, TransactionSource
    from apps.tenant.game.models import ClientAttempt
    from apps.tenant.inventory.models import SuperPrizeEntry, InventoryItem, AcquisitionSource
    from apps.tenant.quest.models import QuestSubmit

    # ── 1. Repeat visits ──────────────────────────────────────────────────────
    visits_qs = ClientBranchVisit.objects.filter(
        visited_at__date__gte=start_date,
        visited_at__date__lte=end_date,
    )
    visits_qs = _branch_filter(visits_qs, branch_ids, 'client__branch__in')
    visit_counts = visits_qs.values('client_id').annotate(cnt=Count('id'))
    repeat_visits = visit_counts.filter(cnt__gte=2).count()
    once_visits   = visit_counts.filter(cnt=1).count()

    # ── 2. Gift sources ───────────────────────────────────────────────────────
    sp_qs = SuperPrizeEntry.objects.filter(
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    sp_qs = _branch_filter(sp_qs, branch_ids, 'client_branch__branch__in')
    free_prizes = sp_qs.values('client_branch').distinct().count()

    coins_qs = CoinTransaction.objects.filter(
        type=TransactionType.EXPENSE,
        source=TransactionSource.SHOP,
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    coins_qs = _branch_filter(coins_qs, branch_ids, 'client__branch__in')
    coin_purchases = coins_qs.values('client').distinct().count()

    # ── 3. Staff involvement ──────────────────────────────────────────────────
    attempts_qs = ClientAttempt.objects.filter(
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    attempts_qs = _branch_filter(attempts_qs, branch_ids, 'client__branch__in')
    served_count     = attempts_qs.filter(served_by__isnull=False).count()
    not_served_count = attempts_qs.filter(served_by__isnull=True).count()

    # ── 4. Quest completion ───────────────────────────────────────────────────
    quest_qs = QuestSubmit.objects.filter(
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    quest_qs = _branch_filter(quest_qs, branch_ids, 'client__branch__in')
    quests_done    = quest_qs.filter(completed_at__isnull=False).count()
    quests_pending = quest_qs.filter(completed_at__isnull=True).count()

    # ── 5. VK stories ─────────────────────────────────────────────────────────
    # Both sides scoped to the period: uploaded in period vs visited but didn't upload.
    from apps.tenant.branch.models import ClientVKStatus
    story_qs = ClientVKStatus.objects.filter(
        is_story_uploaded=True,
        story_uploaded_at__date__gte=start_date,
        story_uploaded_at__date__lte=end_date,
    )
    story_qs = _branch_filter(story_qs, branch_ids, 'client__branch__in')
    stories_uploaded = story_qs.count()

    period_visitor_ids = (
        _branch_filter(visits_qs, branch_ids, 'client__branch__in')
        .values('client_id').distinct()
    )
    stories_not_uploaded = (
        ClientVKStatus.objects
        .filter(client_id__in=period_visitor_ids)
        .exclude(client_id__in=story_qs.values('client_id'))
        .count()
    )

    return {
        'repeat_visits':     {'repeat': repeat_visits,   'first_time': once_visits},
        'gift_sources':      {'free': free_prizes,        'coins': coin_purchases},
        'staff_involvement': {'served': served_count,     'not_served': not_served_count},
        'quests':            {'completed': quests_done,   'pending': quests_pending},
        'vk_stories':        {'uploaded': stories_uploaded, 'not_uploaded': stories_not_uploaded},
    }


# ── RF helpers ────────────────────────────────────────────────────────────────

# ── Standard hints & strategies — ALWAYS override DB values ───────────────────
# This ensures every cafe shows the same short, correct tips regardless of
# whether the management command was run or what custom data is in the DB.
_STANDARD_SEGMENT_DATA: dict[str, dict[str, str]] = {
    'R3F1': {
        'strategy': 'Закрепить первый визит и привести ко второму заказу.',
        'hint': (
            'Цель: Закрепить первый визит и привести ко второму заказу\n'
            'Что отправлять: Приветственное сообщение + простой бонус на следующий заказ '
            '(подарок или небольшая выгода)\n'
            'Частота: 1–2 сообщения за 7–14 дней\n'
            'Если реакция слабая: Отправьте напоминание с тем же бонусом, '
            'затем пауза минимум 2 недели'
        ),
    },
    'R3F2': {
        'strategy': 'Сформировать привычку заказывать регулярно.',
        'hint': (
            'Цель: Сформировать привычку заказывать регулярно\n'
            'Что отправлять: Выгодное предложение или новинка с ограниченным сроком\n'
            'Частота: 1 сообщение раз в 7–10 дней\n'
            'Если реакция слабая: Смените повод (другой бонус/набор) и сделайте паузу 10–14 дней'
        ),
    },
    'R3F3': {
        'strategy': 'Удержать самых активных гостей без перегруза.',
        'hint': (
            'Цель: Удержать самых активных гостей без перегруза\n'
            'Что отправлять: Редкие «приятные» сообщения: подарок или благодарность '
            'без сложных условий\n'
            'Частота: 1 сообщение раз в 10–14 дней\n'
            'Если реакция слабая: Ничего не усиливайте — просто сделайте паузу '
            'и не учащайте рассылки'
        ),
    },
    'R2F1': {
        'strategy': 'Вернуть гостя на повторный заказ.',
        'hint': (
            'Цель: Вернуть гостя на повторный заказ\n'
            'Что отправлять: Простая акция или подарок с коротким сроком действия\n'
            'Частота: 1 сообщение раз в 10–14 дней\n'
            'Если реакция слабая: Через 3–5 дней отправьте другой повод, '
            'затем пауза 2–3 недели'
        ),
    },
    'R2F2': {
        'strategy': 'Поддерживать интерес и не дать «остыть».',
        'hint': (
            'Цель: Поддерживать интерес и не дать «остыть»\n'
            'Что отправлять: Выгодное предложение или напоминание о кафе без давления\n'
            'Частота: 1 сообщение раз в 14 дней\n'
            'Если реакция слабая: Сделайте паузу 2–3 недели, не усиливая бонус'
        ),
    },
    'R2F3': {
        'strategy': 'Не потерять самых ценных гостей.',
        'hint': (
            'Цель: Не потерять самых ценных гостей\n'
            'Что отправлять: Только VIP-поводы: подарок, благодарность, особое предложение\n'
            'Частота: 1 сообщение в месяц (максимум 2)\n'
            'Если реакция слабая: Ничего не отправляйте дополнительно — лучше пауза, '
            'чем лишнее сообщение'
        ),
    },
    'R1F1': {
        'strategy': 'Вернуть до перехода в «потерянные».',
        'hint': (
            'Цель: Вернуть до перехода в «потерянные»\n'
            'Что отправлять: Мягкое напоминание + бонус на возвращение\n'
            'Частота: 2 сообщения за 7–10 дней\n'
            'Если реакция слабая: Если нет отклика — пауза 3–4 недели'
        ),
    },
    'R1F2': {
        'strategy': 'Срочно вернуть ранее постоянных гостей.',
        'hint': (
            'Цель: Срочно вернуть ранее постоянных гостей\n'
            'Что отправлять: Сильное, но понятное предложение с ограниченным сроком\n'
            'Частота: 2 сообщения за 10–14 дней\n'
            'Если реакция слабая: После второго сообщения — пауза минимум 1 месяц'
        ),
    },
    'R1F3': {
        'strategy': 'Аккуратно вернуть VIP-гостя.',
        'hint': (
            'Цель: Аккуратно вернуть VIP-гостя\n'
            'Что отправлять: Личное сообщение + подарок без условий\n'
            'Частота: 1 сообщение + напоминание через 10–14 дней\n'
            'Если реакция слабая: Дальше только пауза, не чаще 1 раза в 1–2 месяца'
        ),
    },
    'R0F1': {
        'strategy': 'Попробовать реактивировать без давления.',
        'hint': (
            'Цель: Попробовать реактивировать без давления\n'
            'Что отправлять: Одно камбэк-предложение или новинка\n'
            'Частота: 1 сообщение раз в 1–2 месяца\n'
            'Если реакция слабая: Если реакции нет — увеличить паузу или исключить рассылки'
        ),
    },
    'R0F2': {
        'strategy': 'Вернуть сильных гостей прошлого.',
        'hint': (
            'Цель: Вернуть сильных гостей прошлого\n'
            'Что отправлять: Персональный повод вернуться с хорошей выгодой\n'
            'Частота: 2 попытки за 6 недель\n'
            'Если реакция слабая: Дальше только редкие сообщения раз в 2–3 месяца'
        ),
    },
    'R0F3': {
        'strategy': 'Финальная попытка вернуть VIP.',
        'hint': (
            'Цель: Финальная попытка вернуть VIP\n'
            'Что отправлять: Очень уважительное предложение или личное обращение\n'
            'Частота: 1 сообщение раз в 6–8 недель\n'
            'Если реакция слабая: Если нет реакции — полностью прекратить рассылки'
        ),
    },
}

# Map (r_score, f_score) → segment code for lookup
_RF_TO_CODE = {
    (4, 1): 'R3F1', (4, 2): 'R3F2', (4, 3): 'R3F3',
    (3, 1): 'R2F1', (3, 2): 'R2F2', (3, 3): 'R2F3',
    (2, 1): 'R1F1', (2, 2): 'R1F2', (2, 3): 'R1F3',
    (1, 1): 'R0F1', (1, 2): 'R0F2', (1, 3): 'R0F3',
}


def _apply_standard_hints(cell: dict) -> None:
    """Override hint & strategy with hardcoded standard values."""
    code = _RF_TO_CODE.get((cell['r_score'], cell['f_score']), '')
    std = _STANDARD_SEGMENT_DATA.get(code)
    if std:
        cell['segment_hint'] = std['hint']
        cell['segment_strategy'] = std['strategy']


# Fixed R/F level labels (r_score=4 → R3 = most recent, r_score=1 → R0 = lost)
_R_META = {
    4: {'label': 'R3', 'name': 'Свежий',    'range': '0–14 дн.'},
    3: {'label': 'R2', 'name': 'Тёплый',    'range': '15–30 дн.'},
    2: {'label': 'R1', 'name': 'Остывший',  'range': '31–60 дн.'},
    1: {'label': 'R0', 'name': 'Холодный',  'range': '>61 дн.'},
}
_F_META = {
    1: {'label': 'F1', 'name': 'Редко',     'range': '1–3 виз.'},
    2: {'label': 'F2', 'name': 'Умеренно',  'range': '4–5 виз.'},
    3: {'label': 'F3', 'name': 'Часто',     'range': '6+ виз.'},
}

# Representative recency/frequency values used to look up segments by r/f score
_R_REPRESENTATIVE = {4: 7, 3: 22, 2: 45, 1: 90}
_F_REPRESENTATIVE = {1: 2, 2: 4, 3: 7}


def _get_score_model(mode: str):
    from apps.tenant.analytics.models import GuestRFScore, GuestRFScoreDelivery
    return GuestRFScoreDelivery if mode == 'delivery' else GuestRFScore


def _get_migration_model(mode: str):
    from apps.tenant.analytics.models import RFMigrationLog, RFMigrationLogDelivery
    return RFMigrationLogDelivery if mode == 'delivery' else RFMigrationLog


def _get_snapshot_model(mode: str):
    from apps.tenant.analytics.models import BranchSegmentSnapshot, BranchSegmentSnapshotDelivery
    return BranchSegmentSnapshotDelivery if mode == 'delivery' else BranchSegmentSnapshot


# ── RF Matrix ─────────────────────────────────────────────────────────────────

def get_rf_matrix(branch_ids: list[int] | None, mode: str = 'restaurant') -> dict:
    """
    Build the RF matrix for the given mode (restaurant | delivery).

    Returns {
      total: int,
      r_levels: [...],    # sorted desc (R3→R0)
      f_levels: [...],    # sorted asc (F1→F3)
      cells: {            # key: "r_f"
        "4_1": {segment_code, segment_name, emoji, color, count, pct},
        ...
      }
    }
    """
    ScoreModel = _get_score_model(mode)
    qs = ScoreModel.objects.select_related('segment').all()
    qs = _branch_filter(qs, branch_ids, 'client__branch_profiles__branch__in')
    if branch_ids:
        qs = qs.distinct()

    total = qs.count()

    groups = (
        qs.values(
            'r_score', 'f_score',
            'segment__code', 'segment__name',
            'segment__emoji', 'segment__color',
            'segment__strategy', 'segment__hint',
            'segment__id',
        )
        .annotate(count=Count('id'))
    )

    cell_lookup = {}
    r_vals_found: set[int] = set()
    f_vals_found: set[int] = set()

    for g in groups:
        r, f = g['r_score'], g['f_score']
        r_vals_found.add(r)
        f_vals_found.add(f)
        cell_lookup[f'{r}_{f}'] = {
            'r_score':       r,
            'f_score':       f,
            'segment_id':    g['segment__id'],
            'segment_code':  g['segment__code'] or '',
            'segment_name':  g['segment__name'] or '—',
            'segment_emoji': g['segment__emoji'] or '',
            'segment_color': g['segment__color'] or '#e0e0e0',
            'segment_strategy': g['segment__strategy'] or '',
            'segment_hint':  g['segment__hint'] or '',
            'count':         g['count'],
            'pct':           round(g['count'] / total * 100, 1) if total else 0.0,
        }

    # Always show full 4×3 grid regardless of which scores are present in data
    r_vals = [4, 3, 2, 1]
    f_vals = [1, 2, 3]

    # Load all segments once — used to fill in segment info for cells with NULL or missing segment
    from apps.tenant.analytics.models import RFSegment
    all_segments = list(RFSegment.objects.all())

    def _find_segment_for_rf(r: int, f: int):
        """Return the RFSegment whose boundaries cover the representative recency/frequency for (r, f)."""
        rec  = _R_REPRESENTATIVE.get(r, 90)
        freq = _F_REPRESENTATIVE.get(f, 1)
        for seg in all_segments:
            if seg.recency_min <= rec <= seg.recency_max and seg.frequency_min <= freq <= seg.frequency_max:
                return seg
        return None

    # Always override segment display info from current boundaries,
    # so that changes to segment definitions are immediately reflected in the matrix
    # without requiring a full RF score recalculation.
    for cell in cell_lookup.values():
        seg = _find_segment_for_rf(cell['r_score'], cell['f_score'])
        if seg:
            cell['segment_id']       = seg.pk
            cell['segment_code']     = seg.code
            cell['segment_name']     = seg.name
            cell['segment_emoji']    = seg.emoji
            cell['segment_color']    = seg.color
            cell['segment_strategy'] = seg.strategy
            cell['segment_hint']     = seg.hint

    cells: dict[str, dict] = {}
    for r in r_vals:
        for f in f_vals:
            key = f'{r}_{f}'
            if key in cell_lookup:
                cells[key] = cell_lookup[key]
            else:
                seg = _find_segment_for_rf(r, f)
                cells[key] = {
                    'r_score':          r,
                    'f_score':          f,
                    'segment_id':       seg.pk       if seg else None,
                    'segment_code':     seg.code     if seg else '',
                    'segment_name':     seg.name     if seg else '—',
                    'segment_emoji':    seg.emoji    if seg else '',
                    'segment_color':    seg.color    if seg else '#e8e8e8',
                    'segment_strategy': seg.strategy if seg else '',
                    'segment_hint':     seg.hint     if seg else '',
                    'count': 0, 'pct': 0.0,
                }

    # ── Always override hints/strategy with hardcoded standard values ─────
    # This guarantees every cafe shows the same correct tips,
    # regardless of what's stored in the DB.
    for cell in cells.values():
        _apply_standard_hints(cell)

    return {
        'total':    total,
        'r_levels': [{'r_score': r, **_R_META.get(r, {'label': f'R{r-1}', 'name': '', 'range': ''})} for r in r_vals],
        'f_levels': [{'f_score': f, **_F_META.get(f, {'label': f'F{f}',   'name': '', 'range': ''})} for f in f_vals],
        'cells':    cells,
    }


# ── RF Summary stats ──────────────────────────────────────────────────────────

def get_rf_summary_stats(branch_ids: list[int] | None, mode: str = 'restaurant') -> dict:
    """
    4 summary cards for the RF analysis header:
    - total:        all digitised guests
    - vip_f3:       guests with f_score == max (frequent visitors)
    - at_risk_r1:   guests with r_score == 2 (cooling)
    - lost_r0:      guests with r_score == 1 (lost/cold)
    """
    ScoreModel = _get_score_model(mode)
    qs = ScoreModel.objects.all()
    qs = _branch_filter(qs, branch_ids, 'client__branch_profiles__branch__in')
    if branch_ids:
        qs = qs.distinct()

    total = qs.count()

    from django.db.models import Max
    max_f = qs.aggregate(m=Max('f_score'))['m'] or 3

    vip_f3    = qs.filter(f_score=max_f).count()
    at_risk   = qs.filter(r_score=2).count()
    lost_r0   = qs.filter(r_score=1).count()

    return {
        'total':    total,
        'vip_f3':   vip_f3,
        'at_risk':  at_risk,
        'lost_r0':  lost_r0,
    }


# ── RF Segment guests list ────────────────────────────────────────────────────

def get_rf_segment_guests(
    branch_ids: list[int] | None, r_score: int, f_score: int,
    mode: str = 'restaurant', limit: int = 50,
) -> list[dict]:
    """Guest list for a specific RF segment cell."""
    ScoreModel = _get_score_model(mode)
    qs = (
        ScoreModel.objects
        .select_related('client', 'segment')
        .filter(r_score=r_score, f_score=f_score)
    )
    qs = _branch_filter(qs, branch_ids, 'client__branch_profiles__branch__in')

    from django.db.models import Max, Sum, Q as _Q
    from apps.tenant.branch.models import ClientBranchVisit, CoinTransaction

    scores = list(qs.distinct()[:limit])
    if not scores:
        return []

    guest_ids = [s.client_id for s in scores]

    # Aggregate across ALL branches for each unique guest
    last_visit_map = {
        r['client__client_id']: r['last']
        for r in ClientBranchVisit.objects
        .filter(client__client_id__in=guest_ids)
        .values('client__client_id')
        .annotate(last=Max('visited_at'))
    }

    balance_map = {
        r['client__client_id']: (r['income'] or 0) - (r['expense'] or 0)
        for r in CoinTransaction.objects
        .filter(client__client_id__in=guest_ids)
        .values('client__client_id')
        .annotate(
            income=Sum('amount', filter=_Q(type='income')),
            expense=Sum('amount', filter=_Q(type='expense')),
        )
    }

    result = []
    for score in scores:
        guest = score.client  # guest.Client
        last_visit = last_visit_map.get(guest.pk)
        result.append({
            'id':           guest.pk,
            'vk_id':        guest.vk_id,
            'first_name':   guest.first_name,
            'last_name':    guest.last_name,
            'recency_days': score.recency_days,
            'frequency':    score.frequency,
            'r_score':      score.r_score,
            'f_score':      score.f_score,
            'last_visit':   last_visit.strftime('%d.%m.%Y') if last_visit else '—',
            'coins':        balance_map.get(guest.pk, 0),
        })
    return result


# ── RF snapshot trend ─────────────────────────────────────────────────────────

def get_rf_snapshot_trend(
    branch_ids: list[int] | None, days: int = 30, mode: str = 'restaurant'
) -> list[dict]:
    """Historical segment trend over the last N days."""
    from datetime import date as date_type, timedelta
    from django.db.models import Sum

    end   = date_type.today()
    start = end - timedelta(days=days)

    SnapshotModel = _get_snapshot_model(mode)
    qs = (
        SnapshotModel.objects
        .filter(date__gte=start, date__lte=end)
        .values('date', 'segment__code', 'segment__color', 'segment__name')
        .annotate(guests=Sum('guests_count'))
        .order_by('date', 'segment__code')
    )
    if branch_ids:
        qs = qs.filter(branch__in=branch_ids)

    by_date: dict[str, list] = defaultdict(list)
    for row in qs:
        by_date[str(row['date'])].append({
            'code':   row['segment__code'],
            'name':   row['segment__name'],
            'color':  row['segment__color'],
            'guests': row['guests'] or 0,
        })

    return [{'date': d, 'segments': segs} for d, segs in sorted(by_date.items())]


# ── RF Migration summary ──────────────────────────────────────────────────────

def get_rf_migration_summary(
    branch_ids: list[int] | None, days: int = 30, mode: str = 'restaurant'
) -> list[dict]:
    """Top migration flows sorted by count descending."""
    from datetime import date as date_type, timedelta

    since = date_type.today() - timedelta(days=days)
    MigModel = _get_migration_model(mode)
    qs = MigModel.objects.filter(
        created_at__date__gte=since,
        to_segment__isnull=False,           # skip records where target segment was deleted
    )
    if branch_ids:
        qs = qs.filter(client__branch_profiles__branch__in=branch_ids)

    # Use select_related to avoid INNER JOIN problem with nullable from_segment
    # (Django .values('from_segment__code') uses INNER JOIN which drops NULL rows)
    counts: dict[tuple, int] = {}
    meta: dict[tuple, dict] = {}

    for mig in qs.select_related('from_segment', 'to_segment').iterator():
        fs = mig.from_segment
        ts = mig.to_segment
        key = (
            fs.code  if fs else '',
            fs.name  if fs else '—',
            fs.emoji if fs else '',
            fs.color if fs else '#94a3b8',
            ts.code,
            ts.name,
            ts.emoji or '',
            ts.color or '#94a3b8',
        )
        counts[key] = counts.get(key, 0) + 1
        meta[key] = {
            'from_code':  key[0], 'from_name':  key[1],
            'from_emoji': key[2], 'from_color': key[3],
            'to_code':    key[4], 'to_name':    key[5],
            'to_emoji':   key[6], 'to_color':   key[7],
        }

    result = sorted(
        [{'count': v, **meta[k]} for k, v in counts.items()],
        key=lambda x: -x['count'],
    )
    return result[:30]


# ── Migration effectiveness ───────────────────────────────────────────────────

def get_migration_effectiveness(
    branch_ids: list[int] | None, days: int = 30, mode: str = 'restaurant'
) -> dict:
    """
    Computes 4 migration KPIs for the period:
    - growth:      moved to segment with lower recency (more recent)
    - cooling:     r_score dropped but not to R0
    - lost_to_r0:  moved to r_score=1 (R0)
    - reactivated: moved FROM r_score=1 to higher r_score
    """
    from datetime import date as date_type, timedelta
    from apps.tenant.analytics.models import RFSegment

    since = date_type.today() - timedelta(days=days)
    MigModel = _get_migration_model(mode)
    qs = MigModel.objects.filter(
        created_at__date__gte=since,
        from_segment__isnull=False,
        to_segment__isnull=False,
    )
    if branch_ids:
        qs = qs.filter(client__branch__in=branch_ids)

    # Map segment pk → r_score (higher = more recent; 1 = R0/lost)
    # Using _r_score(recency_min) ensures consistency with the scoring function:
    #   recency_min=0  → r_score=4 (R3, fresh)
    #   recency_min=15 → r_score=3 (R2, warm)
    #   recency_min=31 → r_score=2 (R1, cooling)
    #   recency_min=61 → r_score=1 (R0, lost)
    seg_r_score = {s.pk: _r_score(s.recency_min) for s in RFSegment.objects.all()}

    growth = cooling = lost_to_r0 = reactivated = 0

    for mig in qs.values('from_segment_id', 'to_segment_id'):
        from_r = seg_r_score.get(mig['from_segment_id'], 0)
        to_r   = seg_r_score.get(mig['to_segment_id'],   0)

        if not from_r or not to_r or from_r == to_r:
            continue                        # segment deleted or no change

        if to_r > from_r:                   # moved to fresher segment → growth
            growth += 1
        elif to_r < from_r:
            if to_r == 1:                   # landed in R0 (lost)
                lost_to_r0 += 1
            else:
                cooling += 1

        if from_r == 1 and to_r > 1:       # came back from R0 → reactivated
            reactivated += 1

    return {
        'growth':      growth,
        'cooling':     cooling,
        'lost_to_r0':  lost_to_r0,
        'reactivated': reactivated,
    }


# ── Combined RF stats ─────────────────────────────────────────────────────────

def get_rf_stats(branch_ids: list[int] | None, mode: str = 'restaurant') -> dict:
    """All RF analysis data in one dict."""
    return {
        'matrix':    get_rf_matrix(branch_ids, mode),
        'summary':   get_rf_summary_stats(branch_ids, mode),
        'trend':     get_rf_snapshot_trend(branch_ids, mode=mode),
        'migrations': get_rf_migration_summary(branch_ids, mode=mode),
    }


# ── Migration history page ────────────────────────────────────────────────────

def get_migration_history(
    branch_ids: list[int] | None, days: int = 30, mode: str = 'restaurant',
    segment_code: str | None = None,
) -> dict:
    """Full migration history data for the migration history page."""
    flows = get_rf_migration_summary(branch_ids, days, mode)
    effectiveness = get_migration_effectiveness(branch_ids, days, mode)

    # Filter by segment if specified
    if segment_code:
        flows = [
            f for f in flows
            if f['from_code'] == segment_code or f['to_code'] == segment_code
        ]

    from apps.tenant.analytics.models import RFSegment
    all_segments = list(RFSegment.objects.values('code', 'name', 'emoji').order_by('recency_min', 'frequency_min'))

    return {
        'flows':         flows,
        'effectiveness': effectiveness,
        'all_segments':  all_segments,
    }


# ── RF Recalculation ──────────────────────────────────────────────────────────

def _r_score(recency_days: int) -> int:
    if recency_days <= 14: return 4
    if recency_days <= 30: return 3
    if recency_days <= 60: return 2
    return 1


def _f_score(frequency: int) -> int:
    if frequency >= 6: return 3
    if frequency >= 4: return 2
    return 1


def recalculate_rf_scores(
    branch_ids: list[int] | None = None,
    mode: str = 'restaurant',
) -> dict:
    """
    Synchronously recalculate RF scores for all guests in the specified branches.

    For each ClientBranch:
      - Computes recency_days and frequency from visits (restaurant) or
        delivery activations (delivery).
      - Derives r_score / f_score from fixed thresholds.
      - Matches the guest to an RFSegment by recency/frequency boundaries.
      - Creates or updates GuestRFScore[Delivery].
      - Logs a migration entry if the segment changed.
    After scoring, refreshes today's BranchSegmentSnapshot[Delivery].

    Returns summary dict: {updated, created, migrations, branches, duration_ms}
    """
    import time
    from datetime import timedelta

    from django.db import transaction
    from django.db.models import Count, Max
    from django.utils import timezone

    from apps.tenant.analytics.models import (
        RFSegment,
        GuestRFScore, GuestRFScoreDelivery,
        RFMigrationLog, RFMigrationLogDelivery,
        BranchSegmentSnapshot, BranchSegmentSnapshotDelivery,
        RFSettings,
    )
    from apps.tenant.branch.models import Branch, ClientBranch, ClientBranchVisit

    t0 = time.monotonic()
    today = timezone.localdate()

    ScoreModel    = GuestRFScoreDelivery if mode == 'delivery' else GuestRFScore
    MigModel      = RFMigrationLogDelivery if mode == 'delivery' else RFMigrationLog
    SnapshotModel = BranchSegmentSnapshotDelivery if mode == 'delivery' else BranchSegmentSnapshot

    segments = list(RFSegment.objects.all())

    def find_segment(recency_days, frequency):
        for seg in segments:
            if (seg.recency_min <= recency_days <= seg.recency_max and
                    seg.frequency_min <= frequency <= seg.frequency_max):
                return seg
        return None

    branch_qs = Branch.objects.filter(is_active=True)
    if branch_ids:
        branch_qs = branch_qs.filter(pk__in=branch_ids)

    # ── Global analysis period: max across all branches ───────────────
    settings_qs = RFSettings.objects.all()
    if branch_ids:
        settings_qs = settings_qs.filter(branch__pk__in=branch_ids)

    agg = settings_qs.aggregate(max_period=Max('analysis_period'))
    analysis_period = agg['max_period'] or 365

    reset_date = None
    latest_reset = settings_qs.exclude(stats_reset_date=None).order_by('-stats_reset_date').first()
    if latest_reset:
        reset_date = latest_reset.stats_reset_date

    since = today - timedelta(days=analysis_period)
    if reset_date:
        reset_day = reset_date.date() if hasattr(reset_date, 'date') else reset_date
        if reset_day > since:
            since = reset_day

    # ── Aggregate visit/delivery data per unique guest.Client ─────────
    if mode == 'restaurant':
        from django.db.models import Exists, OuterRef, Q as _Q
        from apps.tenant.branch.models import (
            TestimonialMessage, CoinTransaction, TransactionType, TransactionSource,
        )
        from apps.tenant.game.models import ClientAttempt

        # Qualifying guest.Client IDs: those who did at least one meaningful action
        # (matches the definition used in get_qr_scan_count)
        qualifying_guest_ids = ClientBranch.objects.filter(
            _Q(vk_status__community_via_app=True)
            | _Q(vk_status__newsletter_via_app=True)
            | _Q(vk_status__is_story_uploaded=True)
            | Exists(ClientAttempt.objects.filter(client=OuterRef('pk')))
            | Exists(TestimonialMessage.objects.filter(
                conversation__client=OuterRef('pk'),
                source=TestimonialMessage.Source.APP,
            ))
            | Exists(CoinTransaction.objects.filter(
                client=OuterRef('pk'),
                type=TransactionType.EXPENSE,
                source=TransactionSource.SHOP,
            ))
        ).values('client_id')

        visit_qs = ClientBranchVisit.objects.filter(
            visited_at__date__gte=since,
            client__client_id__in=qualifying_guest_ids,
        )
        if branch_ids:
            visit_qs = visit_qs.filter(client__branch__pk__in=branch_ids)
        rows = (
            visit_qs
            .values('client__client_id')
            .annotate(freq=Count('id'), last_at=Max('visited_at'))
        )
        visit_map = {
            r['client__client_id']: {'frequency': r['freq'], 'last_at': r['last_at']}
            for r in rows
        }
    else:
        from apps.tenant.delivery.models import Delivery
        delivery_qs = Delivery.objects.filter(
            activated_at__isnull=False,
            activated_at__date__gte=since,
        )
        if branch_ids:
            delivery_qs = delivery_qs.filter(activated_by__branch__pk__in=branch_ids)
        rows = (
            delivery_qs
            .values('activated_by__client_id')
            .annotate(freq=Count('id'), last_at=Max('activated_at'))
        )
        visit_map = {
            r['activated_by__client_id']: {'frequency': r['freq'], 'last_at': r['last_at']}
            for r in rows
        }

    if not visit_map:
        return {
            'updated': 0, 'created': 0, 'migrations': 0,
            'branches': branch_qs.count(),
            'duration_ms': int((time.monotonic() - t0) * 1000),
        }

    # ── Load existing scores ──────────────────────────────────────────
    existing_scores = {
        s.client_id: s
        for s in ScoreModel.objects.filter(
            client_id__in=visit_map.keys(),
        ).select_related('segment')
    }

    total_updated = total_created = total_migrations = 0

    # ── Score each unique guest ───────────────────────────────────────
    with transaction.atomic():
        for guest_id, data in visit_map.items():
            last_at      = data['last_at']
            last_date    = last_at.date() if hasattr(last_at, 'date') else last_at
            recency_days = (today - last_date).days
            frequency    = data['frequency']
            r            = _r_score(recency_days)
            f            = _f_score(frequency)
            segment      = find_segment(recency_days, frequency)

            existing = existing_scores.get(guest_id)
            if existing:
                old_segment = existing.segment
                existing.recency_days = recency_days
                existing.frequency    = frequency
                existing.r_score      = r
                existing.f_score      = f
                existing.segment      = segment
                existing.save(update_fields=[
                    'recency_days', 'frequency', 'r_score', 'f_score', 'segment',
                ])
                total_updated += 1
                if old_segment != segment:
                    MigModel.objects.create(
                        client_id=guest_id,
                        from_segment=old_segment,
                        to_segment=segment,
                    )
                    total_migrations += 1
            else:
                ScoreModel.objects.update_or_create(
                    client_id=guest_id,
                    defaults={
                        'recency_days': recency_days,
                        'frequency':    frequency,
                        'r_score':      r,
                        'f_score':      f,
                        'segment':      segment,
                    },
                )
                total_created += 1

        # ── Refresh today's snapshot per branch ───────────────────────
        for branch in branch_qs:
            for seg in segments:
                count = (
                    ScoreModel.objects
                    .filter(segment=seg, client__branch_profiles__branch=branch)
                    .distinct()
                    .count()
                )
                SnapshotModel.objects.update_or_create(
                    branch=branch,
                    segment=seg,
                    date=today,
                    defaults={'guests_count': count},
                )

    return {
        'updated':     total_updated,
        'created':     total_created,
        'migrations':  total_migrations,
        'branches':    branch_qs.count(),
        'duration_ms': int((time.monotonic() - t0) * 1000),
    }


# ── Branch list helper ────────────────────────────────────────────────────────

def get_branches_list() -> list[dict]:
    """All branches for the filter UI."""
    from apps.tenant.branch.models import Branch

    return list(
        Branch.objects.filter(is_active=True).values('id', 'name').order_by('name')
    )


# ── Stat detail: returns ClientBranch queryset for a given metric ─────────────

def get_stat_clients(
    metric: str,
    branch_ids: list[int] | None,
    start_date: date,
    end_date: date,
):
    """
    Returns a ClientBranch queryset whose members contributed to `metric`
    in the given period. Used by StatsDetailView to render the drilldown list.

    Unsupported or non-client metrics return an empty queryset.
    """
    from django.db.models import Q as _Q
    from apps.tenant.branch.models import (
        ClientBranch, ClientBranchVisit, ClientVKStatus, CoinTransaction,
        TransactionType, TransactionSource,
    )

    base = ClientBranch.objects.select_related('client', 'branch', 'vk_status').order_by(
        'client__first_name', 'client__last_name'
    )
    if branch_ids:
        base = base.filter(branch__in=branch_ids)

    if metric == 'qr_scans':
        from django.db.models import Exists, OuterRef
        from apps.tenant.game.models import ClientAttempt
        from apps.tenant.branch.models import TestimonialMessage
        from apps.tenant.delivery.models import Delivery

        qs = ClientBranchVisit.objects.filter(
            visited_at__date__gte=start_date,
            visited_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)
        return base.filter(
            pk__in=qs.values('client_id'),
        ).filter(
            _Q(vk_status__community_via_app=True)
            | _Q(vk_status__newsletter_via_app=True)
            | _Q(vk_status__is_story_uploaded=True)
            | Exists(ClientAttempt.objects.filter(
                client=OuterRef('pk'),
                created_at__date__gte=start_date,
                created_at__date__lte=end_date,
            ))
            | Exists(TestimonialMessage.objects.filter(
                conversation__client=OuterRef('pk'),
                source=TestimonialMessage.Source.APP,
                created_at__date__gte=start_date,
                created_at__date__lte=end_date,
            ))
            | Exists(Delivery.objects.filter(
                activated_by=OuterRef('pk'),
                activated_at__date__gte=start_date,
                activated_at__date__lte=end_date,
            ))
            | Exists(CoinTransaction.objects.filter(
                client=OuterRef('pk'),
                type=TransactionType.EXPENSE,
                source=TransactionSource.SHOP,
                created_at__date__gte=start_date,
                created_at__date__lte=end_date,
            ))
        )

    if metric == 'total_vk_subscribers':
        qs = ClientVKStatus.objects.filter(
            _Q(community_via_app=True) | _Q(newsletter_via_app=True)
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)
        return base.filter(pk__in=qs.values('client_id'))

    if metric == 'new_community_subscribers':
        qs = ClientVKStatus.objects.filter(
            community_via_app=True,
            community_joined_at__date__gte=start_date,
            community_joined_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)
        return base.filter(pk__in=qs.values('client_id'))

    if metric == 'new_newsletter_subscribers':
        qs = ClientVKStatus.objects.filter(
            newsletter_via_app=True,
            newsletter_joined_at__date__gte=start_date,
            newsletter_joined_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)
        return base.filter(pk__in=qs.values('client_id'))

    if metric == 'first_gift_receivers':
        from apps.tenant.inventory.models import InventoryItem

        qs = InventoryItem.objects.all()
        if branch_ids:
            qs = qs.filter(client_branch__branch__in=branch_ids)
        first_items = (
            qs.values('client_branch')
            .annotate(first_at=Min('created_at'))
            .filter(
                first_at__date__gte=start_date,
                first_at__date__lte=end_date,
            )
        )
        cb_ids = [r['client_branch'] for r in first_items]
        return base.filter(pk__in=cb_ids)

    if metric == 'gift_activators':
        from apps.tenant.inventory.models import InventoryItem

        qs = InventoryItem.objects.filter(
            activated_at__date__gte=start_date,
            activated_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client_branch__branch__in=branch_ids)
        return base.filter(pk__in=qs.values('client_branch_id'))

    if metric == 'coin_purchasers':
        qs = CoinTransaction.objects.filter(
            type=TransactionType.EXPENSE,
            source=TransactionSource.SHOP,
            created_at__date__gte=start_date,
            created_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)
        return base.filter(pk__in=qs.values('client_id'))

    if metric == 'repeat_game_players':
        from apps.tenant.game.models import ClientAttempt
        from django.db.models.functions import TruncDate as _TruncDate

        qs = ClientAttempt.objects.filter(
            created_at__date__gte=start_date,
            created_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)

        pairs = (
            qs.annotate(play_date=_TruncDate('created_at'))
            .values_list('client_id', 'play_date')
            .distinct()
        )
        client_days: dict = {}
        for cb_id, play_date in pairs:
            client_days.setdefault(cb_id, set()).add(play_date)
        cb_ids = [k for k, v in client_days.items() if len(v) >= 2]
        return base.filter(pk__in=cb_ids)

    if metric == 'new_group_with_gift':
        from apps.tenant.inventory.models import SuperPrizeEntry, SuperPrizeTrigger

        vk_qs = ClientVKStatus.objects.filter(
            _Q(community_via_app=True, community_joined_at__date__gte=start_date,
               community_joined_at__date__lte=end_date) |
            _Q(newsletter_via_app=True, newsletter_joined_at__date__gte=start_date,
               newsletter_joined_at__date__lte=end_date)
        )
        if branch_ids:
            vk_qs = vk_qs.filter(client__branch__in=branch_ids)
        sub_ids = set(vk_qs.values_list('client_id', flat=True))
        if not sub_ids:
            return base.none()

        first_prizes = (
            SuperPrizeEntry.objects
            .filter(
                acquired_from=SuperPrizeTrigger.GAME,
                client_branch__in=sub_ids,
            )
            .values('client_branch')
            .annotate(first_at=Min('created_at'))
            .filter(
                first_at__date__gte=start_date,
                first_at__date__lte=end_date,
            )
        )
        cb_ids = [r['client_branch'] for r in first_prizes]
        return base.filter(pk__in=cb_ids)

    if metric == 'birthday_celebrants':
        from django.db.models import F
        from django.db.models.functions import ExtractMonth, ExtractDay
        from apps.tenant.branch.models import ClientBranchVisit

        visits_qs = ClientBranchVisit.objects.filter(
            visited_at__date__gte=start_date,
            visited_at__date__lte=end_date,
            client__birth_date__isnull=False,
        ).annotate(
            visit_month=ExtractMonth('visited_at'),
            visit_day=ExtractDay('visited_at'),
            birth_month=ExtractMonth('client__birth_date'),
            birth_day=ExtractDay('client__birth_date'),
        ).filter(
            visit_month=F('birth_month'),
            visit_day=F('birth_day'),
        )
        if branch_ids:
            visits_qs = visits_qs.filter(client__branch__in=branch_ids)
        cb_ids = visits_qs.values_list('client_id', flat=True).distinct()
        return base.filter(pk__in=cb_ids)

    if metric == 'vk_stories_publishers':
        qs = ClientVKStatus.objects.filter(
            is_story_uploaded=True,
            story_uploaded_at__date__gte=start_date,
            story_uploaded_at__date__lte=end_date,
        )
        if branch_ids:
            qs = qs.filter(client__branch__in=branch_ids)
        return base.filter(pk__in=qs.values('client_id'))

    return base.none()