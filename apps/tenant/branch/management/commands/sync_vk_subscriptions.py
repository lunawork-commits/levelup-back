"""
Синхронизация VK-статусов подписки гостей.

Проходит по всей таблице ClientVKStatus в указанной PostgreSQL-схеме
и сверяет данные с VK API (groups.isMember + messages.isMessagesFromGroupAllowed).

Использование:
  python manage.py sync_vk_subscriptions --schema levone
  python manage.py sync_vk_subscriptions --schema levone --dry-run
  python manage.py sync_vk_subscriptions --schema levone --dry-run --batch-size 200

Правила синхронизации:
  1) VK: подписан,     БД: подписан     → не трогаем
  2) VK: НЕ подписан,  БД: подписан     → обнуляем все поля подписки
  3) VK: подписан,      БД: НЕ подписан  → ставим только is_*=True, остальные не трогаем
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

logger = logging.getLogger(__name__)

# ── VK API ───────────────────────────────────────────────────────────────────

VK_API_VERSION = '5.131'
VK_API_BASE = 'https://api.vk.com/method/'
VK_RATE_LIMIT_DELAY = 0.35  # VK допускает ~3 запроса/с для токена сообщества


def _vk_call(method: str, token: str, **params) -> dict:
    """Синхронный вызов VK API. Raises RuntimeError при ошибках."""
    params['access_token'] = token
    params['v'] = VK_API_VERSION
    url = VK_API_BASE + method + '?' + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as e:
        raise RuntimeError(f'Network error calling VK {method}: {e}') from e

    if 'error' in data:
        err = data['error']
        raise RuntimeError(
            f'VK API {method} error {err.get("error_code")}: {err.get("error_msg")}'
        )

    return data.get('response', {})


def _vk_batch_is_member(token: str, group_id: int, vk_ids: list[int]) -> set[int]:
    """
    Пакетная проверка подписки на сообщество через groups.isMember.
    Возвращает set vk_id тех, кто является участником.
    """
    if not vk_ids:
        return set()

    user_ids_str = ','.join(str(uid) for uid in vk_ids)
    resp = _vk_call(
        'groups.isMember', token,
        group_id=group_id,
        user_ids=user_ids_str,
        extended=0,
    )

    # resp — список: [{"user_id": 123, "member": 1}, ...]
    if isinstance(resp, list):
        return {item['user_id'] for item in resp if item.get('member')}
    return set()


def _vk_is_messages_allowed(token: str, group_id: int, vk_id: int) -> bool:
    """
    Проверка подписки на рассылку через messages.isMessagesFromGroupAllowed.
    К сожалению, этот метод не поддерживает пакетные запросы.
    """
    resp = _vk_call(
        'messages.isMessagesFromGroupAllowed', token,
        group_id=group_id,
        user_id=vk_id,
    )
    return bool(resp.get('is_allowed', 0))


# ── Management command ───────────────────────────────────────────────────────

class Command(BaseCommand):
    help = (
        'Синхронизация VK-статусов подписки гостей. '
        'Проверяет подписку на сообщество (groups.isMember) и '
        'разрешение на сообщения (messages.isMessagesFromGroupAllowed) '
        'для каждой записи в ClientVKStatus.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--schema',
            required=True,
            help='PostgreSQL-схема тенанта (например: levone)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            default=False,
            help='Не вносить изменений в БД — только показать, что было бы изменено.',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=500,
            help='Размер пакета для groups.isMember (по умолчанию 500).',
        )

    def handle(self, *args, **options):
        schema = options['schema']
        dry_run = options['dry_run']
        batch_size = options['batch_size']

        from django_tenants.utils import schema_context

        mode_label = '🔍 DRY-RUN' if dry_run else '⚡ LIVE'
        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(f'  VK Subscription Sync — {mode_label}')
        self.stdout.write(f'  Схема: {schema}')
        self.stdout.write(f'  Время: {datetime.now():%Y-%m-%d %H:%M:%S}')
        self.stdout.write(f'{"=" * 60}\n')

        try:
            with schema_context(schema):
                self._sync(dry_run=dry_run, batch_size=batch_size)
        except Exception as e:
            raise CommandError(f'Ошибка: {e}')

    def _sync(self, *, dry_run: bool, batch_size: int):
        from apps.tenant.branch.models import ClientVKStatus
        from apps.tenant.senler.models import SenlerConfig

        # ── Собираем конфиги VK для каждого branch ───────────────────────
        configs = SenlerConfig.objects.filter(is_active=True).select_related('branch')
        if not configs.exists():
            self.stdout.write(self.style.WARNING('Нет активных SenlerConfig — нечего синхронизировать.'))
            return

        # Маппинг branch_id → (token, group_id, branch_name)
        branch_vk: dict[int, tuple[str, int, str]] = {}
        for cfg in configs:
            if cfg.vk_community_token and cfg.vk_group_id:
                branch_vk[cfg.branch_id] = (
                    cfg.vk_community_token,
                    cfg.vk_group_id,
                    str(cfg.branch),
                )

        if not branch_vk:
            self.stdout.write(self.style.WARNING('Нет SenlerConfig с токеном и group_id.'))
            return

        self.stdout.write(f'Найдено конфигураций VK: {len(branch_vk)}')
        for bid, (_, gid, name) in branch_vk.items():
            self.stdout.write(f'  • branch_id={bid} group_id={gid} ({name})')

        # ── Загружаем все VK-статусы с FK на client и client.client ──────
        all_statuses = list(
            ClientVKStatus.objects
            .select_related('client', 'client__client', 'client__branch')
            .all()
        )

        if not all_statuses:
            self.stdout.write(self.style.WARNING('Таблица ClientVKStatus пуста.'))
            return

        self.stdout.write(f'Всего записей ClientVKStatus: {len(all_statuses)}\n')

        # ── Группируем по branch ─────────────────────────────────────────
        by_branch: dict[int, list[ClientVKStatus]] = {}
        skipped_no_vk = 0
        skipped_no_config = 0

        for status in all_statuses:
            branch_id = status.client.branch_id

            if branch_id not in branch_vk:
                skipped_no_config += 1
                continue

            vk_id = status.client.client.vk_id
            if not vk_id:
                skipped_no_vk += 1
                continue

            by_branch.setdefault(branch_id, []).append(status)

        if skipped_no_config:
            self.stdout.write(self.style.WARNING(
                f'Пропущено (нет VK-конфига для branch): {skipped_no_config}'
            ))
        if skipped_no_vk:
            self.stdout.write(self.style.WARNING(
                f'Пропущено (нет vk_id у гостя): {skipped_no_vk}'
            ))

        # ── Счётчики ─────────────────────────────────────────────────────
        stats = {
            'unchanged': 0,        # Случай 1: совпадает — не трогаем
            'unsubscribed': 0,     # Случай 2: VK не подписан, БД подписан → обнуляем
            'subscribed': 0,       # Случай 3: VK подписан, БД не подписан → ставим true
            'errors': 0,
        }

        # ── Обработка по branch ──────────────────────────────────────────
        for branch_id, statuses in by_branch.items():
            token, group_id, branch_name = branch_vk[branch_id]

            self.stdout.write(f'\n{"─" * 50}')
            self.stdout.write(f'Branch: {branch_name} (group_id={group_id})')
            self.stdout.write(f'Записей для проверки: {len(statuses)}')
            self.stdout.write(f'{"─" * 50}')

            # Маппинг vk_id → ClientVKStatus для быстрого доступа
            vk_to_status: dict[int, ClientVKStatus] = {}
            for s in statuses:
                vk_to_status[s.client.client.vk_id] = s

            all_vk_ids = list(vk_to_status.keys())

            # ── Шаг 1: Пакетная проверка подписки на сообщество ──────────
            member_set: set[int] = set()
            for i in range(0, len(all_vk_ids), batch_size):
                batch = all_vk_ids[i:i + batch_size]
                try:
                    members = _vk_batch_is_member(token, group_id, batch)
                    member_set.update(members)
                    self.stdout.write(
                        f'  groups.isMember batch {i // batch_size + 1}: '
                        f'{len(batch)} проверено, {len(members)} подписаны'
                    )
                except RuntimeError as e:
                    self.stdout.write(self.style.ERROR(f'  Ошибка groups.isMember: {e}'))
                    stats['errors'] += len(batch)
                    continue
                time.sleep(VK_RATE_LIMIT_DELAY)

            # ── Шаг 2: Индивидуальная проверка рассылки ──────────────────
            newsletter_set: set[int] = set()
            self.stdout.write(f'  Проверка messages.isMessagesFromGroupAllowed ({len(all_vk_ids)} запросов)...')

            for idx, vk_id in enumerate(all_vk_ids, 1):
                try:
                    if _vk_is_messages_allowed(token, group_id, vk_id):
                        newsletter_set.add(vk_id)
                except RuntimeError as e:
                    self.stdout.write(self.style.ERROR(
                        f'  Ошибка isMessagesAllowed vk_id={vk_id}: {e}'
                    ))
                    stats['errors'] += 1

                # Прогресс каждые 100 записей
                if idx % 100 == 0:
                    self.stdout.write(f'    ... {idx}/{len(all_vk_ids)} проверено')

                time.sleep(VK_RATE_LIMIT_DELAY)

            self.stdout.write(
                f'  Результат: {len(member_set)} в сообществе, '
                f'{len(newsletter_set)} подписаны на рассылку'
            )

            # ── Шаг 3: Применяем правила ─────────────────────────────────
            now = timezone.now()

            for vk_id, status in vk_to_status.items():
                vk_is_member = vk_id in member_set
                vk_is_subscriber = vk_id in newsletter_set
                db_is_member = status.is_community_member
                db_is_subscriber = status.is_newsletter_subscriber

                guest_name = (
                    f'{status.client.client.first_name} '
                    f'{status.client.client.last_name}'.strip()
                ) or f'vk{vk_id}'

                # Определяем, что нужно изменить для community
                community_action = None  # 'reset' | 'set' | None
                if vk_is_member and db_is_member:
                    pass  # Случай 1
                elif not vk_is_member and db_is_member:
                    community_action = 'reset'  # Случай 2
                elif vk_is_member and not db_is_member:
                    community_action = 'set'  # Случай 3

                # Определяем, что нужно изменить для newsletter
                newsletter_action = None  # 'reset' | 'set' | None
                if vk_is_subscriber and db_is_subscriber:
                    pass  # Случай 1
                elif not vk_is_subscriber and db_is_subscriber:
                    newsletter_action = 'reset'  # Случай 2
                elif vk_is_subscriber and not db_is_subscriber:
                    newsletter_action = 'set'  # Случай 3

                # Нет изменений — пропускаем
                if community_action is None and newsletter_action is None:
                    stats['unchanged'] += 1
                    continue

                # Формируем описание изменений
                changes: list[str] = []
                update_fields: list[str] = ['checked_at']

                # ── Случай 2: VK не подписан, БД подписан → обнуляем ─────
                if community_action == 'reset':
                    changes.append('сообщество: ОТПИСАН → обнуляем')
                    status.is_community_member = False
                    status.community_joined_at = None
                    status.community_via_app = False
                    update_fields += [
                        'is_community_member',
                        'community_joined_at',
                        'community_via_app',
                    ]
                    stats['unsubscribed'] += 1

                if newsletter_action == 'reset':
                    changes.append('рассылка: ОТПИСАН → обнуляем')
                    status.is_newsletter_subscriber = False
                    status.newsletter_joined_at = None
                    status.newsletter_via_app = False
                    update_fields += [
                        'is_newsletter_subscriber',
                        'newsletter_joined_at',
                        'newsletter_via_app',
                    ]
                    stats['unsubscribed'] += 1

                # ── Случай 3: VK подписан, БД не подписан → только is_*=True
                if community_action == 'set':
                    changes.append('сообщество: ПОДПИСАН → ставим is_community_member=True')
                    status.is_community_member = True
                    update_fields += ['is_community_member']
                    stats['subscribed'] += 1

                if newsletter_action == 'set':
                    changes.append('рассылка: ПОДПИСАН → ставим is_newsletter_subscriber=True')
                    status.is_newsletter_subscriber = True
                    update_fields += ['is_newsletter_subscriber']
                    stats['subscribed'] += 1

                # Лог
                action_str = '; '.join(changes)
                if dry_run:
                    self.stdout.write(self.style.WARNING(
                        f'  [DRY-RUN] {guest_name} (vk{vk_id}): {action_str}'
                    ))
                else:
                    status.save(update_fields=update_fields)
                    self.stdout.write(self.style.SUCCESS(
                        f'  [SAVED] {guest_name} (vk{vk_id}): {action_str}'
                    ))

        # ── Итог ─────────────────────────────────────────────────────────
        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write('  ИТОГ:')
        self.stdout.write(f'    Без изменений (совпадает):     {stats["unchanged"]}')
        self.stdout.write(f'    Обнулено (отписались):         {stats["unsubscribed"]}')
        self.stdout.write(f'    Проставлено (подписались):     {stats["subscribed"]}')
        self.stdout.write(f'    Ошибок VK API:                 {stats["errors"]}')
        if dry_run:
            self.stdout.write(self.style.WARNING('    ⚠️  Режим DRY-RUN: изменения НЕ сохранены'))
        else:
            self.stdout.write(self.style.SUCCESS('    ✅ Изменения сохранены в БД'))
        self.stdout.write(f'{"=" * 60}\n')
