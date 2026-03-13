"""
Management command — полная пересборка VK-статуса гостей с нуля.

Порядок шагов:
  1. Сбрасываем все VK-статусы в "не подписан" (community_via_app=None, is_community_member=False, ...)
  2. Синхронизируем с VK API (groups.isMember) — кто сейчас подписан получает is_community_member=True, community_via_app=False
  3. Из подписанных — у кого есть SuperPrizeEntry(game) → community_via_app=True, дата = первый суперприз
  4. У кого суперприз есть, но VK говорит "не подписан" → оставляем как не подписан (отписались)

  Для рассылки (newsletter): VK API не даёт проверить подписку на рассылку напрямую.
  Поэтому: у кого есть суперприз → newsletter_via_app=True (логика игры требовала подписку),
  дата = первый суперприз. Если VK сообщает что не в группе (т.е. в целом неактивен) — тоже True,
  т.к. мог быть в рассылке и отписаться от группы отдельно.

Usage:
    # Dry run:
    python manage.py rebuild_vk_status --schema levone --dry-run

    # Применить:
    python manage.py rebuild_vk_status --schema levone

    # Все тенанты (токен берётся из SenlerConfig):
    python manage.py rebuild_vk_status
"""
import json
import time
import urllib.parse
import urllib.request
import urllib.error

from django.core.management.base import BaseCommand
from django_tenants.utils import schema_context

VK_API_BASE    = 'https://api.vk.com/method/'
VK_API_VERSION = '5.131'
BATCH_SIZE     = 500


def _vk_call(method: str, token: str, **params) -> dict:
    params['access_token'] = token
    params['v']            = VK_API_VERSION
    url = VK_API_BASE + method + '?' + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as e:
        raise RuntimeError(f'Network error: {e}') from e
    if 'error' in data:
        err = data['error']
        raise RuntimeError(f'VK error {err.get("error_code")}: {err.get("error_msg")}')
    return data.get('response', {})


def check_members(group_id: int, user_ids: list, token: str) -> dict:
    """Returns {vk_id: is_member} via groups.isMember (batched)."""
    result = {}
    for i in range(0, len(user_ids), BATCH_SIZE):
        batch = user_ids[i:i + BATCH_SIZE]
        resp = _vk_call(
            'groups.isMember', token,
            group_id=group_id,
            user_ids=','.join(str(u) for u in batch),
            extended=1,
        )
        if isinstance(resp, list):
            for item in resp:
                result[item['user_id']] = bool(item.get('member', 0))
        elif isinstance(resp, int):
            result[batch[0]] = bool(resp)
        time.sleep(0.35)
    return result


def rebuild_schema(schema: str, group_id: int, token: str, dry_run: bool, stdout, style):
    from django.utils import timezone
    from django.db.models import Min
    from apps.tenant.branch.models import ClientBranch, ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry

    with schema_context(schema):

        # ── Шаг 1: сброс всех статусов ───────────────────────────────────────
        stdout.write(f'  [{schema}] Шаг 1: сброс всех VK-статусов...')
        total_reset = ClientVKStatus.objects.count()
        if not dry_run:
            ClientVKStatus.objects.all().update(
                is_community_member=False,
                community_joined_at=None,
                community_via_app=None,
                is_newsletter_subscriber=False,
                newsletter_joined_at=None,
                newsletter_via_app=None,
            )
        stdout.write(f'  [{schema}]   {total_reset} записей сброшено{"" if not dry_run else " [DRY RUN]"}')

        # ── Шаг 2: синхронизация с VK API ────────────────────────────────────
        stdout.write(f'  [{schema}] Шаг 2: синхронизация с VK API (group {group_id})...')

        cbs = list(
            ClientBranch.objects
            .select_related('client')
            .filter(client__vk_id__isnull=False)
        )
        vk_id_to_cbs: dict = {}
        for cb in cbs:
            vk_id = cb.client.vk_id
            if vk_id:
                vk_id_to_cbs.setdefault(vk_id, []).append(cb)

        if not vk_id_to_cbs:
            stdout.write(style.WARNING(f'  [{schema}] Нет гостей с vk_id — пропускаем.'))
            return

        try:
            member_map = check_members(group_id, list(vk_id_to_cbs.keys()), token)
        except RuntimeError as e:
            stdout.write(style.ERROR(f'  [{schema}] VK API ошибка: {e}'))
            return

        now = timezone.now()
        vk_member_cb_ids = set()  # ClientBranch IDs текущих подписчиков группы

        for vk_id, client_branches in vk_id_to_cbs.items():
            is_member = member_map.get(vk_id, False)
            for cb in client_branches:
                vk_status, _ = ClientVKStatus.objects.get_or_create(client=cb)
                if is_member:
                    vk_member_cb_ids.add(cb.id)
                    if not dry_run:
                        vk_status.is_community_member = True
                        vk_status.community_via_app = False  # пока неизвестно — через приложение или нет
                        vk_status.save(update_fields=['is_community_member', 'community_via_app'])

        subscribed_count = sum(1 for v in member_map.values() if v)
        stdout.write(f'  [{schema}]   VK: {subscribed_count} подписаны, {len(member_map) - subscribed_count} нет')

        # ── Шаг 3: атрибуция через игру ──────────────────────────────────────
        stdout.write(f'  [{schema}] Шаг 3: атрибуция через суперприз...')

        game_prize_cb_ids = set(
            SuperPrizeEntry.objects
            .filter(acquired_from='game')
            .values_list('client_branch_id', flat=True)
            .distinct()
        )

        first_prize_dates = dict(
            SuperPrizeEntry.objects
            .filter(acquired_from='game', client_branch_id__in=game_prize_cb_ids)
            .values('client_branch_id')
            .annotate(first_at=Min('created_at'))
            .values_list('client_branch_id', 'first_at')
        )

        # Подписаны в VK + есть суперприз → через приложение
        via_app_ids    = game_prize_cb_ids & vk_member_cb_ids
        # Есть суперприз, но не в VK → отписались
        unsubscribed_ids = game_prize_cb_ids - vk_member_cb_ids

        stdout.write(
            f'  [{schema}]   {len(via_app_ids)} подписаны через приложение, '
            f'{len(unsubscribed_ids)} были подписаны (отписались)'
        )

        if not dry_run:
            for cb_id in via_app_ids:
                prize_date = first_prize_dates.get(cb_id, now)
                try:
                    vk_status = ClientVKStatus.objects.get(client_id=cb_id)
                    vk_status.community_via_app    = True
                    vk_status.community_joined_at  = prize_date
                    vk_status.newsletter_via_app   = True
                    vk_status.newsletter_joined_at = prize_date
                    vk_status.is_newsletter_subscriber = True
                    vk_status.save(update_fields=[
                        'community_via_app', 'community_joined_at',
                        'newsletter_via_app', 'newsletter_joined_at',
                        'is_newsletter_subscriber',
                    ])
                except ClientVKStatus.DoesNotExist:
                    cb = ClientBranch.objects.get(id=cb_id)
                    ClientVKStatus.objects.create(
                        client=cb,
                        is_community_member=True,
                        community_joined_at=prize_date,
                        community_via_app=True,
                        is_newsletter_subscriber=True,
                        newsletter_joined_at=prize_date,
                        newsletter_via_app=True,
                    )

            # Для тех кто отписался от группы но играл — newsletter ставим True
            # (могли подписаться на рассылку отдельно)
            for cb_id in unsubscribed_ids:
                prize_date = first_prize_dates.get(cb_id, now)
                try:
                    vk_status = ClientVKStatus.objects.get(client_id=cb_id)
                    vk_status.newsletter_via_app   = True
                    vk_status.newsletter_joined_at = prize_date
                    vk_status.is_newsletter_subscriber = True
                    vk_status.save(update_fields=[
                        'newsletter_via_app', 'newsletter_joined_at',
                        'is_newsletter_subscriber',
                    ])
                except ClientVKStatus.DoesNotExist:
                    pass

        suffix = ' [DRY RUN]' if dry_run else ''
        stdout.write(style.SUCCESS(f'  [{schema}] Готово{suffix}'))


class Command(BaseCommand):
    help = 'Полная пересборка VK-статуса: сброс → VK sync → атрибуция через игру'

    def add_arguments(self, parser):
        parser.add_argument('--schema',   type=str, help='Tenant schema (default: all)')
        parser.add_argument('--group-id', type=int, help='VK group ID (override SenlerConfig)')
        parser.add_argument('--token',    type=str, help='VK token (override SenlerConfig)')
        parser.add_argument('--dry-run',  action='store_true')

    def handle(self, *args, **options):
        from apps.shared.clients.models import Company
        from apps.tenant.senler.models import SenlerConfig

        schema_filter  = options.get('schema')
        override_gid   = options.get('group_id')
        override_token = options.get('token')
        dry_run        = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN — ничего не записывается\n'))

        tenants = Company.objects.exclude(schema_name='public')
        if schema_filter:
            tenants = tenants.filter(schema_name=schema_filter)

        if not tenants.exists():
            self.stdout.write(self.style.ERROR('No matching tenants found.'))
            return

        for company in tenants:
            schema = company.schema_name
            self.stdout.write(f'\nTenant: {company.name} ({schema})')

            if override_gid and override_token:
                group_id = override_gid
                token    = override_token
            else:
                with schema_context(schema):
                    cfg = SenlerConfig.objects.first()
                if not cfg or not cfg.vk_group_id or not cfg.vk_community_token:
                    self.stdout.write(self.style.WARNING(
                        f'  [{schema}] Нет SenlerConfig. Передай --group-id и --token.'
                    ))
                    continue
                group_id = cfg.vk_group_id
                token    = cfg.vk_community_token

            rebuild_schema(schema, group_id, token, dry_run, self.stdout, self.style)

        self.stdout.write(self.style.SUCCESS('\nDone.'))
