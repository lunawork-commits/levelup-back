"""
Management command — sync VK community membership via VK API groups.isMember.

Checks real subscription state from VK and fixes the DB:
  - is_community_member = actual VK state
  - community_via_app:
      True  = subscribed through our mini-app  (already tracked, preserved)
      False = subscribed externally / pre-existing (set for all newly found members)
      None  = not subscribed

NOTE: VK API cannot tell HOW someone subscribed — only IF they are subscribed.
      All users found subscribed in VK but without via_app attribution get via_app=False.
      Going forward, v5 mini-app will correctly set via_app=True on new subscriptions.

Usage:
    # Dry run — show changes without touching DB:
    python manage.py sync_vk_status --schema levone --group-id 12345 --token vk1.a.xxx --dry-run

    # Apply changes:
    python manage.py sync_vk_status --schema levone --group-id 12345 --token vk1.a.xxx

    # All tenants (reads token from SenlerConfig):
    python manage.py sync_vk_status
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

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


def check_members(group_id: int, user_ids: list[int], token: str) -> dict[int, bool]:
    """Returns {vk_id: is_member} via groups.isMember (batched)."""
    result = {}
    total = len(user_ids)
    for i in range(0, total, BATCH_SIZE):
        batch = user_ids[i:i + BATCH_SIZE]
        resp = _vk_call(
            'groups.isMember',
            token,
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


def check_messages_allowed(group_id: int, user_ids: list[int], token: str) -> dict[int, bool]:
    """Returns {vk_id: is_allowed} via messages.isMessagesFromGroupAllowed (per user)."""
    result = {}
    for vk_id in user_ids:
        try:
            resp = _vk_call(
                'messages.isMessagesFromGroupAllowed',
                token,
                group_id=group_id,
                user_id=vk_id,
            )
            result[vk_id] = bool(resp.get('is_allowed', 0))
        except RuntimeError:
            result[vk_id] = False
        time.sleep(0.35)
    return result


def sync_schema(schema: str, group_id: int, token: str, dry_run: bool, stdout, style):
    from django.utils import timezone
    from apps.tenant.branch.models import ClientBranch, ClientVKStatus

    with schema_context(schema):
        cbs = list(
            ClientBranch.objects
            .select_related('client')
            .filter(client__vk_id__isnull=False)
        )

        if not cbs:
            stdout.write(f'  [{schema}] No guests with vk_id — skipping.')
            return

        vk_id_to_cbs: dict[int, list] = {}
        for cb in cbs:
            vk_id = cb.client.vk_id
            if vk_id:
                vk_id_to_cbs.setdefault(vk_id, []).append(cb)

        all_vk_ids = list(vk_id_to_cbs.keys())

        # ── Community membership ──────────────────────────────────────────
        stdout.write(
            f'  [{schema}] Checking community membership for '
            f'{len(all_vk_ids)} users in group {group_id}...'
        )

        try:
            member_map = check_members(group_id, all_vk_ids, token)
        except RuntimeError as e:
            stdout.write(style.ERROR(f'  [{schema}] groups.isMember error: {e}'))
            return

        subscribed_vk = sum(1 for v in member_map.values() if v)
        stdout.write(
            f'  [{schema}] Community: {subscribed_vk} subscribed, '
            f'{len(member_map) - subscribed_vk} not subscribed'
        )

        # ── Newsletter (messages allowed) ────────────────────────────────
        stdout.write(
            f'  [{schema}] Checking newsletter (messages allowed) for '
            f'{len(all_vk_ids)} users...'
        )

        allowed_map = check_messages_allowed(group_id, all_vk_ids, token)

        allowed_vk = sum(1 for v in allowed_map.values() if v)
        stdout.write(
            f'  [{schema}] Newsletter: {allowed_vk} allowed, '
            f'{len(allowed_map) - allowed_vk} not allowed'
        )

        # ── Apply changes ────────────────────────────────────────────────
        now = timezone.now()
        comm_added = comm_removed = comm_kept = 0
        news_added = news_removed = news_kept = 0

        for vk_id, client_branches in vk_id_to_cbs.items():
            is_member = member_map.get(vk_id, False)
            is_allowed = allowed_map.get(vk_id, False)

            for cb in client_branches:
                vk_status, _ = ClientVKStatus.objects.get_or_create(client=cb)
                update_fields: list[str] = []

                # Community
                if is_member and not vk_status.is_community_member:
                    comm_added += 1
                    if not dry_run:
                        vk_status.is_community_member = True
                        vk_status.community_joined_at = vk_status.community_joined_at or now
                        if vk_status.community_via_app is None:
                            vk_status.community_via_app = False
                        update_fields += [
                            'is_community_member', 'community_joined_at', 'community_via_app',
                        ]
                elif not is_member and vk_status.is_community_member:
                    comm_removed += 1
                    if not dry_run:
                        vk_status.is_community_member = False
                        vk_status.community_joined_at = None
                        vk_status.community_via_app = None
                        update_fields += [
                            'is_community_member', 'community_joined_at', 'community_via_app',
                        ]
                else:
                    comm_kept += 1

                # Newsletter
                if is_allowed and not vk_status.is_newsletter_subscriber:
                    news_added += 1
                    if not dry_run:
                        vk_status.is_newsletter_subscriber = True
                        vk_status.newsletter_joined_at = vk_status.newsletter_joined_at or now
                        if vk_status.newsletter_via_app is None:
                            vk_status.newsletter_via_app = False
                        update_fields += [
                            'is_newsletter_subscriber', 'newsletter_joined_at', 'newsletter_via_app',
                        ]
                elif not is_allowed and vk_status.is_newsletter_subscriber:
                    news_removed += 1
                    if not dry_run:
                        vk_status.is_newsletter_subscriber = False
                        vk_status.newsletter_joined_at = None
                        vk_status.newsletter_via_app = None
                        update_fields += [
                            'is_newsletter_subscriber', 'newsletter_joined_at', 'newsletter_via_app',
                        ]
                else:
                    news_kept += 1

                if update_fields and not dry_run:
                    vk_status.save(update_fields=update_fields)

        suffix = ' [DRY RUN — nothing written]' if dry_run else ''
        stdout.write(style.SUCCESS(
            f'  [{schema}] Community: +{comm_added} added, '
            f'-{comm_removed} removed, {comm_kept} ok{suffix}'
        ))
        stdout.write(style.SUCCESS(
            f'  [{schema}] Newsletter: +{news_added} added, '
            f'-{news_removed} removed, {news_kept} ok{suffix}'
        ))


class Command(BaseCommand):
    help = 'Fix VK community subscription data using VK API groups.isMember'

    def add_arguments(self, parser):
        parser.add_argument('--schema',   type=str, help='Tenant schema (default: all)')
        parser.add_argument('--group-id', type=int, help='VK group ID (overrides SenlerConfig)')
        parser.add_argument('--token',    type=str, help='VK community token (overrides SenlerConfig)')
        parser.add_argument('--dry-run',  action='store_true',
                            help='Show what would change without writing to DB')

    def handle(self, *args, **options):
        from apps.shared.clients.models import Company
        from apps.tenant.senler.models import SenlerConfig

        schema_filter  = options.get('schema')
        override_gid   = options.get('group_id')
        override_token = options.get('token')
        dry_run        = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN — no DB changes will be made.\n'))

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
                        f'  [{schema}] No SenlerConfig with group_id + token. '
                        'Pass --group-id and --token to override.'
                    ))
                    continue
                group_id = cfg.vk_group_id
                token    = cfg.vk_community_token

            sync_schema(schema, group_id, token, dry_run, self.stdout, self.style)

        self.stdout.write(self.style.SUCCESS('\nDone.'))
