"""
Management command — fix community_via_app / newsletter_via_app based on super prize history.

Logic:
    If a guest has SuperPrizeEntry(acquired_from='game') → they played the game
    and the game required subscribing to community + newsletter via mini-app.
    Therefore: community_via_app=True, newsletter_via_app=True.

    IMPORTANT: only guests with community_via_app=None are updated.
    Guests with community_via_app=False were already subscribed BEFORE the app
    (detected by sync_vk_status) — they are skipped to avoid false attribution.

Usage:
    # Dry run — see what would change:
    python manage.py fix_vk_via_app --schema levone --dry-run

    # Apply:
    python manage.py fix_vk_via_app --schema levone

    # All tenants:
    python manage.py fix_vk_via_app
"""
from django.core.management.base import BaseCommand
from django_tenants.utils import schema_context


def fix_schema(schema: str, dry_run: bool, stdout, style):
    from django.utils import timezone
    from apps.tenant.branch.models import ClientBranch, ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry

    with schema_context(schema):
        # Find all ClientBranch IDs that won a game super prize
        game_prize_cb_ids = set(
            SuperPrizeEntry.objects
            .filter(acquired_from='game')
            .values_list('client_branch_id', flat=True)
            .distinct()
        )

        stdout.write(f'  [{schema}] {len(game_prize_cb_ids)} guests have game super prizes')

        if not game_prize_cb_ids:
            stdout.write(f'  [{schema}] Nothing to fix.')
            return

        updated = skipped_preexisting = already_ok = 0
        now = timezone.now()

        # Карта: client_branch_id → datetime первого суперприза
        from django.db.models import Min
        first_prize_dates = dict(
            SuperPrizeEntry.objects
            .filter(acquired_from='game', client_branch_id__in=game_prize_cb_ids)
            .values('client_branch_id')
            .annotate(first_at=Min('created_at'))
            .values_list('client_branch_id', 'first_at')
        )

        for cb_id in game_prize_cb_ids:
            prize_date = first_prize_dates.get(cb_id, now)

            try:
                vk_status = ClientVKStatus.objects.get(client_id=cb_id)
            except ClientVKStatus.DoesNotExist:
                # No VKStatus at all — guest subscribed via app to play
                if not dry_run:
                    cb = ClientBranch.objects.get(id=cb_id)
                    ClientVKStatus.objects.create(
                        client=cb,
                        is_community_member=True,
                        community_joined_at=prize_date,
                        community_via_app=True,
                        is_newsletter_subscriber=True,
                        newsletter_joined_at=prize_date,
                        newsletter_via_app=True,
                        checked_at=now,
                    )
                updated += 1
                continue

            needs_update = False
            update_fields = []

            # community — только если via_app=None (не был подписан до приложения)
            # via_app=False означает подписался до приложения → пропускаем
            if vk_status.community_via_app is None:
                needs_update = True
                if not dry_run:
                    vk_status.community_via_app = True
                    vk_status.is_community_member = True
                    vk_status.community_joined_at = prize_date
                    update_fields += ['community_via_app', 'is_community_member', 'community_joined_at']
            elif vk_status.community_via_app is False:
                skipped_preexisting += 1  # уже был в группе до приложения

            # newsletter — только если via_app=None
            if vk_status.newsletter_via_app is None:
                needs_update = True
                if not dry_run:
                    vk_status.newsletter_via_app = True
                    vk_status.is_newsletter_subscriber = True
                    vk_status.newsletter_joined_at = prize_date
                    update_fields += ['newsletter_via_app', 'is_newsletter_subscriber', 'newsletter_joined_at']
            elif vk_status.newsletter_via_app is False:
                skipped_preexisting += 1  # уже был в рассылке до приложения

            if needs_update:
                if not dry_run and update_fields:
                    vk_status.save(update_fields=update_fields)
                updated += 1
            else:
                already_ok += 1

        suffix = ' [DRY RUN]' if dry_run else ''
        stdout.write(style.SUCCESS(
            f'  [{schema}] {updated} updated (via_app=True), '
            f'{skipped_preexisting} skipped (pre-existing subscribers), '
            f'{already_ok} already correct{suffix}'
        ))


class Command(BaseCommand):
    help = (
        'Set community_via_app=True / newsletter_via_app=True '
        'for guests who won a game super prize (proof they subscribed via mini-app)'
    )

    def add_arguments(self, parser):
        parser.add_argument('--schema',  type=str, help='Tenant schema (default: all)')
        parser.add_argument('--dry-run', action='store_true',
                            help='Show changes without writing to DB')

    def handle(self, *args, **options):
        from apps.shared.clients.models import Company

        schema_filter = options.get('schema')
        dry_run       = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN — no DB changes.\n'))

        tenants = Company.objects.exclude(schema_name='public')
        if schema_filter:
            tenants = tenants.filter(schema_name=schema_filter)

        if not tenants.exists():
            self.stdout.write(self.style.ERROR('No matching tenants found.'))
            return

        for company in tenants:
            self.stdout.write(f'\nTenant: {company.name} ({company.schema_name})')
            fix_schema(company.schema_name, dry_run, self.stdout, self.style)

        self.stdout.write(self.style.SUCCESS('\nDone.'))
