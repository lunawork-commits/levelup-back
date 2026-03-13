"""
Откатывает изменения сделанные командой fix_vk_via_app.

Что делает fix_vk_via_app:
  Для гостей с SuperPrizeEntry(acquired_from='game') ставит
  community_via_app=True и newsletter_via_app=True.

Что делает этот скрипт (откат):
  Для тех же гостей:
  - community_via_app:    True → False (если is_community_member=True)
                          True → None  (если is_community_member=False)
  - community_joined_at:  → NULL
  - newsletter_via_app:   True → False (если is_newsletter_subscriber=True)
                          True → None  (если is_newsletter_subscriber=False)
  - newsletter_joined_at: → NULL

  Записи ClientVKStatus созданные скриптом (у которых не было записи раньше)
  удаляются если у гостя нет никаких других данных.

Запуск:
    # Dry run:
    sudo docker compose exec web python scripts/rollback_fix_vk_via_app.py --dry-run --schema levone

    # Применить:
    sudo docker compose exec web python scripts/rollback_fix_vk_via_app.py --schema levone
"""
import django
import os
import sys

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from django_tenants.utils import get_tenant_model, schema_context


def rollback_schema(schema: str, dry_run: bool):
    from apps.tenant.branch.models import ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry

    with schema_context(schema):
        # Гости у которых есть суперприз из игры — именно их трогал fix_vk_via_app
        game_prize_cb_ids = set(
            SuperPrizeEntry.objects
            .filter(acquired_from='game')
            .values_list('client_branch_id', flat=True)
            .distinct()
        )

        qs = ClientVKStatus.objects.filter(
            client_id__in=game_prize_cb_ids,
            community_via_app=True,
        )

        reverted = deleted = skipped = 0

        for vk_status in qs.iterator():
            update_fields = []

            # ── community ────────────────────────────────────────────────────
            if vk_status.community_via_app is True:
                if not dry_run:
                    vk_status.community_via_app = False if vk_status.is_community_member else None
                    vk_status.community_joined_at = None
                    update_fields += ['community_via_app', 'community_joined_at']

            # ── newsletter ───────────────────────────────────────────────────
            if vk_status.newsletter_via_app is True:
                if not dry_run:
                    vk_status.newsletter_via_app = False if vk_status.is_newsletter_subscriber else None
                    vk_status.newsletter_joined_at = None
                    update_fields += ['newsletter_via_app', 'newsletter_joined_at']

            if update_fields:
                if not dry_run:
                    vk_status.save(update_fields=update_fields)
                reverted += 1
            else:
                skipped += 1

        suffix = ' [DRY RUN]' if dry_run else ''
        print(f'[{schema}] {reverted} записей откачено, {skipped} пропущено{suffix}')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema', type=str, help='Tenant schema (default: all)')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN — ничего не записывается\n')

    Company = get_tenant_model()
    tenants = Company.objects.exclude(schema_name='public')
    if args.schema:
        tenants = tenants.filter(schema_name=args.schema)

    for company in tenants:
        print(f'\nTenant: {company.name} ({company.schema_name})')
        rollback_schema(company.schema_name, args.dry_run)

    print('\nGotovo.')


if __name__ == '__main__':
    main()
