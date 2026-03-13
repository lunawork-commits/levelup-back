"""
Исправляет community_joined_at / newsletter_joined_at которые скрипты
fix_vk_via_app и sync_vk_status выставили как today() для записей с NULL-датой.

Логика:
  community_via_app=True  + joined_at=сегодня → ставим дату первого SuperPrizeEntry(game)
                                                  если нет — NULL (дата неизвестна)
  community_via_app=False + joined_at=сегодня → NULL (подписался до приложения)
  То же самое для newsletter_via_app / newsletter_joined_at

Запуск:
    # Dry run — показывает изменения без записи:
    sudo docker compose exec web python scripts/fix_vk_joined_at.py --dry-run

    # Применить для конкретного tenant:
    sudo docker compose exec web python scripts/fix_vk_joined_at.py --schema levone

    # Применить для всех:
    sudo docker compose exec web python scripts/fix_vk_joined_at.py
"""
import django
import os
import sys

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from datetime import date
from django_tenants.utils import get_tenant_model, schema_context
from django.db.models import Min


def fix_schema(schema: str, dry_run: bool):
    from apps.tenant.branch.models import ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry

    today = date.today()

    with schema_context(schema):
        # Карта: client_branch_id → дата первого суперприза из игры
        first_prize_dates = dict(
            SuperPrizeEntry.objects
            .filter(acquired_from='game')
            .values('client_branch_id')
            .annotate(first_at=Min('created_at'))
            .values_list('client_branch_id', 'first_at')
        )

        qs = ClientVKStatus.objects.filter(
            community_joined_at__date=today
        ) | ClientVKStatus.objects.filter(
            newsletter_joined_at__date=today
        )
        qs = qs.distinct()

        community_fixed = community_nulled = 0
        newsletter_fixed = newsletter_nulled = 0

        for vk_status in qs:
            update_fields = []

            # ── community_joined_at ──────────────────────────────────────────
            if vk_status.community_joined_at and vk_status.community_joined_at.date() == today:
                if vk_status.community_via_app is True:
                    real_date = first_prize_dates.get(vk_status.client_id)
                    new_val = real_date if real_date else None
                    if not dry_run:
                        vk_status.community_joined_at = new_val
                        update_fields.append('community_joined_at')
                    if real_date:
                        community_fixed += 1
                    else:
                        community_nulled += 1
                else:
                    # via_app=False или None — дата неизвестна
                    if not dry_run:
                        vk_status.community_joined_at = None
                        update_fields.append('community_joined_at')
                    community_nulled += 1

            # ── newsletter_joined_at ─────────────────────────────────────────
            if vk_status.newsletter_joined_at and vk_status.newsletter_joined_at.date() == today:
                if vk_status.newsletter_via_app is True:
                    real_date = first_prize_dates.get(vk_status.client_id)
                    new_val = real_date if real_date else None
                    if not dry_run:
                        vk_status.newsletter_joined_at = new_val
                        update_fields.append('newsletter_joined_at')
                    if real_date:
                        newsletter_fixed += 1
                    else:
                        newsletter_nulled += 1
                else:
                    if not dry_run:
                        vk_status.newsletter_joined_at = None
                        update_fields.append('newsletter_joined_at')
                    newsletter_nulled += 1

            if update_fields and not dry_run:
                vk_status.save(update_fields=update_fields)

        suffix = ' [DRY RUN]' if dry_run else ''
        print(f'[{schema}] community_joined_at: {community_fixed} → реальная дата, '
              f'{community_nulled} → NULL{suffix}')
        print(f'[{schema}] newsletter_joined_at: {newsletter_fixed} → реальная дата, '
              f'{newsletter_nulled} → NULL{suffix}')


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
        fix_schema(company.schema_name, args.dry_run)

    print('\nGotovo.')


if __name__ == '__main__':
    main()
