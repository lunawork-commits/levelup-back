"""
Восстанавливает community_via_app / newsletter_via_app используя данные из v3-бэкапа.

Логика:
  Из backup_v3.sql берём поля isJoinedCommunity и isAllowedMessageFromCommunity
  для каждого ClientBranch (по id).

  Затем для гостей с SuperPrizeEntry(game):
    - isJoinedCommunity=False в v3 → подписался ЧЕРЕЗ наше приложение → community_via_app=True
    - isJoinedCommunity=True  в v3 → был подписан ДО приложения     → community_via_app=False
    - Аналогично для newsletter

  Для гостей БЕЗ суперприза (не играли):
    - isJoinedCommunity=True  → community_via_app=False (подписан, но не через игру)
    - isJoinedCommunity=False → оставляем None

  community_joined_at для via_app=True устанавливается = дата первого суперприза.

Запуск:
    sudo docker compose exec web python scripts/fix_vk_from_v3_backup.py --dry-run
    sudo docker compose exec web python scripts/fix_vk_from_v3_backup.py --schema levone
"""
import django
import os
import sys
import re

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from django_tenants.utils import get_tenant_model, schema_context
from django.db.models import Min

BACKUP_FILE = '/app/backup_v3.sql'


def parse_v3_clientbranch(schema: str) -> dict:
    """
    Парсит backup_v3.sql и возвращает:
    {cb_id: {'joined_community': bool, 'allowed_newsletter': bool}}
    """
    result = {}
    in_table = False
    # Колонки: id, birth_date, phone, access_token,
    #   isStoryUploaded(4), isJoinedCommunity(5), isSuperPrizeWinned(6),
    #   isReffered(7), created_on(8), updated_at(9), branch_id(10),
    #   client_id(11), invitedBy_id(12), isAllowedMessageFromCommunity(13)
    IDX_ID          = 0
    IDX_JOINED      = 5
    IDX_NEWSLETTER  = 13

    copy_pattern = re.compile(
        rf'^COPY {re.escape(schema)}\.branch_clientbranch\s*\('
    )

    with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')

            if copy_pattern.match(line):
                in_table = True
                continue

            if in_table:
                if line == '\\.':
                    break
                parts = line.split('\t')
                if len(parts) < 14:
                    continue
                try:
                    cb_id      = int(parts[IDX_ID])
                    joined     = parts[IDX_JOINED] == 't'
                    newsletter = parts[IDX_NEWSLETTER] == 't'
                    result[cb_id] = {
                        'joined_community': joined,
                        'allowed_newsletter': newsletter,
                    }
                except (ValueError, IndexError):
                    continue

    return result


def fix_schema(schema: str, dry_run: bool):
    from apps.tenant.branch.models import ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry

    print(f'  [{schema}] Парсим v3-бэкап...')
    v3_data = parse_v3_clientbranch(schema)
    print(f'  [{schema}] {len(v3_data)} записей из v3')

    if not v3_data:
        print(f'  [{schema}] Данные не найдены в бэкапе — пропускаем.')
        return

    with schema_context(schema):
        # Карта: client_branch_id → дата первого суперприза
        first_prize_dates = dict(
            SuperPrizeEntry.objects
            .filter(acquired_from='game')
            .values('client_branch_id')
            .annotate(first_at=Min('created_at'))
            .values_list('client_branch_id', 'first_at')
        )
        game_cb_ids = set(first_prize_dates.keys())

        community_via_app_true  = 0
        community_via_app_false = 0
        newsletter_via_app_true  = 0
        newsletter_via_app_false = 0
        skipped = 0

        for cb_id, v3 in v3_data.items():
            was_in_community   = v3['joined_community']
            was_in_newsletter  = v3['allowed_newsletter']
            has_game_prize     = cb_id in game_cb_ids
            prize_date         = first_prize_dates.get(cb_id)

            try:
                vk_status = ClientVKStatus.objects.get(client_id=cb_id)
            except ClientVKStatus.DoesNotExist:
                skipped += 1
                continue

            update_fields = []

            # ── community ────────────────────────────────────────────────────
            if has_game_prize and not was_in_community:
                # Не был в группе в v3, но сыграл → подписался через приложение
                new_community_via_app   = True
                new_community_joined_at = prize_date
                community_via_app_true += 1
            elif was_in_community:
                # Был в группе в v3 → pre-existing, дата неизвестна
                new_community_via_app   = False
                new_community_joined_at = None
                community_via_app_false += 1
            else:
                # Не был и не играл → не подписан
                new_community_via_app   = None
                new_community_joined_at = None

            if (vk_status.community_via_app != new_community_via_app
                    or vk_status.community_joined_at != new_community_joined_at):
                if not dry_run:
                    vk_status.community_via_app   = new_community_via_app
                    vk_status.community_joined_at = new_community_joined_at
                    update_fields += ['community_via_app', 'community_joined_at']

            # ── newsletter ───────────────────────────────────────────────────
            if has_game_prize and not was_in_newsletter:
                new_newsletter_via_app   = True
                new_newsletter_joined_at = prize_date
                newsletter_via_app_true += 1
            elif was_in_newsletter:
                new_newsletter_via_app   = False
                new_newsletter_joined_at = None
                newsletter_via_app_false += 1
            else:
                new_newsletter_via_app   = None
                new_newsletter_joined_at = None

            if (vk_status.newsletter_via_app != new_newsletter_via_app
                    or vk_status.newsletter_joined_at != new_newsletter_joined_at):
                if not dry_run:
                    vk_status.newsletter_via_app   = new_newsletter_via_app
                    vk_status.newsletter_joined_at = new_newsletter_joined_at
                    update_fields += ['newsletter_via_app', 'newsletter_joined_at']

            if update_fields and not dry_run:
                vk_status.save(update_fields=update_fields)

        suffix = ' [DRY RUN]' if dry_run else ''
        print(f'  [{schema}] community:   {community_via_app_true} → via_app=True, {community_via_app_false} → False{suffix}')
        print(f'  [{schema}] newsletter:  {newsletter_via_app_true} → via_app=True, {newsletter_via_app_false} → False{suffix}')
        if skipped:
            print(f'  [{schema}] {skipped} ClientVKStatus не найдено (новые гости появившиеся после v3)')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema', type=str, default='levone')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN — ничего не записывается\n')

    Company = get_tenant_model()
    tenants = Company.objects.filter(schema_name=args.schema)

    if not tenants.exists():
        print(f'Tenant {args.schema!r} не найден.')
        return

    for company in tenants:
        print(f'\nTenant: {company.name} ({company.schema_name})')
        fix_schema(company.schema_name, args.dry_run)

    print('\nGotovo.')


if __name__ == '__main__':
    main()
