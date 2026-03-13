"""
Восстанавливает ClientVKStatus из migrate_v5.sql.

Для каждой записи найденной в SQL-файле — перезаписывает текущие значения
в БД оригинальными. Записи которых нет в файле — не трогает.

Запуск:
    sudo docker compose exec web python scripts/restore_vk_status_from_migration.py --dry-run
    sudo docker compose exec web python scripts/restore_vk_status_from_migration.py
"""
import django
import os
import sys
import re

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from django_tenants.utils import schema_context
from django.utils.dateparse import parse_datetime

SQL_FILE = '/app/migrate_v5.sql'
SCHEMA   = 'levone'

# INSERT INTO levone.branch_clientvkstatus (...) VALUES (...);
INSERT_RE = re.compile(
    r"INSERT INTO levone\.branch_clientvkstatus\s*\([^)]+\)\s*VALUES\s*\((.+?)\);",
    re.DOTALL,
)

# Порядок колонок в INSERT
COLUMNS = [
    'client_id', 'is_community_member', 'community_joined_at', 'community_via_app',
    'is_newsletter_subscriber', 'newsletter_joined_at', 'newsletter_via_app',
    'is_story_uploaded', 'story_uploaded_at', 'checked_at',
]


def parse_value(raw: str):
    raw = raw.strip()
    if raw.upper() == 'NULL':
        return None
    if raw.upper() == 'TRUE':
        return True
    if raw.upper() == 'FALSE':
        return False
    # Datetime: E'2026-03-06 18:59:44...' or '2026-...'
    if raw.startswith("E'") or raw.startswith("'"):
        dt_str = raw.strip("E'").rstrip("'")
        return parse_datetime(dt_str)
    # Integer
    try:
        return int(raw)
    except ValueError:
        return raw


def split_values(values_str: str) -> list:
    """Split comma-separated SQL values respecting quoted strings."""
    parts = []
    current = ''
    in_quote = False
    i = 0
    while i < len(values_str):
        c = values_str[i]
        if c == "'" and (i == 0 or values_str[i-1] != '\\'):
            in_quote = not in_quote
            current += c
        elif c == ',' and not in_quote:
            parts.append(current.strip())
            current = ''
        else:
            current += c
        i += 1
    if current.strip():
        parts.append(current.strip())
    return parts


def parse_sql(schema: str) -> dict:
    """Returns {client_id: {field: value, ...}}"""
    records = {}
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        content = f.read()

    pattern = re.compile(
        rf"INSERT INTO {re.escape(schema)}\.branch_clientvkstatus\s*\([^)]+\)\s*VALUES\s*\((.+?)\);",
        re.DOTALL,
    )

    for match in pattern.finditer(content):
        values_str = match.group(1)
        parts = split_values(values_str)
        if len(parts) != len(COLUMNS):
            continue
        row = {col: parse_value(val) for col, val in zip(COLUMNS, parts)}
        records[row['client_id']] = row

    return records


def restore(dry_run: bool):
    from apps.tenant.branch.models import ClientVKStatus

    print(f'Парсим {SQL_FILE}...')
    records = parse_sql(SCHEMA)
    print(f'Найдено {len(records)} записей для {SCHEMA}')

    restored = skipped = not_found = 0

    with schema_context(SCHEMA):
        for client_id, row in records.items():
            try:
                vk = ClientVKStatus.objects.get(client_id=client_id)
            except ClientVKStatus.DoesNotExist:
                not_found += 1
                continue

            # Проверяем что хоть что-то изменилось
            changed = (
                vk.is_community_member      != row['is_community_member']
                or vk.community_joined_at   != row['community_joined_at']
                or vk.community_via_app     != row['community_via_app']
                or vk.is_newsletter_subscriber != row['is_newsletter_subscriber']
                or vk.newsletter_joined_at  != row['newsletter_joined_at']
                or vk.newsletter_via_app    != row['newsletter_via_app']
            )

            if not changed:
                skipped += 1
                continue

            if not dry_run:
                vk.is_community_member      = row['is_community_member']
                vk.community_joined_at      = row['community_joined_at']
                vk.community_via_app        = row['community_via_app']
                vk.is_newsletter_subscriber = row['is_newsletter_subscriber']
                vk.newsletter_joined_at     = row['newsletter_joined_at']
                vk.newsletter_via_app       = row['newsletter_via_app']
                vk.save(update_fields=[
                    'is_community_member', 'community_joined_at', 'community_via_app',
                    'is_newsletter_subscriber', 'newsletter_joined_at', 'newsletter_via_app',
                ])

            restored += 1

    suffix = ' [DRY RUN]' if dry_run else ''
    print(f'Восстановлено: {restored}, без изменений: {skipped}, не найдено в БД: {not_found}{suffix}')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN — ничего не записывается\n')

    restore(args.dry_run)
    print('\nGotovo.')


if __name__ == '__main__':
    main()
