#!/usr/bin/env python3
"""
LevOne v4 → v5 Data Migration Script.

Parses a pg_dump --data-only --inserts file from v4 and outputs
v5-compatible SQL that can be applied to a fresh v5 database.

Usage:
    python scripts/migrate_v4.py backup_v4_data.sql > migrate_v5.sql

Then on the server (after running `python manage.py migrate_schemas`):
    psql -U <user> -d <dbname> -f migrate_v5.sql

NOTES:
  - senler_vkconnection tokens are ENCRYPTED in v4 and cannot be automatically
    migrated. Re-enter VK tokens manually in senler_senlerconfig after migration.
  - clients_knowledgebase (v4 public) is not migrated (moved to per-tenant analytics
    KnowledgeBaseDocument in v5). Re-upload manually.
  - senler_mailingcampaign / messagelog → senler_broadcast + broadcastsend + broadcastrecipient.
  - staff_employeeprofile not migrated (v5 uses ClientBranch.is_employee instead).
"""

import re
import sys
from datetime import datetime, timedelta
from typing import Generator


# ── SQL Value Parser ─────────────────────────────────────────────────────────

def iter_values(s: str) -> Generator:
    """
    Yield Python values from a PostgreSQL VALUES tuple string.
    Handles: NULL, true, false, integers, floats, single-quoted strings
    (including multi-line strings and '' escape sequences).
    """
    i = 0
    n = len(s)
    while i < n:
        while i < n and s[i] in ' \t\n\r':
            i += 1
        if i >= n:
            break

        if s[i] == "'":
            i += 1
            buf = []
            while i < n:
                c = s[i]
                if c == "'" and i + 1 < n and s[i + 1] == "'":
                    buf.append("'")
                    i += 2
                elif c == "'":
                    i += 1
                    break
                else:
                    buf.append(c)
                    i += 1
            yield ''.join(buf)
        elif s[i:i+4].upper() == 'NULL':
            yield None
            i += 4
        elif s[i:i+4].lower() == 'true':
            yield True
            i += 4
        elif s[i:i+5].lower() == 'false':
            yield False
            i += 5
        else:
            j = i
            while j < n and s[j] not in ',)':
                j += 1
            token = s[i:j].strip()
            try:
                yield int(token)
            except ValueError:
                try:
                    yield float(token)
                except ValueError:
                    yield token
            i = j

        while i < n and s[i] in ' \t\n\r':
            i += 1
        if i < n and s[i] == ',':
            i += 1


def extract_inserts(content: str, schema: str, table: str) -> list[list]:
    """Return list of row value-lists for every INSERT into schema.table."""
    qualified = re.escape(f'{schema}.{table}')
    pattern = re.compile(
        rf'INSERT INTO {qualified} VALUES \(',
        re.IGNORECASE,
    )
    rows = []
    for m in pattern.finditer(content):
        start = m.end()
        depth = 1
        in_str = False
        i = start
        while i < len(content) and depth > 0:
            c = content[i]
            if in_str:
                if c == "'" and i + 1 < len(content) and content[i + 1] == "'":
                    i += 2
                    continue
                if c == "'":
                    in_str = False
            else:
                if c == "'":
                    in_str = True
                elif c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                    if depth == 0:
                        values_str = content[start:i]
                        rows.append(list(iter_values(values_str)))
                        break
            i += 1
    return rows


# ── SQL Formatting Helpers ────────────────────────────────────────────────────

def sql_val(v) -> str:
    """Format a Python value as a SQL literal."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, (int, float)):
        return str(v)
    # String - use E'...' with proper escaping
    s = str(v)
    s = s.replace('\\', '\\\\')
    s = s.replace("'", "''")
    s = s.replace('\n', '\\n')
    s = s.replace('\r', '\\r')
    s = s.replace('\t', '\\t')
    return f"E'{s}'"


def b(v) -> bool | None:
    """Coerce to bool."""
    if v is None:
        return None
    return bool(v)


def insert_row(schema: str, table: str, columns: list[str], values: list) -> str:
    cols = ', '.join(columns)
    vals = ', '.join(sql_val(v) for v in values)
    return f'INSERT INTO {schema}.{table} ({cols}) VALUES ({vals});'


def comment(text: str) -> str:
    return f'\n-- {text}'


def parse_interval_minutes(dur_str: str, default: int = 40) -> int:
    """Parse 'HH:MM:SS' interval to minutes."""
    try:
        parts = str(dur_str).split(':')
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return default


def parse_interval_hours(dur_str: str, default: int = 5) -> int:
    """Parse 'HH:MM:SS' interval to hours."""
    try:
        return int(str(dur_str).split(':')[0])
    except Exception:
        return default


def ts_add(ts_str: str, **kwargs) -> str | None:
    """Add timedelta to a timestamp string, return ISO string or None."""
    try:
        fixed = re.sub(r'([+-]\d{2})$', r'\g<1>:00', str(ts_str))
        dt = datetime.fromisoformat(fixed)
        return (dt + timedelta(**kwargs)).isoformat()
    except Exception:
        return None


# ── Public Schema Migrations ──────────────────────────────────────────────────

def migrate_guest_client(content: str, out: list):
    """
    public.guest_client
    v4: id, created_at, updated_at, vk_id, first_name, last_name, is_active(0/1)
    v5: adds photo_url='', gender=NULL, is_active as boolean
    """
    out.append(comment('public.guest_client'))
    rows = extract_inserts(content, 'public', 'guest_client')
    for r in rows:
        out.append(insert_row('public', 'guest_client',
            ['id', 'created_at', 'updated_at', 'vk_id', 'first_name', 'last_name',
             'photo_url', 'gender', 'is_active'],
            [r[0], r[1], r[2], r[3], r[4], r[5], '', None, b(r[6])]
        ))


def migrate_clients_company(content: str, out: list):
    """
    public.clients_company
    v4: id, schema_name, created_at, updated_at, name, description, is_active, paid_until, client_id
    v5: no created_at/updated_at (TenantMixin). Skip public root tenant.
    """
    out.append(comment('public.clients_company'))
    rows = extract_inserts(content, 'public', 'clients_company')
    for r in rows:
        out.append(insert_row('public', 'clients_company',
            ['id', 'schema_name', 'name', 'client_id', 'description', 'is_active', 'paid_until'],
            [r[0], r[1], r[4], r[8], r[5] or '', r[6], r[7]]
        ))


def migrate_clients_domain(content: str, out: list):
    """public.clients_domain — same structure."""
    out.append(comment('public.clients_domain'))
    rows = extract_inserts(content, 'public', 'clients_domain')
    for r in rows:
        out.append(insert_row('public', 'clients_domain',
            ['id', 'domain', 'is_primary', 'tenant_id'],
            r[:4]
        ))


def migrate_users_user(content: str, out: list):
    """
    public.users_user
    v4: id, password, last_login, is_superuser, username, first_name,
        last_name, email, is_staff, is_active, date_joined, company_id
    v5: adds role field. is_superuser → superadmin, else → network_admin.
    """
    out.append(comment('public.users_user'))
    rows = extract_inserts(content, 'public', 'users_user')
    for r in rows:
        role = 'superadmin' if r[3] else 'network_admin'
        out.append(insert_row('public', 'users_user',
            ['id', 'password', 'last_login', 'is_superuser', 'username',
             'first_name', 'last_name', 'email', 'is_staff', 'is_active',
             'date_joined', 'role', 'company_id'],
            [r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9],
             r[10], role, r[11]]
        ))


def migrate_config_clientconfig(content: str, out: list):
    """
    public.clients_companyconfig → public.config_clientconfig
    v4: id, created_at, updated_at, logotype_image, coin_image,
        vk_group_name, vk_group_id, iiko_api_url, iiko_login, iiko_password,
        company_id, dooglys_api_token, dooglys_api_url, vk_mini_app_id(skip)
    v5: adds pos_type (inferred from data), drops timestamps & vk_mini_app_id
    """
    out.append(comment('public.config_clientconfig (from clients_companyconfig)'))
    rows = extract_inserts(content, 'public', 'clients_companyconfig')
    for r in rows:
        iiko_url    = r[7]
        dooglys_url = r[12]
        if iiko_url:
            pos_type = 'iiko'
        elif dooglys_url:
            pos_type = 'dooglys'
        else:
            pos_type = 'none'
        out.append(insert_row('public', 'config_clientconfig',
            ['id', 'company_id', 'logotype_image', 'coin_image',
             'vk_group_id', 'vk_group_name', 'pos_type',
             'iiko_api_url', 'iiko_login', 'iiko_password',
             'dooglys_api_url', 'dooglys_api_token'],
            [r[0], r[10], r[3] or '', r[4] or '',
             r[6] or 0, r[5] or '', pos_type,
             iiko_url or '', r[8] or '', r[9] or '',
             dooglys_url or '', r[11] or '']
        ))


# ── Tenant Schema Migrations ──────────────────────────────────────────────────

def migrate_branch_branch(content: str, schema: str, out: list):
    """
    branch_branch
    v4 (all schemas): id, ct, ut, name, desc, iiko_organization_id, dooglys_branch_id, dooglys_sale_point_id
    v5: adds branch_id (= id), is_active=true, story_image=''
    """
    out.append(comment(f'{schema}.branch_branch'))
    rows = extract_inserts(content, schema, 'branch_branch')
    # Also read story images to set on branches
    story_rows = extract_inserts(content, schema, 'branch_storyimage')
    story_map = {}  # branch_id → image path
    for sr in story_rows:
        # v4 storyimage: id, ct, ut, image_path, branch_id
        story_map[sr[4]] = sr[3]

    for r in rows:
        story_img = story_map.get(r[0], '')
        out.append(insert_row(schema, 'branch_branch',
            ['id', 'branch_id', 'name', 'description', 'is_active',
             'iiko_organization_id', 'dooglys_branch_id', 'dooglys_sale_point_id',
             'story_image', 'created_at', 'updated_at'],
            [r[0], r[0], r[3], r[4] or '', True,
             r[5] or '', r[6], r[7],
             story_img or '', r[1], r[2]]
        ))


def migrate_branch_branchconfig(content: str, schema: str, out: list):
    """
    branch_branchconfig
    v4: id, ct, ut, yandex_map, gis_map, branch_id
    v5: adds address='', phone=''
    """
    out.append(comment(f'{schema}.branch_branchconfig'))
    rows = extract_inserts(content, schema, 'branch_branchconfig')
    for r in rows:
        out.append(insert_row(schema, 'branch_branchconfig',
            ['id', 'branch_id', 'address', 'phone',
             'yandex_map', 'gis_map', 'created_at', 'updated_at'],
            [r[0], r[5], '', '', r[3] or '', r[4] or '', r[1], r[2]]
        ))


def migrate_branch_clientbranch(content: str, schema: str, out: list):
    """
    v4 branch_clientbranch (18 cols) → v5 branch_clientbranch + branch_clientvkstatus

    v4 column positions (0-indexed):
      0: id
      1: created_at
      2: updated_at
      3: birth_date
      4: is_community_member
      5: is_newsletter_subscriber
      6: is_story_uploaded
      7: community_via_app (bool)
      8: newsletter_via_app (bool)
      9: branch_id (FK)
      10: client_id (FK)
      11: notes (text, nullable)
      12: ?? (bool) — extra VK tracking field, not in v5
      13: ?? (bool) — extra VK tracking field, not in v5
      14: birth_date_set_at (datetime, nullable)
      15: is_employee (bool)
      16: newsletter_joined_at or community_joined_at (datetime)
      17: story_uploaded_at (datetime)
    """
    out.append(comment(f'{schema}.branch_clientbranch'))
    rows = extract_inserts(content, schema, 'branch_clientbranch')
    for r in rows:
        # ClientBranch
        out.append(insert_row(schema, 'branch_clientbranch',
            ['id', 'client_id', 'branch_id', 'birth_date',
             'birth_date_set_at', 'is_employee', 'notes',
             'created_at', 'updated_at'],
            [r[0], r[10], r[9], r[3],
             r[14] if len(r) > 14 else None,
             b(r[15]) if len(r) > 15 else False,
             r[11] or '',
             r[1], r[2]]
        ))
        # ClientVKStatus
        # Backfill NULL timestamps with updated_at when the boolean flag is True
        _is_comm = b(r[4])
        _is_news = b(r[5])
        _is_story = b(r[6])
        _ts16 = r[16] if len(r) > 16 and r[16] not in (None, 'NULL') else None
        _ts17 = r[17] if len(r) > 17 and r[17] not in (None, 'NULL') else None
        _fallback = r[2]  # updated_at as fallback timestamp
        out.append(insert_row(schema, 'branch_clientvkstatus',
            ['client_id',
             'is_community_member', 'community_joined_at', 'community_via_app',
             'is_newsletter_subscriber', 'newsletter_joined_at', 'newsletter_via_app',
             'is_story_uploaded', 'story_uploaded_at',
             'checked_at'],
            [r[0],
             _is_comm, _ts16 or (_fallback if _is_comm else None), b(r[7]) if _is_comm else None,
             _is_news, _ts16 or (_fallback if _is_news else None), b(r[8]) if _is_news else None,
             _is_story, _ts17 or (_fallback if _is_story else None),
             _fallback]
        ))


def migrate_branch_clientbranchvisit(content: str, schema: str, out: list):
    """
    v4: id, created_at, updated_at, visited_at, client_id
    v5: id, client_id, visited_at
    """
    out.append(comment(f'{schema}.branch_clientbranchvisit'))
    rows = extract_inserts(content, schema, 'branch_clientbranchvisit')
    for r in rows:
        out.append(insert_row(schema, 'branch_clientbranchvisit',
            ['id', 'client_id', 'visited_at'],
            [r[0], r[4], r[3]]
        ))


def migrate_branch_cointransaction(content: str, schema: str, out: list):
    """
    v4: id, type(UPPER), source(UPPER), amount, description, created_at, client_id
    v5: lowercase type/source
    """
    out.append(comment(f'{schema}.branch_cointransaction'))
    rows = extract_inserts(content, schema, 'branch_cointransaction')
    for r in rows:
        out.append(insert_row(schema, 'branch_cointransaction',
            ['id', 'client_id', 'type', 'source', 'amount', 'description', 'created_at'],
            [r[0], r[6], str(r[1]).lower(), str(r[2]).lower(), r[3], r[4] or '', r[5]]
        ))


def migrate_all_dailycodes(content: str, schema: str, out: list):
    """
    Merge 3 old dailycode tables into one branch_dailycode with purpose field.

    Old tables (all same format: id, ct, ut, valid_date, code, branch_id):
      - branch_dailycode  → purpose='birthday'
      - game_dailycode    → purpose='game'
      - quest_dailycode   → purpose='quest'

    v5 branch_dailycode: id, ct, ut, branch_id, purpose, code, valid_date
    IDs are regenerated to avoid conflicts between the 3 source tables.
    """
    out.append(comment(f'{schema}.branch_dailycode (merged from branch/game/quest dailycodes)'))

    sources = [
        ('branch_dailycode', 'birthday'),
        ('game_dailycode',   'game'),
        ('quest_dailycode',  'quest'),
    ]
    new_id = 1
    for old_table, purpose in sources:
        rows = extract_inserts(content, schema, old_table)
        for r in rows:
            code = str(r[4])[:5] if r[4] else r[4]
            out.append(insert_row(schema, 'branch_dailycode',
                ['id', 'branch_id', 'purpose', 'code', 'valid_date',
                 'created_at', 'updated_at'],
                [new_id, r[5], purpose, code, r[3], r[1], r[2]]
            ))
            new_id += 1


def migrate_all_cooldowns(content: str, schema: str, out: list):
    """
    Merge 4 old cooldown tables into one branch_cooldown with feature field.

    Old tables (all same format: id, last_activated_at, duration('HH:MM:SS'), client_id):
      - game_cooldown      → feature='game'
      - catalog_cooldown    → feature='shop'
      - quest_cooldown      → feature='quest'
      - inventory_cooldown  → feature='inventory'

    v5 branch_cooldown: id, ct, ut, client_id, feature, last_activated_at, duration(int hours), expires_at
    IDs are regenerated to avoid conflicts between the 4 source tables.
    """
    out.append(comment(f'{schema}.branch_cooldown (merged from game/catalog/quest/inventory cooldowns)'))

    sources = [
        ('game_cooldown',      'game'),
        ('catalog_cooldown',   'shop'),
        ('quest_cooldown',     'quest'),
        ('inventory_cooldown', 'inventory'),
    ]
    new_id = 1
    now = datetime.now().isoformat()
    for old_table, feature in sources:
        rows = extract_inserts(content, schema, old_table)
        for r in rows:
            # v4: id(0), last_activated_at(1), duration_interval(2), client_id(3)
            last_act = r[1]
            dur_h = parse_interval_hours(r[2], default=18)
            expires = ts_add(last_act, hours=dur_h) if last_act else None
            out.append(insert_row(schema, 'branch_cooldown',
                ['id', 'client_id', 'feature', 'last_activated_at',
                 'duration', 'expires_at', 'created_at', 'updated_at'],
                [new_id, r[3], feature, last_act,
                 dur_h, expires, now, now]
            ))
            new_id += 1


def migrate_branch_testimonials(content: str, schema: str, out: list):
    """
    v4 branch_branchtestimonials (15 cols) →
    v5 branch_testimonialconversation + branch_testimonialmessage

    v4 positions:
      0: id    1: ct    2: ut    3: vk_sender_id    4: vk_message_id/vk_peer_id
      5: rating (0/NULL for VK)    6: phone    7: branch_id (0 for VK default)
      8: text    9: source ('APP'|'VK_MESSAGE')    10: sentiment
      11: ai_comment    12: is_replied    13: client_branch_id    14: has_unread
    """
    out.append(comment(f'{schema}.branch_testimonialconversation + testimonialmessage'))
    rows = extract_inserts(content, schema, 'branch_branchtestimonials')
    for r in rows:
        branch_id = r[7] if r[7] and r[7] not in (0, '0') else 1
        rating = r[5] if r[5] and r[5] not in (0, '0') else None
        out.append(insert_row(schema, 'branch_testimonialconversation',
            ['id', 'branch_id', 'client_id', 'vk_sender_id', 'sentiment',
             'ai_comment', 'has_unread', 'is_replied', 'last_message_at',
             'created_at', 'updated_at'],
            [r[0], branch_id, r[13], r[3] or '', r[10] or 'WAITING',
             r[11] or '', b(r[14]), b(r[12]), r[1], r[1], r[2]]
        ))
        out.append(insert_row(schema, 'branch_testimonialmessage',
            ['conversation_id', 'source', 'text', 'rating',
             'phone', 'table_number', 'vk_message_id', 'created_at'],
            [r[0], str(r[9]), r[8] or '', rating,
             r[6] or '', None, r[4] or '', r[1]]
        ))


def migrate_testimonial_replies(content: str, schema: str, out: list):
    """
    v4 branch_testimonialreply → additional branch_testimonialmessage rows

    v4 positions:
      0: id    1: ct    2: ut    3: text    4: timestamp
      5: is_read    6: ?    7: responded_by_user_id    8: testimonial_id(conversation)
      9: direction ('incoming'|'outgoing')    10: source_type ('vk_message'|'admin_reply'|'app_review')
      11: vk_message_id

    v5 source mapping:
      incoming + vk_message  → VK_MESSAGE
      incoming + app_review  → APP
      outgoing + admin_reply → ADMIN_REPLY
    """
    out.append(comment(f'{schema}.branch_testimonialmessage (from branch_testimonialreply)'))
    rows = extract_inserts(content, schema, 'branch_testimonialreply')
    for r in rows:
        direction = str(r[9]).lower()
        src_type = str(r[10]).lower()
        if direction == 'outgoing' or src_type == 'admin_reply':
            source = 'ADMIN_REPLY'
        elif src_type == 'app_review':
            source = 'APP'
        else:
            source = 'VK_MESSAGE'
        out.append(insert_row(schema, 'branch_testimonialmessage',
            ['conversation_id', 'source', 'text', 'rating',
             'phone', 'table_number', 'vk_message_id', 'created_at'],
            [r[8], source, r[3] or '', None,
             '', None, r[11] or '', r[1]]
        ))


def migrate_branch_promotions(content: str, schema: str, out: list):
    """
    branch_promotions — same in v4 and v5.
    v4: id, ct, ut, title, discount, dates, images, branch_id
    """
    out.append(comment(f'{schema}.branch_promotions'))
    rows = extract_inserts(content, schema, 'branch_promotions')
    for r in rows:
        if len(r) < 8:
            continue
        out.append(insert_row(schema, 'branch_promotions',
            ['id', 'branch_id', 'title', 'discount', 'dates', 'images',
             'created_at', 'updated_at'],
            [r[0], r[7], r[3], r[4] or '', r[5] or '', r[6] or '', r[1], r[2]]
        ))


def migrate_catalog_product(content: str, schema: str, out: list):
    """
    catalog_product
    v4: id, ct, ut, name, desc, image, price, is_super_prize, is_active, branch_id, is_birthday_prize
    v5: adds category_id=NULL, ordering=0
    """
    out.append(comment(f'{schema}.catalog_product'))
    rows = extract_inserts(content, schema, 'catalog_product')
    for r in rows:
        out.append(insert_row(schema, 'catalog_product',
            ['id', 'branch_id', 'category_id', 'name', 'description',
             'image', 'price', 'is_active', 'is_super_prize',
             'is_birthday_prize', 'ordering', 'created_at', 'updated_at'],
            [r[0], r[9], None, r[3], r[4] or '',
             r[5] or '', r[6], b(r[8]), b(r[7]),
             b(r[10]) if len(r) > 10 else False, 0, r[1], r[2]]
        ))


def migrate_inventory_inventory(content: str, schema: str, out: list):
    """
    v4 inventory_inventory → v5 inventory_inventoryitem
    v4: id, ct, ut, acquired_from(UPPER), duration('HH:MM:SS'), activated_at, used_at, client_branch_id, product_id
    """
    ACQMAP = {
        'SUPERPRIZE': 'super_prize',
        'BIRTHDAY_PRIZE': 'birthday',
        'BUY': 'purchase',
    }
    out.append(comment(f'{schema}.inventory_inventoryitem (from inventory_inventory)'))
    rows = extract_inserts(content, schema, 'inventory_inventory')
    for r in rows:
        acq = ACQMAP.get(str(r[3]).upper(), str(r[3]).lower())
        dur_min = parse_interval_minutes(r[4], default=40)
        # Calculate expires_at from activated_at + duration
        expires = ts_add(r[5], minutes=dur_min) if r[5] else None
        out.append(insert_row(schema, 'inventory_inventoryitem',
            ['id', 'client_branch_id', 'product_id', 'acquired_from',
             'description', 'duration', 'activated_at', 'expires_at',
             'used_at', 'created_at', 'updated_at'],
            [r[0], r[7], r[8], acq, '', dur_min,
             r[5], expires, r[6], r[1], r[2]]
        ))


def migrate_inventory_superprize(content: str, schema: str, out: list):
    """
    v4 inventory_superprize → v5 inventory_superprizeentry
    v4: id, ct, ut, acquired_from(UPPER), claimed_at, client_branch_id, product_id
    """
    out.append(comment(f'{schema}.inventory_superprizeentry (from inventory_superprize)'))
    rows = extract_inserts(content, schema, 'inventory_superprize')
    for r in rows:
        acq = str(r[3]).lower()
        out.append(insert_row(schema, 'inventory_superprizeentry',
            ['id', 'client_branch_id', 'acquired_from', 'description',
             'product_id', 'expires_at', 'claimed_at', 'issued_at',
             'created_at', 'updated_at'],
            [r[0], r[5], acq, '', r[6],
             None, r[4], None, r[1], r[2]]
        ))


def migrate_game_clientattempt(content: str, schema: str, out: list):
    """
    game_clientattempt — same structure.
    v4: id, ct, ut, client_id, served_by_id
    """
    out.append(comment(f'{schema}.game_clientattempt'))
    rows = extract_inserts(content, schema, 'game_clientattempt')
    for r in rows:
        out.append(insert_row(schema, 'game_clientattempt',
            ['id', 'client_id', 'served_by_id', 'created_at', 'updated_at'],
            [r[0], r[3], r[4], r[1], r[2]]
        ))


def migrate_quest_quest(content: str, schema: str, out: list):
    """
    quest_quest
    v4: id, ct, ut, name, desc, reward, is_active, branch_id
    v5: adds ordering=0
    """
    out.append(comment(f'{schema}.quest_quest'))
    rows = extract_inserts(content, schema, 'quest_quest')
    for r in rows:
        out.append(insert_row(schema, 'quest_quest',
            ['id', 'branch_id', 'name', 'description', 'reward',
             'is_active', 'ordering', 'created_at', 'updated_at'],
            [r[0], r[7], r[3], r[4] or '', r[5],
             b(r[6]), 0, r[1], r[2]]
        ))


def migrate_quest_questsubmit(content: str, schema: str, out: list):
    """
    quest_questsubmit
    v4: id, ct, ut, is_completed(bool), activated_at, duration('HH:MM:SS'), client_id, quest_id, served_by_id
    v5: duration in minutes, adds expires_at, completed_at (= ut if is_completed)
    """
    out.append(comment(f'{schema}.quest_questsubmit'))
    rows = extract_inserts(content, schema, 'quest_questsubmit')
    for r in rows:
        if len(r) < 8:
            continue
        dur_min = parse_interval_minutes(r[5], default=40)
        expires = ts_add(r[4], minutes=dur_min)
        completed_at = r[2] if r[3] else None  # updated_at if is_completed
        out.append(insert_row(schema, 'quest_questsubmit',
            ['id', 'client_id', 'quest_id', 'served_by_id', 'activated_at',
             'duration', 'expires_at', 'completed_at', 'created_at', 'updated_at'],
            [r[0], r[6], r[7], r[8] if len(r) > 8 else None,
             r[4], dur_min, expires, completed_at, r[1], r[2]]
        ))


def migrate_delivery_delivery(content: str, schema: str, out: list):
    """
    delivery_delivery
    v4: id, ct, ut, code, duration('HH:MM:SS'), activated_at, branch_id, order_source
    v5: adds short_code, duration in hours, expires_at, activated_by_id
    """
    out.append(comment(f'{schema}.delivery_delivery'))
    rows = extract_inserts(content, schema, 'delivery_delivery')
    for r in rows:
        if len(r) < 8:
            continue
        code = str(r[3])
        short_code = code[-5:] if len(code) >= 5 else code
        dur_h = parse_interval_hours(r[4], default=5)
        expires = ts_add(r[1], hours=dur_h)
        out.append(insert_row(schema, 'delivery_delivery',
            ['id', 'branch_id', 'code', 'short_code', 'order_source',
             'duration', 'expires_at', 'activated_at', 'activated_by_id',
             'created_at', 'updated_at'],
            [r[0], r[6], code, short_code, str(r[7]),
             dur_h, expires, r[5], None, r[1], r[2]]
        ))


def migrate_telegram_bot(content: str, schema: str, out: list):
    """
    branch_telegrambot → telegram_telegrambot
    v4: id, ct, ut, name, bot_username, api, branch_id
    """
    out.append(comment(f'{schema}.telegram_telegrambot (from branch_telegrambot)'))
    rows = extract_inserts(content, schema, 'branch_telegrambot')
    for r in rows:
        out.append(insert_row(schema, 'telegram_telegrambot',
            ['id', 'name', 'bot_username', 'api', 'branch_id',
             'created_at', 'updated_at'],
            [r[0], r[3], r[4], r[5], r[6], r[1], r[2]]
        ))


def migrate_telegram_botadmin(content: str, schema: str, out: list):
    """
    branch_botadmin → telegram_botadmin
    v4: id, ct, ut, chat_id, name, verification_token, is_active, bot_id
    """
    out.append(comment(f'{schema}.telegram_botadmin (from branch_botadmin)'))
    rows = extract_inserts(content, schema, 'branch_botadmin')
    for r in rows:
        out.append(insert_row(schema, 'telegram_botadmin',
            ['id', 'bot_id', 'name', 'chat_id', 'verification_token',
             'is_active', 'created_at', 'updated_at'],
            [r[0], r[7], r[4], r[3], r[5], b(r[6]), r[1], r[2]]
        ))


# ── Analytics (stats) ─────────────────────────────────────────────────────────

def migrate_stats_rfsegment(content: str, schema: str, out: list):
    """
    stats_rfsegment → analytics_rfsegment
    v4: id, code, name, recency_min, recency_max, frequency_min, frequency_max,
        emoji, color, strategy, hint, last_campaign_date
    v5: adds created_at, updated_at
    """
    _TS = '2026-01-01 00:00:00+00'
    out.append(comment(f'{schema}.analytics_rfsegment (from stats_rfsegment)'))
    rows = extract_inserts(content, schema, 'stats_rfsegment')
    for r in rows:
        out.append(insert_row(schema, 'analytics_rfsegment',
            ['id', 'created_at', 'updated_at', 'code', 'name',
             'recency_min', 'recency_max', 'frequency_min', 'frequency_max',
             'emoji', 'color', 'strategy', 'hint', 'last_campaign_date'],
            [r[0], _TS, _TS, r[1], r[2], r[3], r[4], r[5], r[6],
             r[7], r[8], r[9], r[10] or '', r[11]]
        ))


def migrate_stats_guestrfscore(content: str, schema: str, out: list):
    """
    stats_guestrfscore → analytics_guestrfscore
    v4: id, recency_days, frequency, r_score, f_score, calculated_at, client_id, segment_id
    """
    out.append(comment(f'{schema}.analytics_guestrfscore'))
    rows = extract_inserts(content, schema, 'stats_guestrfscore')
    for r in rows:
        out.append(insert_row(schema, 'analytics_guestrfscore',
            ['id', 'client_id', 'recency_days', 'frequency',
             'r_score', 'f_score', 'segment_id', 'calculated_at'],
            [r[0], r[6], r[1], r[2], max(r[3], 1), max(r[4], 1), r[7], r[5]]
        ))


def migrate_stats_rfsettings(content: str, schema: str, out: list):
    """
    stats_rfsettings → analytics_rfsettings
    v4: id, analysis_period, branch_id, stats_reset_date
    v5: adds created_at, updated_at
    """
    _TS = '2026-01-01 00:00:00+00'
    out.append(comment(f'{schema}.analytics_rfsettings'))
    rows = extract_inserts(content, schema, 'stats_rfsettings')
    for r in rows:
        out.append(insert_row(schema, 'analytics_rfsettings',
            ['id', 'created_at', 'updated_at', 'branch_id', 'analysis_period', 'stats_reset_date'],
            [r[0], _TS, _TS, r[2], r[1], r[3]]
        ))


def migrate_stats_branchsegmentsnapshot(content: str, schema: str, out: list):
    """
    stats_branchsegmentsnapshot → analytics_branchsegmentsnapshot
    v4: id, guests_count, date, created_at, branch_id, segment_id
    """
    out.append(comment(f'{schema}.analytics_branchsegmentsnapshot'))
    rows = extract_inserts(content, schema, 'stats_branchsegmentsnapshot')
    for r in rows:
        out.append(insert_row(schema, 'analytics_branchsegmentsnapshot',
            ['id', 'branch_id', 'segment_id', 'guests_count', 'date',
             'created_at', 'updated_at'],
            [r[0], r[4], r[5], r[1], r[2], r[3], r[3]]
        ))


def migrate_stats_rfmigrationlog(content: str, schema: str, out: list):
    """
    stats_rfmigrationlog → analytics_rfmigrationlog
    v4: id, ct, client_id, from_segment_id, to_segment_id
    """
    out.append(comment(f'{schema}.analytics_rfmigrationlog'))
    rows = extract_inserts(content, schema, 'stats_rfmigrationlog')
    for r in rows:
        out.append(insert_row(schema, 'analytics_rfmigrationlog',
            ['id', 'created_at', 'client_id', 'from_segment_id', 'to_segment_id'],
            [r[0], r[1], r[2], r[3], r[4]]
        ))


# ── Senler ────────────────────────────────────────────────────────────────────

def migrate_senler_messagetemplate(content: str, schema: str, out: list):
    """
    senler_messagetemplate → senler_autobroadcasttemplate
    v4: id, ct, ut, type, text, is_active
    v5 type mapping: post_game → after_game_3h, birthday_7days → birthday_7d,
                     birthday_today → birthday, birthday_1day → birthday_1d
    """
    TYPE_MAP = {
        'post_game':      'after_game_3h',
        'birthday_7days': 'birthday_7d',
        'birthday_7day':  'birthday_7d',
        'birthday_today': 'birthday',
        'birthday_1day':  'birthday_1d',
        'birthday_1days': 'birthday_1d',
    }
    out.append(comment(f'{schema}.senler_autobroadcasttemplate (from senler_messagetemplate)'))
    rows = extract_inserts(content, schema, 'senler_messagetemplate')
    for r in rows:
        v4_type = str(r[3]).lower()
        v5_type = TYPE_MAP.get(v4_type, v4_type)
        out.append(insert_row(schema, 'senler_autobroadcasttemplate',
            ['id', 'type', 'message_text', 'image', 'is_active',
             'created_at', 'updated_at'],
            [r[0], v5_type, r[4], '', b(r[5]), r[1], r[2]]
        ))


def migrate_senler_broadcasts(content: str, schema: str, out: list):
    """
    v4 senler_mailingcampaign → v5 senler_broadcast + senler_broadcastsend
    v4 senler_messagelog      → v5 senler_broadcastrecipient + senler_autobroadcastlog

    v4 mailingcampaign columns: id, ct, ut, name, description, image, is_scheduled,
                                status, sent_at, group_id, recipient_count
    v4 messagelog columns: id, ct, ut, message_sent_at, status, error_code,
                           vk_id, is_outgoing, read_at, campaign_id, client_id, message_type
    """
    STATUS_MAP = {
        'completed': 'done',
        'pending':   'pending',
        'running':   'running',
        'failed':    'failed',
    }
    RECIPIENT_STATUS_MAP = {
        'sent':    'sent',
        'blocked': 'skipped',
        'failed':  'failed',
    }
    TYPE_MAP = {
        'post_game':      'after_game_3h',
        'birthday_7days': 'birthday_7d',
        'birthday_7day':  'birthday_7d',
        'birthday_today': 'birthday',
        'birthday_1day':  'birthday_1d',
        'birthday_1days': 'birthday_1d',
    }

    # Build AutoBroadcastTemplate type→id lookup from already-migrated templates
    template_lookup = {}
    for r in extract_inserts(content, schema, 'senler_messagetemplate'):
        v4_type = str(r[3]).lower()
        v5_type = TYPE_MAP.get(v4_type, v4_type)
        template_lookup[v5_type] = r[0]
        template_lookup[v4_type] = r[0]

    # Find first branch_id for this schema (needed for Broadcast.branch FK)
    branch_rows = extract_inserts(content, schema, 'branch_branch')
    default_branch_id = branch_rows[0][0] if branch_rows else 1

    # ── Step 1: Campaigns → Broadcast + BroadcastSend ─────────────────────
    out.append(comment(f'{schema}.senler_broadcast + senler_broadcastsend '
                       f'(from senler_mailingcampaign)'))
    campaigns = extract_inserts(content, schema, 'senler_mailingcampaign')
    campaign_ids = set()
    for r in campaigns:
        campaign_ids.add(r[0])
        name = str(r[3])
        desc = str(r[4]) if r[4] else ''
        status = STATUS_MAP.get(str(r[7]).lower(), 'done')
        sent_at = r[8]
        is_auto = '(auto)' in name.lower() or 'авто' in desc.lower()

        if is_auto:
            # Determine template from name
            tpl_id = None
            name_lower = name.lower()
            if 'рожден' in name_lower or 'birthday' in name_lower:
                tpl_id = template_lookup.get('birthday')
            elif 'game' in name_lower or 'игр' in name_lower:
                tpl_id = template_lookup.get('after_game_3h')

            out.append(insert_row(schema, 'senler_broadcastsend',
                ['id', 'broadcast_id', 'auto_broadcast_template_id',
                 'status', 'trigger_type', 'triggered_by',
                 'started_at', 'finished_at',
                 'recipients_count', 'sent_count', 'failed_count', 'skipped_count',
                 'error_message', 'created_at', 'updated_at'],
                [r[0], None, tpl_id,
                 status, 'auto', name,
                 sent_at, sent_at,
                 0, 0, 0, 0,
                 '', r[1], r[2]]
            ))
        else:
            # Manual campaign → Broadcast template + BroadcastSend
            out.append(insert_row(schema, 'senler_broadcast',
                ['id', 'branch_id', 'name', 'message_text',
                 'audience_type', 'gender_filter',
                 'created_at', 'updated_at'],
                [r[0], default_branch_id, name, desc or name,
                 'all', 'all',
                 r[1], r[2]]
            ))
            out.append(insert_row(schema, 'senler_broadcastsend',
                ['id', 'broadcast_id', 'auto_broadcast_template_id',
                 'status', 'trigger_type', 'triggered_by',
                 'started_at', 'finished_at',
                 'recipients_count', 'sent_count', 'failed_count', 'skipped_count',
                 'error_message', 'created_at', 'updated_at'],
                [r[0], r[0], None,
                 status, 'manual', '',
                 sent_at, sent_at,
                 0, 0, 0, 0,
                 '', r[1], r[2]]
            ))

    # ── Step 2: MessageLogs → BroadcastRecipient ──────────────────────────
    out.append(comment(f'{schema}.senler_broadcastrecipient + senler_autobroadcastlog '
                       f'(from senler_messagelog)'))
    logs = extract_inserts(content, schema, 'senler_messagelog')

    # Separate campaign-linked vs orphan auto-sends
    orphan_groups = {}   # (date_str, message_type) → [rows]
    send_stats = {}      # send_id → {sent, failed, skipped, total}

    for r in logs:
        campaign_id = r[9]
        msg_type = str(r[11]) if len(r) > 11 and r[11] else None
        r_status = RECIPIENT_STATUS_MAP.get(str(r[4]).lower(), 'sent')

        if campaign_id and campaign_id in campaign_ids:
            # Link to existing BroadcastSend created from campaign
            out.append(insert_row(schema, 'senler_broadcastrecipient',
                ['id', 'send_id', 'client_branch_id', 'vk_id',
                 'status', 'sent_at', 'error'],
                [r[0], campaign_id, r[10] if r[10] else None,
                 r[6] if r[6] else 0,
                 r_status, r[3], str(r[5] or '')]
            ))
            send_stats.setdefault(campaign_id, {'sent': 0, 'failed': 0, 'skipped': 0, 'total': 0})
            send_stats[campaign_id]['total'] += 1
            if r_status == 'sent':
                send_stats[campaign_id]['sent'] += 1
            elif r_status == 'failed':
                send_stats[campaign_id]['failed'] += 1
            elif r_status == 'skipped':
                send_stats[campaign_id]['skipped'] += 1
        elif msg_type and msg_type not in ('None', 'NULL', ''):
            date_str = str(r[3])[:10]
            orphan_groups.setdefault((date_str, msg_type), []).append(r)

    # Update campaign BroadcastSend stats
    for send_id, st in send_stats.items():
        out.append(
            f"UPDATE {schema}.senler_broadcastsend SET "
            f"recipients_count={st['total']}, sent_count={st['sent']}, "
            f"failed_count={st['failed']}, skipped_count={st['skipped']} "
            f"WHERE id={send_id};"
        )

    # ── Step 3: Orphan auto-sends → new BroadcastSend + BroadcastRecipient
    send_id_counter = 10000  # high range to avoid conflicts with campaign IDs
    for (date_str, msg_type), group in sorted(orphan_groups.items()):
        send_id_counter += 1
        v5_type = TYPE_MAP.get(msg_type, msg_type)
        tpl_id = template_lookup.get(v5_type) or template_lookup.get(msg_type)

        sent_count = sum(1 for r in group if str(r[4]).lower() == 'sent')
        failed_count = sum(1 for r in group if str(r[4]).lower() == 'failed')
        skipped_count = sum(1 for r in group if str(r[4]).lower() == 'blocked')
        first_ts = group[0][3]

        out.append(insert_row(schema, 'senler_broadcastsend',
            ['id', 'broadcast_id', 'auto_broadcast_template_id',
             'status', 'trigger_type', 'triggered_by',
             'started_at', 'finished_at',
             'recipients_count', 'sent_count', 'failed_count', 'skipped_count',
             'error_message', 'created_at', 'updated_at'],
            [send_id_counter, None, tpl_id,
             'done', 'auto', f'{msg_type} auto {date_str}',
             first_ts, first_ts,
             len(group), sent_count, failed_count, skipped_count,
             '', first_ts, first_ts]
        ))

        for r in group:
            r_status = RECIPIENT_STATUS_MAP.get(str(r[4]).lower(), 'sent')
            out.append(insert_row(schema, 'senler_broadcastrecipient',
                ['id', 'send_id', 'client_branch_id', 'vk_id',
                 'status', 'sent_at', 'error'],
                [r[0], send_id_counter, r[10] if r[10] else None,
                 r[6] if r[6] else 0,
                 r_status, r[3], str(r[5] or '')]
            ))

            # AutoBroadcastLog for dedup
            if r_status == 'sent' and r[6] and r[6] not in (None, 'NULL'):
                out.append(insert_row(schema, 'senler_autobroadcastlog',
                    ['trigger_type', 'vk_id', 'sent_at'],
                    [v5_type, r[6], r[3]]
                ))


# ── Sequence Reset ────────────────────────────────────────────────────────────

def reset_sequences(tenant_schemas: list[str], out: list):
    """Reset all PostgreSQL sequences after bulk INSERT with explicit IDs."""
    out.append(comment('Reset sequences'))

    public_tables = [
        'guest_client', 'clients_company', 'clients_domain',
        'users_user', 'config_clientconfig',
    ]
    for t in public_tables:
        out.append(
            f"SELECT setval(pg_get_serial_sequence('public.{t}', 'id'), "
            f"COALESCE((SELECT MAX(id) FROM public.{t}), 1));"
        )

    tenant_tables = [
        'branch_branch', 'branch_branchconfig', 'branch_clientbranch',
        'branch_clientvkstatus', 'branch_clientbranchvisit',
        'branch_cointransaction', 'branch_dailycode', 'branch_cooldown',
        'branch_testimonialconversation', 'branch_testimonialmessage',
        'branch_promotions',
        'catalog_product',
        'inventory_inventoryitem', 'inventory_superprizeentry',
        'game_clientattempt',
        'quest_quest', 'quest_questsubmit',
        'analytics_rfsegment', 'analytics_guestrfscore',
        'analytics_rfsettings', 'analytics_branchsegmentsnapshot',
        'analytics_rfmigrationlog',
        'senler_autobroadcasttemplate',
        'senler_broadcast', 'senler_broadcastsend', 'senler_broadcastrecipient',
        'senler_autobroadcastlog',
        'delivery_delivery',
        'telegram_telegrambot', 'telegram_botadmin',
    ]
    for schema in tenant_schemas:
        for t in tenant_tables:
            out.append(
                f"SELECT setval(pg_get_serial_sequence('{schema}.{t}', 'id'), "
                f"COALESCE((SELECT MAX(id) FROM {schema}.{t}), 1));"
            )


# ── Truncate ──────────────────────────────────────────────────────────────────

def truncate_all(tenant_schemas: list[str], out: list):
    """Clear all destination tables before importing."""
    out.append('\n-- ═══════════════════ TRUNCATE EXISTING DATA ═══════════════════')
    for schema in tenant_schemas:
        out.append(
            f'TRUNCATE '
            f'{schema}.branch_branch, '
            f'{schema}.analytics_rfsegment, '
            f'{schema}.catalog_product, '
            f'{schema}.senler_autobroadcasttemplate, '
            f'{schema}.senler_broadcast, '
            f'{schema}.telegram_telegrambot, '
            f'{schema}.delivery_delivery '
            f'RESTART IDENTITY CASCADE;'
        )
    out.append(
        'TRUNCATE public.guest_client, public.clients_company, '
        'public.config_clientconfig, public.users_user '
        'RESTART IDENTITY CASCADE;'
    )


# ── Main Orchestration ────────────────────────────────────────────────────────

def process_public(content: str, out: list):
    out.append('\n-- ═══════════════════ PUBLIC SCHEMA ═══════════════════')
    migrate_guest_client(content, out)
    migrate_clients_company(content, out)
    migrate_clients_domain(content, out)
    migrate_users_user(content, out)
    migrate_config_clientconfig(content, out)


def process_tenant(content: str, schema: str, out: list):
    out.append(f'\n-- ═══════════════════ TENANT: {schema} ═══════════════════')

    # Core structure
    migrate_branch_branch(content, schema, out)
    migrate_branch_branchconfig(content, schema, out)

    # Clients & visits
    migrate_branch_clientbranch(content, schema, out)
    migrate_branch_clientbranchvisit(content, schema, out)
    migrate_branch_cointransaction(content, schema, out)

    # Daily codes (merged from 3 tables)
    migrate_all_dailycodes(content, schema, out)

    # Cooldowns (merged from 4 tables)
    migrate_all_cooldowns(content, schema, out)

    # Testimonials (split into conversation + messages)
    migrate_branch_testimonials(content, schema, out)
    migrate_testimonial_replies(content, schema, out)

    # Promotions
    migrate_branch_promotions(content, schema, out)

    # Catalog & Game
    migrate_catalog_product(content, schema, out)
    migrate_game_clientattempt(content, schema, out)

    # Inventory
    migrate_inventory_inventory(content, schema, out)
    migrate_inventory_superprize(content, schema, out)

    # Quests
    migrate_quest_quest(content, schema, out)
    migrate_quest_questsubmit(content, schema, out)

    # Delivery
    migrate_delivery_delivery(content, schema, out)

    # Analytics (RF)
    migrate_stats_rfsegment(content, schema, out)
    migrate_stats_guestrfscore(content, schema, out)
    migrate_stats_rfsettings(content, schema, out)
    migrate_stats_branchsegmentsnapshot(content, schema, out)
    migrate_stats_rfmigrationlog(content, schema, out)

    # Senler
    migrate_senler_messagetemplate(content, schema, out)
    migrate_senler_broadcasts(content, schema, out)

    # Telegram
    migrate_telegram_bot(content, schema, out)
    migrate_telegram_botadmin(content, schema, out)


def main():
    if len(sys.argv) < 2:
        print('Usage: python scripts/migrate_v4.py backup_v4_data.sql > migrate_v5.sql',
              file=sys.stderr)
        sys.exit(1)

    src = sys.argv[1]
    skip_schemas = set()
    if len(sys.argv) > 2 and sys.argv[2] == '--skip-dev':
        skip_schemas.add('dev')

    print(f'Reading {src} ...', file=sys.stderr)
    with open(src, encoding='utf-8') as f:
        content = f.read()

    out: list[str] = []
    out.append('-- LevOne v4 → v5 Data Migration (auto-generated)')
    out.append('-- Run AFTER: python manage.py migrate_schemas')
    out.append('-- ')
    out.append('-- MANUAL STEPS after migration:')
    out.append('--   1. Re-enter VK tokens in senler_senlerconfig via admin')
    out.append('--   2. Re-upload knowledge base documents in analytics')
    out.append('--   3. Verify media files (products/, broadcasts/, story_image/)')
    out.append('')
    out.append("SET session_replication_role = 'replica';  -- disable FK triggers")
    out.append('BEGIN;')

    # Discover tenant schemas
    schemas: set[str] = set()
    for m in re.finditer(r'INSERT INTO (\w+)\.\w+ VALUES', content):
        s = m.group(1)
        if s != 'public':
            schemas.add(s)
    tenant_schemas = sorted(schemas - skip_schemas)
    print(f'Found tenant schemas: {tenant_schemas}', file=sys.stderr)

    truncate_all(tenant_schemas, out)
    process_public(content, out)

    for schema in tenant_schemas:
        process_tenant(content, schema, out)

    reset_sequences(tenant_schemas, out)

    out.append('')
    out.append('COMMIT;')
    out.append("SET session_replication_role = 'origin';")
    out.append('')
    out.append('-- DONE ✓')

    print('\n'.join(out))
    print(f'Done. {len(out)} output lines.', file=sys.stderr)


if __name__ == '__main__':
    main()
