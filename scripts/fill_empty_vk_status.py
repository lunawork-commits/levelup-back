"""
Заполняет ClientVKStatus для гостей у которых нет никаких данных.

«Пустая» запись — все VK-поля равны дефолту:
  is_community_member=False, community_via_app=None, community_joined_at=None,
  is_newsletter_subscriber=False, newsletter_via_app=None, newsletter_joined_at=None.

Так же обрабатываются гости у которых записи ClientVKStatus вообще нет.

Логика:
  1. Собираем "пустых" гостей с vk_id.
  2. Батчами проверяем через VK API groups.isMember.
  3. Подписан в VK:
       - is_community_member = True
       - community_via_app   = True  + community_joined_at = дата первого суперприза (если есть суперприз от игры)
       - community_via_app   = False + community_joined_at = None                    (если суперприза нет — был до приложения)
  4. Не подписан:
       - is_community_member = False, community_via_app = None

Запуск:
    sudo docker compose exec web python scripts/fill_empty_vk_status.py --dry-run
    sudo docker compose exec web python scripts/fill_empty_vk_status.py --schema levone
"""
import django
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from django.db.models import Min, Q
from django_tenants.utils import get_tenant_model, schema_context

VK_API_BASE    = 'https://api.vk.com/method/'
VK_API_VERSION = '5.131'
BATCH_SIZE     = 500


# ── VK API ────────────────────────────────────────────────────────────────────

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


def check_members(group_id: int, vk_ids: list, token: str) -> dict:
    """Returns {vk_id: is_member}."""
    result = {}
    for i in range(0, len(vk_ids), BATCH_SIZE):
        batch = vk_ids[i:i + BATCH_SIZE]
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


# ── Core logic ────────────────────────────────────────────────────────────────

def fill_schema(schema: str, group_id: int, token: str, dry_run: bool):
    from apps.tenant.branch.models import ClientBranch, ClientVKStatus
    from apps.tenant.inventory.models import SuperPrizeEntry

    with schema_context(schema):

        # ── 1. Гости без записи vkstatus вообще ──────────────────────────────
        cb_ids_no_status = set(
            ClientBranch.objects
            .filter(client__vk_id__isnull=False)
            .exclude(vk_status__isnull=False)
            .values_list('id', flat=True)
        )

        # ── 2. Гости с "пустой" записью (все поля дефолтные) ─────────────────
        cb_ids_empty = set(
            ClientVKStatus.objects.filter(
                is_community_member=False,
                community_via_app__isnull=True,
                community_joined_at__isnull=True,
                is_newsletter_subscriber=False,
                newsletter_via_app__isnull=True,
                newsletter_joined_at__isnull=True,
            ).values_list('client_id', flat=True)
        )

        target_cb_ids = cb_ids_no_status | cb_ids_empty
        print(f'  [{schema}] Без записи: {len(cb_ids_no_status)}, пустых записей: {len(cb_ids_empty)}')
        print(f'  [{schema}] Итого обрабатываем: {len(target_cb_ids)} гостей')

        if not target_cb_ids:
            print(f'  [{schema}] Нечего делать.')
            return

        # ── 3. Собираем vk_id → [cb_ids] ─────────────────────────────────────
        cbs = list(
            ClientBranch.objects
            .select_related('client')
            .filter(id__in=target_cb_ids, client__vk_id__isnull=False)
        )
        vk_id_to_cbs: dict = {}
        for cb in cbs:
            vk_id_to_cbs.setdefault(cb.client.vk_id, []).append(cb)

        if not vk_id_to_cbs:
            print(f'  [{schema}] Нет гостей с vk_id — пропускаем.')
            return

        # ── 4. VK API ─────────────────────────────────────────────────────────
        print(f'  [{schema}] Проверяем {len(vk_id_to_cbs)} VK-аккаунтов через API...')
        try:
            member_map = check_members(group_id, list(vk_id_to_cbs.keys()), token)
        except RuntimeError as e:
            print(f'  [{schema}] VK API ошибка: {e}')
            return

        subscribed = sum(1 for v in member_map.values() if v)
        print(f'  [{schema}] VK: {subscribed} подписаны, {len(member_map) - subscribed} нет')

        # ── 5. Суперпризы от игры ─────────────────────────────────────────────
        first_prize_dates = dict(
            SuperPrizeEntry.objects
            .filter(acquired_from='game', client_branch_id__in=target_cb_ids)
            .values('client_branch_id')
            .annotate(first_at=Min('created_at'))
            .values_list('client_branch_id', 'first_at')
        )
        game_cb_ids = set(first_prize_dates.keys())
        print(f'  [{schema}] Суперпризов от игры: {len(game_cb_ids)}')

        # ── 6. Обновляем ─────────────────────────────────────────────────────
        created = updated = skipped = 0

        for vk_id, client_branches in vk_id_to_cbs.items():
            is_member = member_map.get(vk_id, False)

            for cb in client_branches:
                has_game_prize = cb.id in game_cb_ids
                prize_date     = first_prize_dates.get(cb.id)

                if is_member:
                    if has_game_prize:
                        community_via_app   = True
                        community_joined_at = prize_date
                    else:
                        community_via_app   = False
                        community_joined_at = None
                    is_community_member = True
                else:
                    is_community_member = False
                    community_via_app   = None
                    community_joined_at = None

                # Создаём или обновляем
                if cb.id in cb_ids_no_status:
                    if not dry_run:
                        ClientVKStatus.objects.create(
                            client=cb,
                            is_community_member=is_community_member,
                            community_via_app=community_via_app,
                            community_joined_at=community_joined_at,
                        )
                    created += 1
                else:
                    # Пустая запись — обновляем только если есть что менять
                    if is_community_member or community_via_app is not None:
                        if not dry_run:
                            ClientVKStatus.objects.filter(client=cb).update(
                                is_community_member=is_community_member,
                                community_via_app=community_via_app,
                                community_joined_at=community_joined_at,
                            )
                        updated += 1
                    else:
                        skipped += 1

        suffix = ' [DRY RUN]' if dry_run else ''
        print(
            f'  [{schema}] Создано: {created}, обновлено: {updated}, '
            f'пропущено (не подписаны, нет приза): {skipped}{suffix}'
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema',   type=str, help='Tenant schema (default: all)')
    parser.add_argument('--group-id', type=int, help='VK group ID (override SenlerConfig)')
    parser.add_argument('--token',    type=str, help='VK token (override SenlerConfig)')
    parser.add_argument('--dry-run',  action='store_true')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN — ничего не записывается\n')

    Company = get_tenant_model()
    tenants = Company.objects.exclude(schema_name='public')
    if args.schema:
        tenants = tenants.filter(schema_name=args.schema)

    if not tenants.exists():
        print(f'Tenant не найден.')
        return

    for company in tenants:
        schema = company.schema_name
        print(f'\nTenant: {company.name} ({schema})')

        if args.group_id and args.token:
            group_id = args.group_id
            token    = args.token
        else:
            from apps.tenant.senler.models import SenlerConfig
            with schema_context(schema):
                cfg = SenlerConfig.objects.first()
            if not cfg or not cfg.vk_group_id or not cfg.vk_community_token:
                print(f'  [{schema}] Нет SenlerConfig. Передай --group-id и --token.')
                continue
            group_id = cfg.vk_group_id
            token    = cfg.vk_community_token

        fill_schema(schema, group_id, token, args.dry_run)

    print('\nGotovo.')


if __name__ == '__main__':
    main()
