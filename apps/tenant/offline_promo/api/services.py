"""
Offline promo services — business logic for the «Новые клиенты» mechanic.

All functions are independently testable and do exactly one thing.

ВАЖНО: эта механика полностью изолирована от основной игры в кафе.
Пользователи, пришедшие через офлайн-рекламу, не попадают в основную
аналитику (RF-анализ, сегментация, визиты).
"""
from __future__ import annotations

from datetime import timedelta

from django.core import signing
from django.db import transaction
from django.utils import timezone

from apps.tenant.branch.models import ClientBranch, ClientVKStatus
from ..models import (
    OfflinePromoScan, OfflinePromoGift,
    OfflinePromoConfigModel, GIFT_VALIDITY_DAYS,
)


# ── Exceptions ────────────────────────────────────────────────────────────────

class ClientNotFound(Exception):
    pass


class AlreadyPlayed(Exception):
    """Guest already played the offline promo game."""
    def __init__(self, gift: OfflinePromoGift):
        self.gift = gift


class PromoDisabled(Exception):
    """Offline promo is disabled for this branch."""
    pass


class InvalidToken(Exception):
    pass


class GiftNotFound(Exception):
    pass


class GiftNotUsable(Exception):
    """Gift is already activated or expired."""
    def __init__(self, status: str):
        self.status = status


# ── Internal helpers ──────────────────────────────────────────────────────────

def _get_client_branch(vk_id: int, branch_id: int) -> ClientBranch:
    try:
        return ClientBranch.objects.select_related('branch').get(
            client__vk_id=vk_id, branch__branch_id=branch_id,
        )
    except ClientBranch.DoesNotExist:
        raise ClientNotFound


def _get_promo_config(branch) -> OfflinePromoConfigModel | None:
    try:
        return branch.offline_promo_config
    except OfflinePromoConfigModel.DoesNotExist:
        return None


# ── Public service functions ──────────────────────────────────────────────────

def check_already_played(vk_id: int, branch_id: int) -> dict:
    """
    Проверяет, участвовал ли гость уже в офлайн-промо.

    Returns:
        {'already_played': bool, 'gift': dict | None}
    """
    client_branch = _get_client_branch(vk_id, branch_id)

    try:
        gift = client_branch.offline_promo_gift
        return {
            'already_played': True,
            'gift': {
                'id': gift.pk,
                'product_name': gift.product.name if gift.product else None,
                'status': gift.status,
                'days_remaining': gift.days_remaining,
                'expires_at': gift.expires_at.isoformat(),
                'activated_at': gift.activated_at.isoformat() if gift.activated_at else None,
            },
        }
    except OfflinePromoGift.DoesNotExist:
        return {'already_played': False, 'gift': None}


def record_scan(
    vk_id: int,
    branch_id: int,
    source: str = '',
    ip_address: str | None = None,
) -> OfflinePromoScan:
    """
    Записывает факт сканирования QR-кода из офлайн-рекламы.

    Записывается КАЖДЫЙ раз (даже повторно) для подсчёта общего числа
    сканирований.
    """
    client_branch = _get_client_branch(vk_id, branch_id)
    return OfflinePromoScan.objects.create(
        client=client_branch,
        source=source,
        ip_address=ip_address,
    )


def start_offline_game(
    vk_id: int,
    branch_id: int,
    source: str = '',
    ip_address: str | None = None,
) -> dict:
    """
    Phase 1 — Проверяет право на игру и выдаёт сессионный токен.

    Записывает скан при каждом переходе. Если гость уже играл —
    выбрасывает AlreadyPlayed.

    Returns:
        {'session_token': str, 'score': int, 'congratulation_text': str}

    Raises:
        ClientNotFound  — профиль не найден
        AlreadyPlayed   — гость уже участвовал
        PromoDisabled   — офлайн-промо выключено для этой точки
    """
    client_branch = _get_client_branch(vk_id, branch_id)

    # Записываем скан (каждый раз)
    OfflinePromoScan.objects.create(
        client=client_branch,
        source=source,
        ip_address=ip_address,
    )

    # Проверяем конфигурацию
    config = _get_promo_config(client_branch.branch)
    if config and not config.is_enabled:
        raise PromoDisabled

    # Проверяем уникальность участия
    try:
        existing_gift = client_branch.offline_promo_gift
        raise AlreadyPlayed(existing_gift)
    except OfflinePromoGift.DoesNotExist:
        pass

    # Генерируем сессионный токен (10 мин TTL)
    token = signing.dumps({
        'cbid': client_branch.pk,
        'type': 'offline_promo',
        'src': source,
    })

    congrats = ''
    min_order = ''
    if config:
        congrats = config.congratulation_text
        min_order = config.min_order_text

    return {
        'session_token': token,
        'score': 10,  # максимальный score для анимации
        'congratulation_text': congrats,
        'min_order_text': min_order,
    }


@transaction.atomic
def claim_offline_gift(
    session_token: str,
    product_id: int | None = None,
) -> dict:
    """
    Phase 2 — Создаёт подарок и закрепляет его за гостем.

    Replay-safe: если подарок уже существует, возвращает его.
    Токен действует 10 минут.

    Returns:
        {'gift': OfflinePromoGift, 'congratulation_text': str}

    Raises:
        InvalidToken — токен невалиден, просрочен или не offline_promo
    """
    try:
        payload = signing.loads(session_token, max_age=600)
    except signing.BadSignature:
        raise InvalidToken

    if payload.get('type') != 'offline_promo':
        raise InvalidToken

    try:
        client_branch = (
            ClientBranch.objects
            .select_for_update()
            .select_related('branch')
            .get(pk=payload['cbid'])
        )
    except (ClientBranch.DoesNotExist, KeyError):
        raise InvalidToken

    # Идемпотентность: если подарок уже есть, возвращаем его
    try:
        existing = client_branch.offline_promo_gift
        return {
            'gift': existing,
            'congratulation_text': existing.congratulation_text,
        }
    except OfflinePromoGift.DoesNotExist:
        pass

    # Определяем продукт (если передан)
    product = None
    if product_id:
        from apps.tenant.catalog.models import Product
        product = Product.objects.filter(pk=product_id).first()

    # Получаем конфигурацию
    config = _get_promo_config(client_branch.branch)
    validity_days = config.gift_validity_days if config else GIFT_VALIDITY_DAYS
    congrats = config.congratulation_text if config else ''

    now = timezone.now()
    gift = OfflinePromoGift.objects.create(
        client=client_branch,
        product=product,
        source=payload.get('src', ''),
        congratulation_text=congrats,
        expires_at=now + timedelta(days=validity_days),
    )

    return {
        'gift': gift,
        'congratulation_text': congrats,
    }


def select_gift_product(
    vk_id: int,
    branch_id: int,
    product_id: int,
) -> OfflinePromoGift:
    """
    Гость выбирает конкретный продукт для своего подарка.

    Вызывается после claim_offline_gift, если приз ещё не выбран.
    """
    client_branch = _get_client_branch(vk_id, branch_id)

    try:
        gift = client_branch.offline_promo_gift
    except OfflinePromoGift.DoesNotExist:
        raise GiftNotFound

    if not gift.is_usable:
        raise GiftNotUsable(gift.status)

    from apps.tenant.catalog.models import Product
    product = Product.objects.filter(pk=product_id).first()
    if not product:
        raise GiftNotFound

    gift.product = product
    gift.save(update_fields=['product'])
    return gift


@transaction.atomic
def activate_gift_in_cafe(
    vk_id: int,
    branch_id: int,
    activation_branch_id: int | None = None,
) -> OfflinePromoGift:
    """
    Активировать подарок в кафе (подтверждение от персонала).

    activation_branch_id — точка, где происходит активация.
    Если не указан, используется branch_id из профиля гостя.

    Raises:
        GiftNotFound  — подарок не найден
        GiftNotUsable — подарок уже активирован или истёк
    """
    client_branch = _get_client_branch(vk_id, branch_id)

    try:
        gift = (
            OfflinePromoGift.objects
            .select_for_update()
            .get(client=client_branch)
        )
    except OfflinePromoGift.DoesNotExist:
        raise GiftNotFound

    if not gift.is_usable:
        raise GiftNotUsable(gift.status)

    # Определяем точку активации
    activation_branch = None
    if activation_branch_id:
        from apps.tenant.branch.models import Branch
        activation_branch = Branch.objects.filter(
            branch_id=activation_branch_id,
        ).first() or client_branch.branch
    else:
        activation_branch = client_branch.branch

    gift.activate(branch=activation_branch)
    return gift


def get_gift_status(vk_id: int, branch_id: int) -> dict | None:
    """
    Возвращает статус подарка гостя или None если подарка нет.
    """
    client_branch = _get_client_branch(vk_id, branch_id)

    try:
        gift = client_branch.offline_promo_gift
    except OfflinePromoGift.DoesNotExist:
        return None

    return {
        'id': gift.pk,
        'product_id': gift.product_id,
        'product_name': gift.product.name if gift.product else None,
        'product_image': gift.product.image.url if gift.product and gift.product.image else None,
        'status': gift.status,
        'days_remaining': gift.days_remaining,
        'received_at': gift.received_at.isoformat(),
        'expires_at': gift.expires_at.isoformat(),
        'activated_at': gift.activated_at.isoformat() if gift.activated_at else None,
        'activated_branch': gift.activated_branch.name if gift.activated_branch else None,
        'source': gift.source,
    }


# ── Analytics helpers (for «Новые клиенты» dashboard) ────────────────────────

def get_new_clients_stats(
    branch_ids: list[int] | None,
    start_date,
    end_date,
    source: str | None = None,
) -> dict:
    """
    Все метрики для раздела «Новые клиенты».

    branch_ids — фильтр по точкам (None = все)
    source     — фильтр по рекламному источнику (None = все)
    """
    from django.db.models import Q

    # ── Base querysets with filters ──────────────────────────────────────────

    scan_qs = OfflinePromoScan.objects.filter(
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )
    gift_qs = OfflinePromoGift.objects.filter(
        created_at__date__gte=start_date,
        created_at__date__lte=end_date,
    )

    if branch_ids:
        scan_qs = scan_qs.filter(client__branch__in=branch_ids)
        gift_qs = gift_qs.filter(client__branch__in=branch_ids)

    if source:
        scan_qs = scan_qs.filter(source=source)
        gift_qs = gift_qs.filter(source=source)

    # ── Metric 1: Total scans ────────────────────────────────────────────────

    total_scans = scan_qs.count()

    # ── Metric 2: Unique users who scanned ───────────────────────────────────

    unique_users = scan_qs.values('client').distinct().count()

    # ── Metric 3: VK community subscribers (from offline promo users) ────────

    promo_client_ids = scan_qs.values_list('client_id', flat=True).distinct()

    vk_community_subs = ClientVKStatus.objects.filter(
        client__in=promo_client_ids,
        is_community_member=True,
    ).count()

    # ── Metric 4: VK newsletter subscribers (from offline promo users) ───────

    vk_newsletter_subs = ClientVKStatus.objects.filter(
        client__in=promo_client_ids,
        is_newsletter_subscriber=True,
    ).count()

    # ── Metric 5: Gifts received ─────────────────────────────────────────────

    gifts_received = gift_qs.count()

    # ── Metric 6: Gifts activated in café ────────────────────────────────────

    gifts_activated = gift_qs.filter(activated_at__isnull=False).count()

    # ── Metric 7: Gifts expired ──────────────────────────────────────────────

    now = timezone.now()
    gifts_expired = gift_qs.filter(
        activated_at__isnull=True,
        expires_at__lt=now,
    ).count()

    # ── Metric 8: Gifts still active ─────────────────────────────────────────

    gifts_active = gift_qs.filter(
        activated_at__isnull=True,
        expires_at__gte=now,
    ).count()

    # ── Conversion rate: scans → activations ─────────────────────────────────

    conversion_rate = (
        round(gifts_activated / unique_users * 100, 1)
        if unique_users > 0
        else 0.0
    )

    return {
        'total_scans': total_scans,
        'unique_users': unique_users,
        'vk_community_subscribers': vk_community_subs,
        'vk_newsletter_subscribers': vk_newsletter_subs,
        'gifts_received': gifts_received,
        'gifts_activated': gifts_activated,
        'gifts_expired': gifts_expired,
        'gifts_active': gifts_active,
        'conversion_rate': conversion_rate,
    }


def get_new_clients_detail(
    branch_ids: list[int] | None,
    start_date,
    end_date,
    source: str | None = None,
) -> list[dict]:
    """
    Детальный список пользователей для раздела «Новые клиенты».

    Возвращает список с датами сканирования, получения и активации подарков.
    """
    from django.db.models import Min

    scan_qs = OfflinePromoScan.objects.all()
    if branch_ids:
        scan_qs = scan_qs.filter(client__branch__in=branch_ids)
    if source:
        scan_qs = scan_qs.filter(source=source)

    # Уникальные клиенты с датой первого скана
    client_data = (
        scan_qs
        .filter(
            created_at__date__gte=start_date,
            created_at__date__lte=end_date,
        )
        .values('client_id')
        .annotate(first_scan=Min('created_at'))
        .order_by('-first_scan')
    )

    result = []
    for row in client_data[:200]:  # лимит 200 записей
        cb_id = row['client_id']
        try:
            cb = ClientBranch.objects.select_related('client', 'branch').get(pk=cb_id)
        except ClientBranch.DoesNotExist:
            continue

        gift_info = None
        try:
            gift = cb.offline_promo_gift
            gift_info = {
                'product_name': gift.product.name if gift.product else None,
                'status': gift.status,
                'received_at': gift.received_at.isoformat(),
                'expires_at': gift.expires_at.isoformat(),
                'activated_at': gift.activated_at.isoformat() if gift.activated_at else None,
                'source': gift.source,
            }
        except OfflinePromoGift.DoesNotExist:
            pass

        vk_info = {}
        try:
            vk = cb.vk_status
            vk_info = {
                'is_community_member': vk.is_community_member,
                'is_newsletter_subscriber': vk.is_newsletter_subscriber,
            }
        except ClientVKStatus.DoesNotExist:
            pass

        result.append({
            'client_branch_id': cb_id,
            'vk_id': cb.client.vk_id,
            'name': str(cb.client),
            'branch_name': cb.branch.name,
            'first_scan_at': row['first_scan'].isoformat(),
            'gift': gift_info,
            'vk': vk_info,
        })

    return result


def get_promo_sources(
    branch_ids: list[int] | None = None,
) -> list[dict]:
    """
    Список всех рекламных источников с количеством сканирований.
    Для фильтра в админке.
    """
    from django.db.models import Count

    qs = OfflinePromoScan.objects.exclude(source='')
    if branch_ids:
        qs = qs.filter(client__branch__in=branch_ids)

    return list(
        qs.values('source')
        .annotate(count=Count('id'))
        .order_by('-count')
    )
