from django.db import transaction
from django.utils import timezone
from django.db.models import Q, Sum
from django.db.models.functions import Coalesce

from apps.shared.guest.models import Client
from ..models import (
    Branch, ClientBranch, ClientBranchVisit, ClientVKStatus, CoinTransaction, Promotions,
    TestimonialConversation, TestimonialMessage,
)


# ── Exceptions ────────────────────────────────────────────────────────────────

class BranchNotFound(Exception):
    pass


class BranchInactive(Exception):
    pass


class ClientNotFound(Exception):
    pass


class ClientBlocked(Exception):
    pass


# ── Internal helpers ──────────────────────────────────────────────────────────

def _profile_qs():
    """ClientBranch queryset with pre-loaded relations and balance annotation."""
    return (
        ClientBranch.objects
        .select_related('client', 'vk_status')
        .annotate(
            _coins_balance=(
                Coalesce(Sum('transactions__amount', filter=Q(transactions__type='income')), 0)
                - Coalesce(Sum('transactions__amount', filter=Q(transactions__type='expense')), 0)
            )
        )
    )


def _image_url(field) -> str | None:
    """Returns relative image URL or None if no file is associated."""
    return field.url if (field and field.name) else None


# ── Public service functions ──────────────────────────────────────────────────

def get_branch_info(branch_id: int, *, tenant=None) -> dict:
    """
    Returns branch data merged with tenant config for the given branch_id.

    branch_id: the QR-code integer ID (Branch.branch_id field), not DB PK.
    tenant:    django-tenants Company instance (from request.tenant).

    Raises:
        BranchNotFound  — branch_id doesn't exist
        BranchInactive  — branch is disabled
    """
    try:
        branch = Branch.objects.select_related('config').get(branch_id=branch_id)
    except Branch.DoesNotExist:
        raise BranchNotFound

    if not branch.is_active:
        raise BranchInactive

    branch_config = getattr(branch, 'config', None)

    config = None
    if tenant is not None:
        from apps.shared.config.models import ClientConfig
        try:
            config = ClientConfig.objects.get(company=tenant)
        except ClientConfig.DoesNotExist:
            pass

    return {
        'id':              branch.pk,
        'branch_id':       branch.branch_id,
        'name':            branch.name,
        'address':         branch_config.address    if branch_config else '',
        'phone':           branch_config.phone      if branch_config else '',
        'yandex_map':      branch_config.yandex_map if branch_config else '',
        'gis_map':         branch_config.gis_map    if branch_config else '',
        'logotype_url':    _image_url(config.logotype_image) if config else None,
        'coin_icon_url':   _image_url(config.coin_image)     if config else None,
        'vk_group_id':     config.vk_group_id   if config else None,
        'vk_group_name':   config.vk_group_name if config else None,
        'story_image_url': _image_url(branch.story_image),
    }


def get_client_profile(vk_id: int, branch_id: int) -> ClientBranch:
    """
    Returns ClientBranch for the given (vk_id, branch_id) pair.

    Raises:
        ClientNotFound — no profile exists for this combination
    """
    try:
        return _profile_qs().get(client__vk_id=vk_id, branch__branch_id=branch_id)
    except ClientBranch.DoesNotExist:
        raise ClientNotFound


@transaction.atomic
def register_or_get_client(
    vk_id: int,
    branch_id: int,
    *,
    first_name: str = '',
    last_name: str = '',
    photo_url: str = '',
    birth_date=None,
) -> tuple[ClientBranch, bool]:
    """
    Atomically finds or creates a ClientBranch for the given guest.

    Flow:
      1. Validate Branch (exists, active)
      2. Get or create guest.Client by vk_id; sync mutable fields if changed
      3. Get or create ClientBranch (guest × branch)
      4. Record QR-scan visit (6-hour cooldown, atomic)

    Returns:
        (ClientBranch, created: bool)

    Raises:
        BranchNotFound  — branch_id not found
        BranchInactive  — branch is disabled
        ClientBlocked   — client.is_active=False
    """
    try:
        branch = Branch.objects.get(branch_id=branch_id)
    except Branch.DoesNotExist:
        raise BranchNotFound

    if not branch.is_active:
        raise BranchInactive

    client, client_created = Client.objects.get_or_create(
        vk_id=vk_id,
        defaults={
            'first_name': first_name,
            'last_name':  last_name,
            'photo_url':  photo_url,
        },
    )

    if not client_created:
        if not client.is_active:
            raise ClientBlocked

        # VK profile fields (name, photo) can change between sessions
        updates = {}
        if first_name and client.first_name != first_name:
            updates['first_name'] = first_name
        if last_name and client.last_name != last_name:
            updates['last_name'] = last_name
        if photo_url and client.photo_url != photo_url:
            updates['photo_url'] = photo_url
        if updates:
            for attr, val in updates.items():
                setattr(client, attr, val)
            client.save(update_fields=list(updates))

    profile, created = ClientBranch.objects.get_or_create(
        client=client,
        branch=branch,
        defaults={
            'birth_date': birth_date,
            'birth_date_set_at': timezone.localdate() if birth_date else None,
        },
    )

    # Record visit: atomic, 6-hour cooldown
    ClientBranchVisit.record_visit(profile)

    # ── Sync VK membership status on first registration ──────────────────
    if created:
        _sync_vk_status_on_register(profile)

    # Re-fetch with all relations and fresh balance annotation
    return _profile_qs().get(pk=profile.pk), created


def _sync_vk_status_on_register(profile: ClientBranch) -> None:
    """
    При первой регистрации проверяет через VK API:
      - groups.isMember              → is_community_member
      - messages.isMessagesFromGroupAllowed → is_newsletter_subscriber

    Если SenlerConfig или токен не настроен — молча пропускает.
    """
    import json
    import logging
    import urllib.error
    import urllib.parse
    import urllib.request

    from apps.tenant.senler.models import SenlerConfig

    logger = logging.getLogger(__name__)

    try:
        config = SenlerConfig.objects.get(branch=profile.branch)
    except SenlerConfig.DoesNotExist:
        return

    token = config.vk_community_token
    group_id = config.vk_group_id
    if not token or not group_id:
        return

    vk_id = profile.client.vk_id

    def _vk_call(method, **params):
        params['access_token'] = token
        params['v'] = '5.131'
        url = f'https://api.vk.com/method/{method}?' + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        if 'error' in data:
            raise RuntimeError(data['error'].get('error_msg', ''))
        return data.get('response', {})

    is_member = False
    is_subscriber = False

    try:
        resp = _vk_call('groups.isMember', group_id=group_id, user_id=vk_id)
        is_member = bool(resp) if isinstance(resp, int) else bool(resp.get('member', 0))
    except Exception as e:
        logger.warning('VK sync on register groups.isMember vk_id=%s: %s', vk_id, e)

    try:
        resp = _vk_call('messages.isMessagesFromGroupAllowed', group_id=group_id, user_id=vk_id)
        is_subscriber = bool(resp.get('is_allowed', 0))
    except Exception as e:
        logger.warning('VK sync on register isMessagesFromGroupAllowed vk_id=%s: %s', vk_id, e)

    ClientVKStatus.sync(profile, is_member=is_member, is_subscriber=is_subscriber)


@transaction.atomic
def update_client_profile(
    vk_id: int,
    branch_id: int,
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    photo_url: str | None = None,
    birth_date=None,
    community_via_app: bool | None = None,
    newsletter_via_app: bool | None = None,
    is_community_member: bool | None = None,
    is_newsletter_subscriber: bool | None = None,
) -> ClientBranch:
    """
    Partially updates a client profile. Only non-None fields are written.

      first_name / last_name / photo_url → guest.Client
      birth_date                         → ClientBranch
      is_community_member=True/False     → ClientVKStatus.sync() — факт подписки из VK Bridge,
                                           без атрибуции (pre-existing → via_app=False)
      is_newsletter_subscriber=True/False→ то же для рассылки
      community_via_app=True             → ClientVKStatus.mark_subscribed(community=True)
      newsletter_via_app=True            → ClientVKStatus.mark_subscribed(newsletter=True)

    Raises:
        ClientNotFound — no profile for (vk_id, branch_id)
    """
    profile = get_client_profile(vk_id=vk_id, branch_id=branch_id)

    cb_updates = []
    if birth_date is not None:
        profile.birth_date = birth_date
        cb_updates.append('birth_date')
        if profile.birth_date_set_at is None:
            profile.birth_date_set_at = timezone.localdate()
            cb_updates.append('birth_date_set_at')
    if cb_updates:
        profile.save(update_fields=cb_updates)

    client = profile.client
    cl_updates = []
    if first_name is not None:
        client.first_name = first_name
        cl_updates.append('first_name')
    if last_name is not None:
        client.last_name = last_name
        cl_updates.append('last_name')
    if photo_url is not None:
        client.photo_url = photo_url
        cl_updates.append('photo_url')
    if cl_updates:
        client.save(update_fields=cl_updates)

    # Sync membership status from VK Bridge (app init) — no attribution change
    if is_community_member is not None or is_newsletter_subscriber is not None:
        current = getattr(profile, 'vk_status', None)
        ClientVKStatus.sync(
            profile,
            is_member=(
                is_community_member if is_community_member is not None
                else (current.is_community_member if current else False)
            ),
            is_subscriber=(
                is_newsletter_subscriber if is_newsletter_subscriber is not None
                else (current.is_newsletter_subscriber if current else False)
            ),
        )

    # Attribution: user explicitly joined via app
    if community_via_app or newsletter_via_app:
        vk_status, _ = ClientVKStatus.objects.get_or_create(client=profile)
        vk_status.mark_subscribed(
            community=bool(community_via_app),
            newsletter=bool(newsletter_via_app),
        )

    return _profile_qs().get(pk=profile.pk)


def _get_active_branch(branch_id: int) -> Branch:
    try:
        branch = Branch.objects.get(branch_id=branch_id)
    except Branch.DoesNotExist:
        raise BranchNotFound
    if not branch.is_active:
        raise BranchInactive
    return branch


def get_employees(branch_id: int):
    """Returns ClientBranch queryset filtered to employees for the given branch."""
    branch = _get_active_branch(branch_id)
    return _profile_qs().filter(branch=branch, is_employee=True)


def get_promotions(branch_id: int):
    """Returns Promotions queryset for the given branch."""
    branch = _get_active_branch(branch_id)
    return Promotions.objects.filter(branch=branch)


def get_transactions(vk_id: int, branch_id: int):
    """Returns CoinTransaction queryset for the given (vk_id, branch_id) profile."""
    profile = get_client_profile(vk_id=vk_id, branch_id=branch_id)
    return CoinTransaction.objects.filter(client=profile)


@transaction.atomic
def upload_story(vk_id: int, branch_id: int) -> tuple[ClientVKStatus, bool]:
    """
    Marks that the guest published a VK story via the mini-app.

    Returns:
        (ClientVKStatus, uploaded: bool)
        uploaded=True  — first upload, status updated.
        uploaded=False — already uploaded before (idempotent).

    Raises:
        ClientNotFound — no profile for (vk_id, branch_id)
    """
    profile = get_client_profile(vk_id=vk_id, branch_id=branch_id)
    vk_status, _ = ClientVKStatus.objects.get_or_create(client=profile)
    uploaded = vk_status.mark_story_uploaded()
    return vk_status, uploaded


# ── Testimonials ──────────────────────────────────────────────────────────────

def _get_or_create_conversation(
    branch: Branch, vk_sender_id: str,
) -> TestimonialConversation:
    """
    Возвращает существующий тред для этого отправителя или создаёт новый.
    Если есть зарегистрированный ClientBranch с таким vk_id — привязывает его.
    """
    from django.utils import timezone

    conv, created = TestimonialConversation.objects.get_or_create(
        branch=branch,
        vk_sender_id=vk_sender_id,
        defaults={'has_unread': True},
    )

    # Попытка привязать ClientBranch если гость уже зарегистрирован.
    # Проверяем при каждом вызове (не только при создании): гость мог
    # зарегистрироваться в приложении позже, чем оставил первый отзыв.
    if not conv.client_id:
        try:
            cb = ClientBranch.objects.filter(
                branch=branch,
                client__vk_id=int(vk_sender_id),
            ).first()
        except (ValueError, TypeError):
            cb = None
        if cb:
            conv.client = cb
            conv.save(update_fields=['client'])

    return conv


def submit_app_review(
    vk_id: int,
    branch_id: int,
    review: str,
    rating: int | None = None,
    phone: str = '',
    table: int | None = None,
) -> TestimonialMessage:
    """
    Создаёт сообщение типа APP в существующем или новом треде.
    Вызывается из POST /api/v1/testimonials/.
    """
    from django.utils import timezone

    branch = Branch.objects.filter(branch_id=branch_id, is_active=True).first()
    if not branch:
        raise BranchNotFound(f'branch_id={branch_id}')

    conv = _get_or_create_conversation(branch, str(vk_id))

    msg = TestimonialMessage.objects.create(
        conversation=conv,
        source=TestimonialMessage.Source.APP,
        text=review,
        rating=rating,
        phone=phone or '',
        table_number=table,
    )

    conv.has_unread = True
    conv.is_replied = False
    conv.last_message_at = timezone.now()
    conv.save(update_fields=['has_unread', 'is_replied', 'last_message_at'])

    from apps.tenant.analytics.ai_service import analyze_and_save
    analyze_and_save(conv.id, review, TestimonialMessage.Source.APP)

    return msg


def handle_vk_incoming_message(
    group_id: int,
    from_id: int,
    message_id: int,
    text: str,
) -> list[TestimonialMessage]:
    """
    Создаёт сообщение типа VK_MESSAGE в тредах всех точек, привязанных к группе.
    Идентификация: SenlerConfig.vk_group_id → Branch (может быть несколько).
    Возвращает пустой список если сообщение уже было обработано или конфиг не найден.
    """
    from django.utils import timezone
    from apps.tenant.senler.models import SenlerConfig

    configs = SenlerConfig.objects.select_related('branch').filter(vk_group_id=group_id)
    if not configs.exists():
        return []

    vk_sender_id = str(from_id)
    vk_msg_id_str = str(message_id)
    created_messages = []

    for config in configs:
        branch = config.branch
        # Deduplication per branch: use branch-scoped vk_message_id
        conv = _get_or_create_conversation(branch, vk_sender_id)

        if TestimonialMessage.objects.filter(
            conversation__branch=branch,
            vk_message_id=vk_msg_id_str,
        ).exists():
            continue

        msg = TestimonialMessage.objects.create(
            conversation=conv,
            source=TestimonialMessage.Source.VK_MESSAGE,
            text=text,
            vk_message_id=vk_msg_id_str,
        )

        conv.has_unread = True
        conv.is_replied = False
        conv.last_message_at = timezone.now()
        conv.save(update_fields=['has_unread', 'is_replied', 'last_message_at'])

        from apps.tenant.analytics.ai_service import analyze_and_save
        analyze_and_save(conv.id, text, TestimonialMessage.Source.VK_MESSAGE)

        created_messages.append(msg)

    return created_messages


def send_vk_reply(
    conversation: TestimonialConversation,
    reply_text: str,
    sender_name: str = 'Администратор',
) -> TestimonialMessage:
    """
    Отправляет сообщение от имени группы в ВКонтакте и сохраняет его в тред.
    Требует SenlerConfig с vk_community_token для этой точки.
    Raises ValueError если нет токена или vk_sender_id.
    """
    import random
    import urllib.request
    import urllib.parse
    import json as _json
    from django.utils import timezone
    from apps.tenant.senler.models import SenlerConfig

    if not conversation.vk_sender_id:
        raise ValueError('Нет VK ID отправителя — не знаем кому ответить')

    try:
        config = SenlerConfig.objects.get(branch=conversation.branch)
    except SenlerConfig.DoesNotExist:
        raise ValueError('SenlerConfig не настроен для этой точки')

    if not config.vk_community_token:
        raise ValueError('VK community token не задан в настройках')

    # VK messages.send
    params = {
        'user_id':    conversation.vk_sender_id,
        'message':    reply_text,
        'random_id':  random.randint(1, 2**31),
        'access_token': config.vk_community_token,
        'v':          '5.131',
    }
    url = 'https://api.vk.com/method/messages.send?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=10) as resp:
        result = _json.loads(resp.read())

    if 'error' in result:
        raise ValueError(f'VK API error: {result["error"].get("error_msg", result["error"])}')

    msg = TestimonialMessage.objects.create(
        conversation=conversation,
        source=TestimonialMessage.Source.ADMIN_REPLY,
        text=reply_text,
    )

    conversation.is_replied = True
    conversation.has_unread = False
    conversation.last_message_at = timezone.now()
    conversation.save(update_fields=['is_replied', 'has_unread', 'last_message_at'])

    return msg


# ── VK membership events ───────────────────────────────────────────────────────

_MEMBERSHIP_EVENTS = frozenset({'group_join', 'group_leave', 'message_allow', 'message_deny'})


def apply_vk_membership_event(
    group_id: int,
    vk_user_id: int,
    event_type: str,
) -> bool:
    """
    Обновляет ClientVKStatus на основе события VK Callback/LongPoll.

    Обрабатывает:
      group_join     → is_community_member=True
      group_leave    → is_community_member=False (сбрасывает joined_at и via_app)
      message_allow  → is_newsletter_subscriber=True
      message_deny   → is_newsletter_subscriber=False (сбрасывает joined_at и via_app)

    Returns True если запись была обновлена, False если гость не найден в системе
    или состояние уже актуально.
    """
    if event_type not in _MEMBERSHIP_EVENTS:
        return False

    from apps.tenant.senler.models import SenlerConfig

    config = SenlerConfig.objects.select_related('branch').filter(vk_group_id=group_id).first()
    if config is None:
        return False

    try:
        cb = ClientBranch.objects.select_related('client').get(
            branch=config.branch,
            client__vk_id=vk_user_id,
        )
    except ClientBranch.DoesNotExist:
        return False  # Гость не в нашей системе — игнорируем

    now = timezone.now()
    vk_status, _ = ClientVKStatus.objects.get_or_create(client=cb)
    update_fields: list[str] = []

    if event_type == 'group_join' and not vk_status.is_community_member:
        vk_status.is_community_member = True
        vk_status.community_joined_at = now
        # via_app не трогаем: если уже mark_subscribed() ставил True — сохраняем
        update_fields += ['is_community_member', 'community_joined_at']

    elif event_type == 'group_leave' and vk_status.is_community_member:
        vk_status.is_community_member = False
        vk_status.community_joined_at = None
        vk_status.community_via_app = None
        update_fields += ['is_community_member', 'community_joined_at', 'community_via_app']

    elif event_type == 'message_allow' and not vk_status.is_newsletter_subscriber:
        vk_status.is_newsletter_subscriber = True
        vk_status.newsletter_joined_at = now
        update_fields += ['is_newsletter_subscriber', 'newsletter_joined_at']

    elif event_type == 'message_deny' and vk_status.is_newsletter_subscriber:
        vk_status.is_newsletter_subscriber = False
        vk_status.newsletter_joined_at = None
        vk_status.newsletter_via_app = None
        update_fields += ['is_newsletter_subscriber', 'newsletter_joined_at', 'newsletter_via_app']

    if update_fields:
        vk_status.save(update_fields=update_fields)
        return True

    return False
