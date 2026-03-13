from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import (
    BranchIdRequestSerializer,
    BranchInfoSerializer,
    ClientGetRequestSerializer,
    ClientProfileResponseSerializer,
    ClientRegistrationRequestSerializer,
    ClientUpdateRequestSerializer,
    CoinTransactionSerializer,
    EmployeeSerializer,
    PromotionSerializer,
    TestimonialCreateSerializer,
    VKStoryRequestSerializer,
    VKStoryResponseSerializer,
)
from .services import (
    BranchInactive, BranchNotFound, ClientBlocked, ClientNotFound,
    get_branch_info, get_client_profile,
    get_employees, get_promotions, get_transactions,
    handle_vk_incoming_message, register_or_get_client,
    submit_app_review, update_client_profile, upload_story,
)


class BranchInfoView(APIView):
    """
    GET /api/v1/branches/<branch_id>/

    Returns branch contact info + tenant branding/VK config.
    Called immediately after domain resolution, before guest identification.
    """

    def get(self, request: Request, branch_id: int) -> Response:
        try:
            data = get_branch_info(branch_id, tenant=getattr(request, 'tenant', None))
        except BranchNotFound:
            return Response(
                {'detail': 'Торговая точка не найдена.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except BranchInactive:
            return Response(
                {'detail': 'Торговая точка неактивна.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        return Response(BranchInfoSerializer(data).data)


class ClientView(APIView):
    """
    Единая точка для работы с профилем гостя.

    GET  ?vk_id=&branch_id=  — получить профиль
    POST                      — зарегистрировать или войти + записать визит
    PATCH                     — обновить данные профиля
    """

    def get(self, request: Request) -> Response:
        s = ClientGetRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            profile = get_client_profile(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(ClientProfileResponseSerializer(profile).data)

    def post(self, request: Request) -> Response:
        s = ClientRegistrationRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            profile, created = register_or_get_client(**s.validated_data)
        except BranchNotFound:
            return Response(
                {'detail': 'Торговая точка не найдена.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except BranchInactive:
            return Response(
                {'detail': 'Торговая точка неактивна.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        except ClientBlocked:
            return Response(
                {'detail': 'Аккаунт заблокирован.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        resp_status = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return Response(ClientProfileResponseSerializer(profile).data, status=resp_status)

    def patch(self, request: Request) -> Response:
        s = ClientUpdateRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            profile = update_client_profile(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(ClientProfileResponseSerializer(profile).data)


class EmployeeView(APIView):
    """
    GET /api/v1/employees/?branch_id=
    Returns all ClientBranch records with is_employee=True for the branch.
    """

    def get(self, request: Request) -> Response:
        s = BranchIdRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            employees = get_employees(**s.validated_data)
        except BranchNotFound:
            return Response(
                {'detail': 'Торговая точка не найдена.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except BranchInactive:
            return Response(
                {'detail': 'Торговая точка неактивна.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        return Response(EmployeeSerializer(employees, many=True).data)


class PromotionView(APIView):
    """
    GET /api/v1/promotions/?branch_id=
    Returns all promotions for the branch.
    """

    def get(self, request: Request) -> Response:
        s = BranchIdRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            promotions = get_promotions(**s.validated_data)
        except BranchNotFound:
            return Response(
                {'detail': 'Торговая точка не найдена.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except BranchInactive:
            return Response(
                {'detail': 'Торговая точка неактивна.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        return Response(PromotionSerializer(promotions, many=True).data)


class TransactionsView(APIView):
    """
    GET /api/v1/transactions/?vk_id=&branch_id=
    Returns all coin transactions for the ClientBranch.
    """

    def get(self, request: Request) -> Response:
        s = ClientGetRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            transactions = get_transactions(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(CoinTransactionSerializer(transactions, many=True).data)


class VKStoryView(APIView):
    """
    POST /api/v1/vk/story/

    Called by the mini-app after the guest successfully publishes a VK story.
    Marks is_story_uploaded=True on ClientVKStatus (idempotent).

    Response:
      200 — already uploaded before (no change)
      201 — first upload, status updated
      404 — guest not found
    """

    def post(self, request: Request) -> Response:
        s = VKStoryRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            vk_status, first_upload = upload_story(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        resp_status = status.HTTP_201_CREATED if first_upload else status.HTTP_200_OK
        return Response(
            VKStoryResponseSerializer({
                'is_story_uploaded': vk_status.is_story_uploaded,
                'story_uploaded_at': vk_status.story_uploaded_at,
                'first_upload':      first_upload,
            }).data,
            status=resp_status,
        )


class TestimonialCreateView(APIView):
    """
    POST /api/v1/testimonials/

    Принимает отзыв из мини-приложения ВКонтакте.
    Создаёт или дополняет тред переписки с этим гостем.

    Response 201 — сообщение сохранено.
    """

    def post(self, request: Request) -> Response:
        s = TestimonialCreateSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        d = s.validated_data
        try:
            submit_app_review(
                vk_id=d['vk_id'],
                branch_id=d['branch_id'],
                review=d['review'],
                rating=d.get('rating'),
                phone=d.get('phone', ''),
                table=d.get('table'),
            )
        except BranchNotFound:
            return Response(
                {'detail': 'Торговая точка не найдена или неактивна.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response({'detail': 'Отзыв сохранён.'}, status=status.HTTP_201_CREATED)


class VKCallbackView(APIView):
    """
    POST /api/v1/vk/callback/

    Принимает события Callback API ВКонтакте.

    Обрабатывает:
      — confirmation  → возвращает строку подтверждения из SenlerConfig
      — message_new   → сохраняет входящее сообщение в тред
    """

    authentication_classes = []
    permission_classes      = []

    def post(self, request: Request) -> Response:
        from apps.tenant.senler.models import SenlerConfig

        data     = request.data
        event    = data.get('type')
        group_id = data.get('group_id')
        secret   = data.get('secret', '')

        if not group_id:
            return Response('ok')

        # ── Confirmation handshake ────────────────────────────────────────────
        if event == 'confirmation':
            try:
                config = SenlerConfig.objects.get(vk_group_id=group_id)
                return Response(config.vk_callback_confirmation or 'ok')
            except SenlerConfig.DoesNotExist:
                return Response('ok')

        # ── Secret check ─────────────────────────────────────────────────────
        try:
            config = SenlerConfig.objects.get(vk_group_id=group_id)
        except SenlerConfig.DoesNotExist:
            return Response('ok')

        if config.vk_callback_secret and secret != config.vk_callback_secret:
            return Response(status=status.HTTP_403_FORBIDDEN)

        # ── New incoming message ──────────────────────────────────────────────
        if event == 'message_new':
            msg_obj = data.get('object', {})
            message = msg_obj.get('message', msg_obj)   # VK API 5.103+
            from_id    = message.get('from_id')
            message_id = message.get('id')
            text       = (message.get('text') or '').strip()

            # Skip outgoing messages (from_id < 0 means sent by the community/bot)
            if from_id and from_id > 0 and message_id and text:
                handle_vk_incoming_message(
                    group_id=group_id,
                    from_id=from_id,
                    message_id=message_id,
                    text=text,
                )

        return Response('ok')
