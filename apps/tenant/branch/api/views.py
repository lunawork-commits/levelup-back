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
    VKAuthRequestSerializer,
    VKStoryRequestSerializer,
    VKStoryResponseSerializer,
)
from .services import (
    BranchInactive, BranchNotFound, ClientBlocked, ClientNotFound, VKAuthError,
    VKCallbackConfirmation, VKCallbackForbidden,
    get_branch_info, get_client_profile,
    get_employees, get_promotions, get_transactions,
    handle_vk_callback, register_or_get_client,
    submit_app_review, update_client_profile, upload_story, vk_web_auth,
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
        for key in ('logotype_url', 'coin_icon_url', 'story_image_url'):
            if data.get(key):
                data[key] = request.build_absolute_uri(data[key])
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
        return Response(PromotionSerializer(promotions, many=True, context={'request': request}).data)


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
        try:
            submit_app_review(**s.validated_data)
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
        from django.http import HttpResponse
        try:
            handle_vk_callback(request.data)
        except VKCallbackConfirmation as e:
            return HttpResponse(e.code, content_type='text/plain')
        except VKCallbackForbidden:
            return Response(status=status.HTTP_403_FORBIDDEN)
        return Response('ok')


class VKAuthView(APIView):
    """
    POST /api/v1/vk/auth/

    VK ID OAuth2 PKCE — точка входа для веб-приложения.

    Фронтенд инициирует PKCE flow (генерирует code_verifier / code_challenge,
    редиректит пользователя на id.vk.ru). После авторизации VK возвращает
    code + device_id в redirect_uri. Фронт отправляет их сюда.

    Бэкенд:
      1. Обменивает code на access_token server-to-server (id.vk.ru/oauth2/auth)
      2. Получает профиль пользователя (id.vk.ru/oauth2/user_info)
      3. Регистрирует / логинит гостя
      4. Возвращает ClientProfile

    Responses:
      200 — гость уже зарегистрирован
      201 — новая регистрация
      400 — невалидный/просроченный code или ошибка VK API
      403 — аккаунт заблокирован или точка неактивна
      404 — торговая точка не найдена
    """
    authentication_classes = []
    permission_classes = []

    def post(self, request: Request) -> Response:
        s = VKAuthRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            profile, created = vk_web_auth(**s.validated_data)
        except VKAuthError as e:
            return Response({'detail': str(e)}, status=status.HTTP_400_BAD_REQUEST)
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
