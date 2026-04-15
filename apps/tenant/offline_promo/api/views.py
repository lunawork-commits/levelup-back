"""
Offline promo API views — «Новые клиенты» mechanic.

All endpoints are isolated from the main game flow.
Users from offline ads do NOT enter the main analytics pipeline.
"""
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from drf_spectacular.utils import extend_schema
from drf_spectacular.types import OpenApiTypes

from .serializers import (
    OfflinePromoStartSerializer,
    OfflinePromoClaimSerializer,
    OfflinePromoCheckSerializer,
    OfflinePromoSessionSerializer,
    OfflineGiftSelectSerializer,
    OfflineGiftActivateSerializer,
    OfflineGiftStatusSerializer,
    NewClientsStatsQuerySerializer,
)
from .services import (
    AlreadyPlayed,
    ClientNotFound,
    GiftNotFound,
    GiftNotUsable,
    InvalidToken,
    PromoDisabled,
    activate_gift_in_cafe,
    check_already_played,
    claim_offline_gift,
    get_gift_status,
    get_new_clients_detail,
    get_new_clients_stats,
    get_promo_sources,
    select_gift_product,
    start_offline_game,
)


class OfflinePromoCheckView(APIView):
    """
    GET /api/v1/offline-promo/check/?vk_id=&branch_id=

    Проверяет, участвовал ли гость уже в офлайн-промо.
    Вызывается фронтендом ДО нажатия «ПУСК».
    """

    @extend_schema(
        parameters=[OfflinePromoCheckSerializer],
        responses={200: OpenApiTypes.OBJECT, 404: OpenApiTypes.OBJECT},
    )
    def get(self, request: Request) -> Response:
        s = OfflinePromoCheckSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)

        try:
            result = check_already_played(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(result)


class OfflinePromoStartView(APIView):
    """
    POST /api/v1/offline-promo/start/

    Phase 1: Записывает скан, проверяет уникальность участия,
    возвращает сессионный токен и параметры анимации.

    При повторной попытке возвращает 409 с данными заглушки.
    """

    @extend_schema(
        request=OfflinePromoStartSerializer,
        responses={
            200: OfflinePromoSessionSerializer,
            404: OpenApiTypes.OBJECT,
            409: OpenApiTypes.OBJECT,
            403: OpenApiTypes.OBJECT,
        },
    )
    def post(self, request: Request) -> Response:
        s = OfflinePromoStartSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        # Получаем IP для аналитики
        ip = request.META.get('HTTP_X_FORWARDED_FOR', '').split(',')[0].strip()
        if not ip:
            ip = request.META.get('REMOTE_ADDR')

        try:
            result = start_offline_game(
                ip_address=ip,
                **s.validated_data,
            )
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except AlreadyPlayed as e:
            return Response(
                {
                    'detail': 'already_played',
                    'message': (
                        'Вы уже участвовали в этой акции 🎁\n'
                        'Ваш подарок уже был начислен ранее и доступен '
                        'в разделе «Мои подарки» в течение 10 дней.\n'
                        'Забрать его можно в кафе.'
                    ),
                    'gift': {
                        'id': e.gift.pk,
                        'status': e.gift.status,
                        'days_remaining': e.gift.days_remaining,
                        'product_name': e.gift.product.name if e.gift.product else None,
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )
        except PromoDisabled:
            return Response(
                {'detail': 'Акция временно недоступна.'},
                status=status.HTTP_403_FORBIDDEN,
            )

        return Response(OfflinePromoSessionSerializer(result).data)


class OfflinePromoClaimView(APIView):
    """
    POST /api/v1/offline-promo/claim/

    Phase 2: Создаёт подарок за гостем после анимации.
    Токен действует 10 минут, replay-safe.
    """

    @extend_schema(
        request=OfflinePromoClaimSerializer,
        responses={200: OpenApiTypes.OBJECT, 400: OpenApiTypes.OBJECT},
    )
    def post(self, request: Request) -> Response:
        s = OfflinePromoClaimSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        try:
            result = claim_offline_gift(**s.validated_data)
        except InvalidToken:
            return Response(
                {'detail': 'Сессия игры недействительна или истекла.'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        gift = result['gift']
        return Response({
            'type': 'offline_gift',
            'gift': {
                'id': gift.pk,
                'product_name': gift.product.name if gift.product else None,
                'status': gift.status,
                'days_remaining': gift.days_remaining,
                'expires_at': gift.expires_at.isoformat(),
            },
            'congratulation_text': result['congratulation_text'],
        })


class OfflineGiftSelectView(APIView):
    """
    POST /api/v1/offline-promo/gift/select/

    Гость выбирает конкретный продукт из каталога для своего подарка.
    """

    @extend_schema(
        request=OfflineGiftSelectSerializer,
        responses={200: OpenApiTypes.OBJECT, 400: OpenApiTypes.OBJECT, 404: OpenApiTypes.OBJECT},
    )
    def post(self, request: Request) -> Response:
        s = OfflineGiftSelectSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        try:
            gift = select_gift_product(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except GiftNotFound:
            return Response(
                {'detail': 'Подарок не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except GiftNotUsable as e:
            return Response(
                {'detail': f'Подарок недоступен (статус: {e.status}).'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response({
            'id': gift.pk,
            'product_name': gift.product.name if gift.product else None,
            'status': gift.status,
        })


class OfflineGiftActivateView(APIView):
    """
    POST /api/v1/offline-promo/gift/activate/

    Активация подарка в кафе (подтверждение персоналом).
    Это ключевой момент конверсии из офлайн-рекламы в визит.
    """

    @extend_schema(
        request=OfflineGiftActivateSerializer,
        responses={200: OpenApiTypes.OBJECT, 400: OpenApiTypes.OBJECT, 404: OpenApiTypes.OBJECT},
    )
    def post(self, request: Request) -> Response:
        s = OfflineGiftActivateSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        try:
            gift = activate_gift_in_cafe(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except GiftNotFound:
            return Response(
                {'detail': 'Подарок не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except GiftNotUsable as e:
            return Response(
                {'detail': f'Подарок недоступен (статус: {e.status}).'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response({
            'id': gift.pk,
            'status': gift.status,
            'activated_at': gift.activated_at.isoformat(),
            'activated_branch': gift.activated_branch.name if gift.activated_branch else None,
        })


class OfflineGiftStatusView(APIView):
    """
    GET /api/v1/offline-promo/gift/status/?vk_id=&branch_id=

    Статус подарка гостя.
    """

    @extend_schema(
        parameters=[OfflineGiftStatusSerializer],
        responses={200: OpenApiTypes.OBJECT, 404: OpenApiTypes.OBJECT},
    )
    def get(self, request: Request) -> Response:
        s = OfflineGiftStatusSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)

        try:
            result = get_gift_status(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        if result is None:
            return Response(
                {'detail': 'Подарок не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(result)


# ── Analytics views ──────────────────────────────────────────────────────────

class NewClientsStatsView(APIView):
    """
    GET /api/v1/analytics/new-clients/

    Дашборд «Новые клиенты» — метрики по пользователям
    из офлайн-рекламы.

    Query params:
      branch_ids — comma-separated Branch PKs
      period     — today | 7d | 30d | 90d | year | all
      start      — YYYY-MM-DD
      end        — YYYY-MM-DD
      source     — фильтр по рекламному источнику
    """

    @extend_schema(
        parameters=[NewClientsStatsQuerySerializer],
        responses={200: OpenApiTypes.OBJECT},
    )
    def get(self, request: Request) -> Response:
        ser = NewClientsStatsQuerySerializer(data=request.query_params)
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)

        branch_ids = ser.validated_data['branch_ids'] or None
        start_date = ser.validated_data['start']
        end_date   = ser.validated_data['end']
        source     = ser.validated_data.get('source', '') or None

        stats = get_new_clients_stats(branch_ids, start_date, end_date, source)

        return Response({
            'stats': stats,
            'meta': {
                'start':      str(start_date),
                'end':        str(end_date),
                'branch_ids': branch_ids or [],
                'source':     source or '',
            },
        })


class NewClientsDetailView(APIView):
    """
    GET /api/v1/analytics/new-clients/detail/

    Детальный список пользователей из офлайн-рекламы
    с датами сканирования, получения и активации подарков.
    """

    @extend_schema(
        parameters=[NewClientsStatsQuerySerializer],
        responses={200: OpenApiTypes.OBJECT},
    )
    def get(self, request: Request) -> Response:
        ser = NewClientsStatsQuerySerializer(data=request.query_params)
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)

        branch_ids = ser.validated_data['branch_ids'] or None
        start_date = ser.validated_data['start']
        end_date   = ser.validated_data['end']
        source     = ser.validated_data.get('source', '') or None

        detail = get_new_clients_detail(branch_ids, start_date, end_date, source)

        return Response({
            'users': detail,
            'total': len(detail),
        })


class NewClientsSourcesView(APIView):
    """
    GET /api/v1/analytics/new-clients/sources/

    Список рекламных источников с количеством сканирований.
    Для фильтра в дашборде.
    """

    @extend_schema(
        parameters=[NewClientsStatsQuerySerializer],
        responses={200: OpenApiTypes.OBJECT},
    )
    def get(self, request: Request) -> Response:
        branch_ids_raw = request.query_params.get('branch_ids', '')
        branch_ids = None
        if branch_ids_raw:
            try:
                branch_ids = [int(x.strip()) for x in branch_ids_raw.split(',') if x.strip()]
            except ValueError:
                pass

        sources = get_promo_sources(branch_ids)
        return Response({'sources': sources})
