from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import (
    QuestActivateSerializer,
    QuestCooldownSerializer,
    QuestListItemSerializer,
    QuestRequestSerializer,
    QuestSubmitInputSerializer,
    QuestSubmitSerializer,
)
from .services import (
    ClientNotFound, InvalidCode,
    QuestAlreadyActivated, QuestAlreadyCompleted,
    QuestCooldownActive, QuestExpired,
    QuestNotFound, QuestSubmitNotFound,
    activate_quest, activate_quest_cooldown,
    get_active_quest, get_quest_cooldown, get_quests,
    submit_quest,
)


class QuestListView(APIView):
    """
    GET /api/v1/quest/?vk_id=&branch_id=
    Returns all active quests with a per-guest completion flag.
    """

    def get(self, request: Request) -> Response:
        s = QuestRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            items = get_quests(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(QuestListItemSerializer(items, many=True).data)


class QuestActiveView(APIView):
    """
    GET /api/v1/quest/active/?vk_id=&branch_id=
    Returns the guest's current pending quest submit, or {} if none is active.
    """

    def get(self, request: Request) -> Response:
        s = QuestRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            submit = get_active_quest(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        if submit is None:
            return Response({})
        return Response(QuestSubmitSerializer(submit).data)


class QuestActivateView(APIView):
    """
    POST /api/v1/quest/activate/
    Starts a quest for the guest.  Idempotent for already-pending submits.
    """

    def post(self, request: Request) -> Response:
        s = QuestActivateSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            submit = activate_quest(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except QuestNotFound:
            return Response(
                {'detail': 'Квест не найден или недоступен.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except QuestCooldownActive as e:
            remaining = e.cooldown.remaining
            return Response(
                {
                    'detail': 'Квесты на перезарядке.',
                    'expires_at': e.cooldown.expires_at,
                    'seconds_remaining': int(remaining.total_seconds()) if remaining else 0,
                },
                status=status.HTTP_409_CONFLICT,
            )
        except QuestAlreadyActivated as e:
            return Response(QuestSubmitSerializer(e.submit).data)
        except QuestAlreadyCompleted:
            return Response(
                {'detail': 'Квест уже выполнен.'},
                status=status.HTTP_409_CONFLICT,
            )
        except QuestExpired:
            return Response(
                {'detail': 'Время на выполнение квеста истекло.'},
                status=status.HTTP_409_CONFLICT,
            )
        return Response(QuestSubmitSerializer(submit).data, status=status.HTTP_201_CREATED)


class QuestSubmitView(APIView):
    """
    POST /api/v1/quest/submit/
    Completes a pending quest with today's daily QUEST code.
    """

    def post(self, request: Request) -> Response:
        s = QuestSubmitInputSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            submit = submit_quest(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except InvalidCode:
            return Response(
                {'detail': 'Неверный код дня.'},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except QuestSubmitNotFound:
            return Response(
                {'detail': 'Активный квест не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except QuestExpired:
            return Response(
                {'detail': 'Время на выполнение квеста истекло.'},
                status=status.HTTP_409_CONFLICT,
            )
        return Response(QuestSubmitSerializer(submit).data)


class QuestCooldownView(APIView):
    """
    GET  /api/v1/quest/cooldown/?vk_id=&branch_id= — current QUEST cooldown status
    POST /api/v1/quest/cooldown/ — manually activate cooldown (admin / debug)
    """

    def get(self, request: Request) -> Response:
        s = QuestRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            cooldown = get_quest_cooldown(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        if cooldown is None:
            return Response({'is_active': False, 'expires_at': None, 'seconds_remaining': 0})
        return Response(QuestCooldownSerializer(cooldown).data)

    def post(self, request: Request) -> Response:
        s = QuestRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            cooldown = activate_quest_cooldown(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(QuestCooldownSerializer(cooldown).data)
