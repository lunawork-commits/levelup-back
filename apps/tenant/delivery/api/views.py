from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import (
    CodeActivationRequestSerializer,
    DeliverySerializer,
    WebhookRequestSerializer,
)
from .services import (
    BranchNotFound, ClientNotFound, DeliveryNotFound,
    activate_delivery, register_delivery, verify_webhook_signature,
)


class DeliveryWebhook(APIView):
    """
    POST /api/v1/webhook/delivery/

    Receives a new delivery order from a POS system (iiko / Dooglys).
    Secured with DELIVERY_WEBHOOK_SECRET (X-Webhook-Secret header).
    Returns 201 when created, 200 when the code already exists (idempotent).
    """

    def post(self, request: Request) -> Response:
        if not verify_webhook_signature(request):
            return Response(
                {'detail': 'Неверная подпись запроса.'},
                status=status.HTTP_403_FORBIDDEN,
            )

        s = WebhookRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        try:
            delivery, created = register_delivery(**s.validated_data)
        except BranchNotFound:
            return Response(
                {'detail': 'Торговая точка не найдена.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        resp_status = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return Response(DeliverySerializer(delivery).data, status=resp_status)


class DeliveryCodeView(APIView):
    """
    POST /api/v1/code/

    Guest enters the 5-digit short code to activate their delivery.
    Idempotent: re-submitting the same code by the same client returns 200.
    """

    def post(self, request: Request) -> Response:
        s = CodeActivationRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)

        try:
            delivery = activate_delivery(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except DeliveryNotFound:
            return Response(
                {'detail': 'Код не найден или срок его действия истёк.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(DeliverySerializer(delivery).data)
