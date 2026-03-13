from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import (
    BuyRequestSerializer,
    BuyResponseSerializer,
    CatalogRequestSerializer,
    CooldownRequestSerializer,
    CooldownResponseSerializer,
    ProductSerializer,
)
from .services import (
    BranchInactive, BranchNotFound, ClientNotFound,
    InsufficientBalance, ProductNotFound, ShopOnCooldown,
    activate_shop_cooldown, buy_product,
    get_active_products, get_shop_cooldown,
)


class CatalogView(APIView):
    """
    GET /api/v1/catalog/?branch_id=
    Returns all active products for the branch, ordered by category/product ordering.
    """

    def get(self, request: Request) -> Response:
        s = CatalogRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            products = get_active_products(**s.validated_data)
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
        return Response(ProductSerializer(products, many=True).data)


class CooldownView(APIView):
    """
    GET  /api/v1/catalog/cooldown/?vk_id=&branch_id= — SHOP cooldown status
    POST /api/v1/catalog/cooldown/                   — activate SHOP cooldown
    """

    def get(self, request: Request) -> Response:
        s = CooldownRequestSerializer(data=request.query_params)
        s.is_valid(raise_exception=True)
        try:
            cooldown = get_shop_cooldown(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        if cooldown is None:
            return Response({'is_active': False, 'expires_at': None, 'seconds_remaining': 0})
        return Response(CooldownResponseSerializer(cooldown).data)

    def post(self, request: Request) -> Response:
        s = CooldownRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            cooldown = activate_shop_cooldown(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(CooldownResponseSerializer(cooldown).data)


class BuyView(APIView):
    """
    POST /api/v1/catalog/buy/
    Purchases a product: deducts coins, creates InventoryItem, activates SHOP cooldown.
    """

    def post(self, request: Request) -> Response:
        s = BuyRequestSerializer(data=request.data)
        s.is_valid(raise_exception=True)
        try:
            item = buy_product(**s.validated_data)
        except ClientNotFound:
            return Response(
                {'detail': 'Профиль гостя не найден.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except ProductNotFound:
            return Response(
                {'detail': 'Товар не найден или недоступен.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except ShopOnCooldown:
            return Response(
                {'detail': 'Магазин на перезарядке. Попробуйте позже.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        except InsufficientBalance:
            return Response(
                {'detail': 'Недостаточно монет для покупки.'},
                status=status.HTTP_400_BAD_REQUEST,
            )
        return Response(BuyResponseSerializer(item).data, status=status.HTTP_201_CREATED)
