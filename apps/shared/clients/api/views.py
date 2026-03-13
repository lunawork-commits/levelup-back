from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status

from .serializers import TenantDomainResponseSerializer
from .services import CompanyExpired, CompanyInactive, CompanyNotFound, get_tenant_domain


class TenantDomainView(APIView):
    """
    GET /api/company/<client_id>/

    Возвращает домен тенанта по публичному ID компании.
    Используется при первом открытии приложения гостем.
    """

    def get(self, request: Request, client_id: int) -> Response:
        try:
            data = get_tenant_domain(client_id)
        except CompanyNotFound:
            return Response(
                {'detail': 'Компания не найдена.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        except CompanyInactive:
            return Response(
                {'detail': 'Компания неактивна.'},
                status=status.HTTP_403_FORBIDDEN,
            )
        except CompanyExpired:
            return Response(
                {'detail': 'Срок подписки компании истёк.'},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = TenantDomainResponseSerializer(data)
        return Response(serializer.data)
