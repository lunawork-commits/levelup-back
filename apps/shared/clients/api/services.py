from django.utils import timezone

from apps.shared.clients.models import Company, Domain


class CompanyNotFound(Exception):
    pass


class CompanyInactive(Exception):
    pass


class CompanyExpired(Exception):
    pass


def get_tenant_domain(client_id: int) -> dict:
    """
    По client_id компании возвращает словарь с доменом и названием.

    Raises:
        CompanyNotFound  — компания с таким client_id не найдена
        CompanyInactive  — компания деактивирована
        CompanyExpired   — подписка истекла
    """
    try:
        company = Company.objects.get(client_id=client_id)
    except Company.DoesNotExist:
        raise CompanyNotFound

    if not company.is_active:
        raise CompanyInactive

    if company.paid_until < timezone.localdate():
        raise CompanyExpired

    domain = (
        Domain.objects
        .filter(tenant=company, is_primary=True)
        .first()
        or Domain.objects.filter(tenant=company).first()
    )

    return {
        'domain': domain.domain if domain else None,
        'name': company.name,
    }
