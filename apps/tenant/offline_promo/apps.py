from django.apps import AppConfig


class OfflinePromoConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.tenant.offline_promo'
    verbose_name = 'Новые клиенты (офлайн-реклама)'
