from django.apps import AppConfig


class ConfigConfig(AppConfig):
    name = 'apps.shared.config'
    verbose_name = 'Конфигурация'

    def ready(self):
        import apps.shared.config.signals  # noqa: F401
