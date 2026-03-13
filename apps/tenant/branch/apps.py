from django.apps import AppConfig


class BranchAppConfig(AppConfig):
    name = 'apps.tenant.branch'
    verbose_name = 'Точки'

    def ready(self):
        import apps.tenant.branch.signals  # noqa: F401
