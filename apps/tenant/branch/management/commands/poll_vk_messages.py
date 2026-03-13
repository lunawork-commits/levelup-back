"""
Management command — manual VK polling (useful without Celery or for testing).

Usage:
    # Poll all active tenants + branches:
    python manage.py poll_vk_messages

    # Poll specific tenant schema:
    python manage.py poll_vk_messages --schema levone

    # Poll specific branch inside a schema:
    python manage.py poll_vk_messages --schema levone --branch 1

    # Continuous loop (like a Celery Beat replacement):
    python manage.py poll_vk_messages --loop --interval 30
"""
import time

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Poll VK group messages and save to testimonials'

    def add_arguments(self, parser):
        parser.add_argument('--schema',   type=str, help='Tenant schema name (default: all)')
        parser.add_argument('--branch',   type=int, help='Branch ID (default: all in schema)')
        parser.add_argument('--loop',     action='store_true', help='Run in a loop')
        parser.add_argument('--interval', type=int, default=30, help='Loop interval in seconds')

    def handle(self, *args, **options):
        from django_tenants.utils import get_tenant_model, schema_context
        from apps.tenant.branch.tasks import poll_branch_messages

        schema_filter = options.get('schema')
        branch_filter = options.get('branch')
        loop          = options['loop']
        interval      = options['interval']

        def run_once():
            TenantModel = get_tenant_model()
            qs = TenantModel.objects.exclude(schema_name='public')
            if schema_filter:
                qs = qs.filter(schema_name=schema_filter)

            total_new = 0
            for tenant in qs:
                with schema_context(tenant.schema_name):
                    from apps.tenant.senler.models import SenlerConfig
                    configs = SenlerConfig.objects.filter(is_active=True).select_related('branch')
                    if branch_filter:
                        configs = configs.filter(branch_id=branch_filter)

                    for cfg in configs:
                        result = poll_branch_messages(cfg.branch_id)
                        total_new += result['new_messages']
                        for err in result['errors']:
                            self.stderr.write(
                                self.style.WARNING(
                                    f'  [{tenant.schema_name}/branch={cfg.branch_id}] {err}'
                                )
                            )
                        if result['new_messages']:
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f'  [{tenant.schema_name}/branch={cfg.branch_id}] '
                                    f'+{result["new_messages"]} new messages'
                                )
                            )
            return total_new

        if loop:
            self.stdout.write(f'Starting VK polling loop every {interval}s (Ctrl+C to stop)…')
            while True:
                try:
                    n = run_once()
                    if n:
                        self.stdout.write(self.style.SUCCESS(f'Total new: {n}'))
                    time.sleep(interval)
                except KeyboardInterrupt:
                    self.stdout.write('\nStopped.')
                    break
        else:
            n = run_once()
            self.stdout.write(self.style.SUCCESS(f'Done. Total new messages: {n}'))
