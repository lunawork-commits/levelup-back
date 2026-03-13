"""
Снимает флаг is_employee со всех ClientBranch во всех тенантах.
Запуск:
    sudo docker compose exec web python scripts/clear_employees.py
"""
import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from django_tenants.utils import get_tenant_model, schema_context
from apps.tenant.branch.models import ClientBranch

TenantModel = get_tenant_model()
tenants = TenantModel.objects.exclude(schema_name='public')

total = 0
for tenant in tenants:
    with schema_context(tenant.schema_name):
        updated = ClientBranch.objects.filter(is_employee=True).update(is_employee=False)
        if updated:
            print(f'[{tenant.schema_name}] снято {updated} сотрудников')
        total += updated

print(f'\nГотово. Всего обновлено: {total}')
