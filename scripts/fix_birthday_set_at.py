"""
Устанавливает birth_date_set_at = сегодня для всех гостей у которых
есть birth_date но нет birth_date_set_at (мигрированные из v4).
Это предотвращает мгновенное получение подарка после установки даты рождения.

Запуск:
    sudo docker compose exec web python scripts/fix_birthday_set_at.py
"""
import django
import os
import sys

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings')
django.setup()

from datetime import date
from django_tenants.utils import get_tenant_model, schema_context
from apps.tenant.branch.models import ClientBranch

TenantModel = get_tenant_model()
tenants = TenantModel.objects.exclude(schema_name='public')

today = date.today()
total = 0

for tenant in tenants:
    with schema_context(tenant.schema_name):
        updated = ClientBranch.objects.filter(
            birth_date__isnull=False,
            birth_date_set_at__isnull=True,
        ).update(birth_date_set_at=today)
        if updated:
            print(f'[{tenant.schema_name}] обновлено {updated} профилей')
        total += updated

print(f'\nГотово. Всего обновлено: {total}')
