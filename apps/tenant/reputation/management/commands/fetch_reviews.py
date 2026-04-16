"""
Менеджмент-команда для синхронизации отзывов конкретного филиала.

Пример:
    python manage.py tenant_command fetch_reviews --schema=pilot1 --branch=42 --source=yandex --dry-run
    python manage.py tenant_command fetch_reviews --schema=pilot1 --branch=42 --source=gis

`--schema` — стандартный флаг django-tenants; команда запускается внутри
указанной тенант-схемы. Внутри команды мы уже в нужной schema, поэтому
schema_context не оборачиваем.
"""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.tenant.reputation.sources import GisSource, YandexSource

_SOURCE_MAP = {
    YandexSource.key: YandexSource,
    GisSource.key:    GisSource,
}


class Command(BaseCommand):
    help = 'Скачать отзывы с внешней площадки для одного филиала (для тестов / бэкфила).'

    def add_arguments(self, parser):
        parser.add_argument('--branch', type=int, required=True, help='Branch.id')
        parser.add_argument(
            '--source',
            choices=list(_SOURCE_MAP.keys()),
            required=True,
            help='yandex | gis',
        )
        parser.add_argument(
            '--limit', type=int, default=50,
            help='Максимум отзывов за один запуск (по умолчанию 50).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Печатать распарсенные отзывы, но ничего не писать в БД.',
        )

    def handle(self, *args, **opts):
        from apps.tenant.branch.models import Branch
        from apps.tenant.reputation.models import ExternalReview, ReputationSyncState

        branch_id: int = opts['branch']
        source: str   = opts['source']
        limit: int    = opts['limit']
        dry_run: bool = opts['dry_run']

        source_cls = _SOURCE_MAP[source]

        try:
            branch = Branch.objects.select_related('config').get(pk=branch_id)
        except Branch.DoesNotExist:
            raise CommandError(f'Branch id={branch_id} не найден в текущей схеме.')

        map_url = branch.config.yandex_map if source == YandexSource.key else branch.config.gis_map
        external_id = source_cls.extract_external_id(map_url or '')
        if not external_id:
            raise CommandError(
                f'Не могу извлечь external_id из URL карты для {source}: "{map_url!r}". '
                f'Проверь BranchConfig.{"yandex_map" if source == YandexSource.key else "gis_map"}.'
            )

        self.stdout.write(f'→ source={source} external_id={external_id} branch={branch.name}')

        created = skipped = 0
        for fetched in source_cls().fetch(external_id, limit=limit):
            if dry_run:
                self.stdout.write(self.style.WARNING(
                    f'  [dry] {fetched.rating or "—"}★ {fetched.author_name or "?"}: '
                    f'{(fetched.text or "")[:80]}'
                ))
                continue

            _, is_new = ExternalReview.objects.get_or_create(
                source=source,
                external_id=fetched.external_id,
                defaults={
                    'branch':       branch,
                    'author_name':  fetched.author_name,
                    'rating':       fetched.rating,
                    'text':         fetched.text,
                    'published_at': fetched.published_at,
                    'raw':          fetched.raw,
                },
            )
            if is_new:
                created += 1
                self.stdout.write(self.style.SUCCESS(
                    f'  + {fetched.rating or "—"}★ {fetched.author_name or "?"} ({fetched.external_id})'
                ))
            else:
                skipped += 1

        if not dry_run:
            state, _ = ReputationSyncState.objects.get_or_create(branch=branch, source=source)
            state.last_run_at = timezone.now()
            state.last_ok_at = timezone.now()
            state.last_error = ''
            state.reviews_fetched = (state.reviews_fetched or 0) + created
            state.save(update_fields=['last_run_at', 'last_ok_at', 'last_error', 'reviews_fetched', 'updated_at'])

        self.stdout.write(self.style.SUCCESS(
            f'Готово: создано {created}, пропущено дублей {skipped} (dry-run={dry_run})'
        ))
