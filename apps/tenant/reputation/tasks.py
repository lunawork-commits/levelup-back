"""
Celery tasks для синхронизации внешних отзывов.

Schedule (определяется в main/celery.py beat_schedule):
  — fetch_external_reviews_task          — ежедневно 04:00, fan-out по тенантам/филиалам/источникам
  — fetch_reviews_for_branch_task        — одна пара (branch, source); ставится в очередь из fan-out
"""
from __future__ import annotations

import logging
import random
import time
from typing import Type

from celery import shared_task
from django.conf import settings
from django.utils import timezone
from django_tenants.utils import get_tenant_model, schema_context

from .sources import BaseReviewSource, GisSource, YandexSource

logger = logging.getLogger(__name__)

SOURCE_REGISTRY: dict[str, Type[BaseReviewSource]] = {
    YandexSource.key: YandexSource,
    GisSource.key:    GisSource,
}


# ── Fan-out ──────────────────────────────────────────────────────────────────

@shared_task(name='apps.tenant.reputation.tasks.fetch_external_reviews_task')
def fetch_external_reviews_task() -> dict:
    """
    Перебирает тенантов, внутри каждого — филиалы с reputation_enabled=True,
    и диспатчит под-задачу на каждую пару (branch, source).

    Глобальный kill-switch: REPUTATION_FETCH_ENABLED. Если False —
    выходим не запуская ни одного запроса.
    """
    if not getattr(settings, 'REPUTATION_FETCH_ENABLED', False):
        logger.info('fetch_external_reviews: REPUTATION_FETCH_ENABLED is off — skip')
        return {'dispatched': 0, 'skipped': 'kill_switch'}

    TenantModel = get_tenant_model()
    dispatched = 0

    for tenant in TenantModel.objects.exclude(schema_name='public'):
        with schema_context(tenant.schema_name):
            from apps.tenant.branch.models import Branch

            branches = (
                Branch.objects
                .filter(is_active=True, config__reputation_enabled=True)
                .select_related('config')
            )
            for branch in branches:
                for source_key, source_cls in SOURCE_REGISTRY.items():
                    map_url = _map_url_for(branch.config, source_key)
                    if not source_cls.extract_external_id(map_url):
                        continue  # URL пустой или формат не распознан
                    fetch_reviews_for_branch_task.delay(
                        branch_id=branch.id,
                        source=source_key,
                        schema_name=tenant.schema_name,
                    )
                    dispatched += 1

    logger.info('fetch_external_reviews: dispatched %d tasks', dispatched)
    return {'dispatched': dispatched}


# ── Per-branch worker ────────────────────────────────────────────────────────

@shared_task(
    name='apps.tenant.reputation.tasks.fetch_reviews_for_branch_task',
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def fetch_reviews_for_branch_task(
    self,
    *,
    branch_id: int,
    source: str,
    schema_name: str,
) -> dict:
    """
    Обрабатывает один филиал + один источник. Импорты внутри функции —
    чтобы задача могла быть сериализована до инициализации Django apps.
    """
    from apps.tenant.branch.models import Branch
    from apps.tenant.reputation.models import ExternalReview, ReputationSyncState

    # Защита от DoS площадок: небольшой случайный jitter между параллельными воркерами
    time.sleep(random.uniform(1.0, 3.0))

    source_cls = SOURCE_REGISTRY.get(source)
    if source_cls is None:
        logger.warning('fetch_reviews_for_branch: unknown source=%s', source)
        return {'ok': False, 'reason': 'unknown_source'}

    with schema_context(schema_name):
        try:
            branch = Branch.objects.select_related('config').get(pk=branch_id)
        except Branch.DoesNotExist:
            return {'ok': False, 'reason': 'branch_missing'}

        map_url = _map_url_for(branch.config, source)
        external_id = source_cls.extract_external_id(map_url)
        if not external_id:
            return {'ok': False, 'reason': 'no_external_id'}

        state, _ = ReputationSyncState.objects.get_or_create(branch=branch, source=source)
        state.last_run_at = timezone.now()

        try:
            created = 0
            for fetched in source_cls().fetch(external_id):
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

            state.last_ok_at = timezone.now()
            state.last_error = ''
            state.reviews_fetched = (state.reviews_fetched or 0) + created
            state.save(update_fields=['last_run_at', 'last_ok_at', 'last_error', 'reviews_fetched', 'updated_at'])
            return {'ok': True, 'created': created, 'branch_id': branch_id, 'source': source}

        except Exception as exc:
            logger.exception(
                'fetch_reviews_for_branch failed branch=%s source=%s schema=%s',
                branch_id, source, schema_name,
            )
            state.last_error = f'{type(exc).__name__}: {exc}'[:4000]
            state.save(update_fields=['last_run_at', 'last_error', 'updated_at'])
            # Не ретраим — упали именно на парсинге. Админ увидит last_error
            # в дашборде и решит вручную. Следующая синхронизация завтра.
            return {'ok': False, 'error': state.last_error}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _map_url_for(config, source: str) -> str:
    if source == YandexSource.key:
        return config.yandex_map or ''
    if source == GisSource.key:
        return config.gis_map or ''
    return ''
