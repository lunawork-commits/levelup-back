# apps.tenant.reputation

Модуль импортирует отзывы с внешних площадок (Яндекс.Карты, 2ГИС) и
даёт админке UI для подготовки ответов. Публикация ответа на площадке
пока ручная — через deep-link, до подключения партнёрского API 2GIS.

## Состав

- `models.py` — `ExternalReview`, `ReputationSyncState`, choices `ReviewSource` / `ReviewStatus`
- `sources/` — парсеры: `base.py` (ABC), `yandex.py`, `gis.py`, `utils.py` (сессия с ретраями, UA-ротация)
- `tasks.py` — Celery: `fetch_external_reviews_task` (fan-out по тенантам) и `fetch_reviews_for_branch_task` (воркер на одну пару branch × source)
- `management/commands/fetch_reviews.py` — ручной запуск; использовать через `tenant_command`
- `ai_service.py` — `suggest_reply(...)` — Claude Haiku 4.5 генерирует черновик
- `api/` — DRF-вьюхи + URLы (см. `API.md` → раздел *Reputation*)

## Настройки

```bash
REPUTATION_FETCH_ENABLED=True       # глобальный kill-switch, default False
GIS_PUBLIC_KEY=<partner-key>        # ключ public-api.reviews.2gis.com
ANTHROPIC_API_KEY=<...>             # нужен для generate-reply
```

## Как включить для филиала

1. Заполнить `BranchConfig.yandex_map` и/или `BranchConfig.gis_map` (URL точек на картах).
2. Поставить `BranchConfig.reputation_enabled = True`.
3. Включить feature-flag на платформе: `REPUTATION_FETCH_ENABLED=True`.
4. Ежедневная beat-задача запускается в 04:00 (см. `main/celery.py`).
5. Для форс-ручной синхронизации — POST `/api/v1/reputation/sync/` или
   `python manage.py tenant_command fetch_reviews --schema=<tenant> --branch=<id> --source=yandex`.

## Архитектурные заметки

- Уникальность обеспечивает `UniqueConstraint(source, external_id)` — повторный fetch той же страницы не создаёт дубликатов.
- Fan-out целенаправленно: одна задача на (branch, source), чтобы не упереться в `CELERY_TASK_TIME_LIMIT=300s` на крупных тенантах.
- `ReputationSyncState` копит ошибки последнего запуска — одна битая точка не валит остальные.
- `schema_name` при диспатче сохраняется в аргументах задачи, воркер заходит в нужную схему через `schema_context(...)`.

## Будущая работа

- Подключить partner.api.2gis для автоматической публикации ответа.
- Добавить источник Google Maps (когда появится доступ у платформы).
- Хранить историю пересмотров ответа (сейчас перезаписывается одно поле `reply_text`).
