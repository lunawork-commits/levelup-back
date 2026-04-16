import logging
import re
from datetime import datetime, timezone
from typing import Iterable

from django.conf import settings

from .base import BaseReviewSource, FetchedReview
from .utils import make_session

logger = logging.getLogger(__name__)

# Публичный эндпоинт reviews-API, которым пользуется фронт 2gis.ru.
# Требует параметр `key` — публичный ключ, который зашит в JS-странице
# карточки организации и периодически обновляется. Мы храним его в
# settings.GIS_PUBLIC_KEY (из env). Без ключа парсер молча вернёт [].
_FETCH_URL_TMPL = 'https://public-api.reviews.2gis.com/2.0/branches/{firm_id}/reviews'

# Формат ссылки на филиал 2ГИС:
#   https://2gis.ru/moscow/firm/70000001234567890
#   https://2gis.ru/moscow/firm/70000001234567890/tab/reviews
#   https://go.2gis.com/abcde — короткие ссылки не поддерживаем
_FIRM_FROM_URL = re.compile(r'/firm/(\d{6,25})')


class GisSource(BaseReviewSource):
    """
    Парсер отзывов с 2ГИС.

    Почему парсер: доступ к partner.api.2gis.ru выдают по заявке (нам его
    согласовали, но ключ ротируется — будет включён следующей итерацией).
    Пока — читаем тот же публичный эндпоинт, которым пользуется сайт 2gis.ru.

    Когда получим партнёрский ключ — добавим метод post_reply() и оставим
    fetch() прежним: публичный read-API стабилен, а партнёрский нужен только
    для записи ответов.
    """

    key = 'gis'
    reply_deeplink_template = '{map_url}/tab/reviews'

    @classmethod
    def extract_external_id(cls, map_url: str) -> str | None:
        if not map_url:
            return None
        match = _FIRM_FROM_URL.search(map_url)
        return match.group(1) if match else None

    def fetch(self, external_id: str, *, limit: int = 50) -> Iterable[FetchedReview]:
        public_key = getattr(settings, 'GIS_PUBLIC_KEY', '') or ''
        if not public_key:
            logger.warning('GIS_PUBLIC_KEY is not configured — skip 2GIS fetch for %s', external_id)
            return []

        session = make_session()
        timeout = getattr(session, 'request_timeout', 20)
        params = {
            'key':      public_key,
            'limit':    min(limit, 50),
            'fields':   'reviews.text,reviews.rating,reviews.date_created,reviews.user',
            'is_advertiser': 'false',
            'sort_by':  'date_edited',
        }
        response = session.get(
            _FETCH_URL_TMPL.format(firm_id=external_id),
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()

        reviews = _extract_reviews_list(payload)
        for item in reviews[:limit]:
            parsed = _parse_review(item)
            if parsed is not None:
                yield parsed


# ── parsing helpers ──────────────────────────────────────────────────────────

def _extract_reviews_list(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    # 2ГИС кладёт отзывы в meta-обёртку; на всякий случай пробуем несколько путей
    for path in (('reviews',), ('result', 'reviews'), ('data', 'reviews')):
        node: object = payload
        for key in path:
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                node = None
                break
        if isinstance(node, list):
            return node
    return []


def _parse_review(item: dict) -> FetchedReview | None:
    if not isinstance(item, dict):
        return None
    external_id = str(item.get('id') or item.get('review_id') or '').strip()
    if not external_id:
        return None

    user = item.get('user') or {}
    author_name = ''
    if isinstance(user, dict):
        author_name = str(user.get('name') or '').strip()

    rating_raw = item.get('rating')
    rating: int | None
    try:
        rating = int(rating_raw) if rating_raw is not None else None
    except (TypeError, ValueError):
        rating = None
    if rating is not None and not 1 <= rating <= 5:
        rating = None

    text = str(item.get('text') or '').strip()
    published_at = _parse_ts(item.get('date_created') or item.get('date_edited'))

    return FetchedReview(
        external_id=external_id,
        author_name=author_name,
        rating=rating,
        text=text,
        published_at=published_at,
        raw=item,
    )


def _parse_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    return None
