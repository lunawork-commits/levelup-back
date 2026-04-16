import logging
import re
from datetime import datetime, timezone
from typing import Iterable

from .base import BaseReviewSource, FetchedReview
from .utils import make_session

logger = logging.getLogger(__name__)

# Публичный эндпоинт, которым пользуется фронт Яндекс.Карт (не задокументирован,
# но стабильно работает уже несколько лет — на нём сидят Toweco/DailyGrow/Pointer).
# Страница организации выдаёт JSON c отзывами при ajax=1 и businessId.
_FETCH_URL = 'https://yandex.ru/maps/api/business/fetchReviews'

# Формат ссылки на организацию:
#   https://yandex.ru/maps/213/moscow/?oid=12345678901
#   https://yandex.ru/maps/org/slug/12345678901/
#   https://yandex.ru/maps/org/slug/12345678901
# В первом случае oid идёт параметром, во втором — последним сегментом пути
# (11–12 цифр). Коротыш /-/CDxyz мы не поддерживаем — админ пусть даёт полный URL.
_OID_FROM_QUERY   = re.compile(r'[?&]oid=(\d{6,20})')
_OID_FROM_PATH    = re.compile(r'/org/[^/]+/(\d{6,20})')
_OID_FROM_PATH2   = re.compile(r'/org/(\d{6,20})')


class YandexSource(BaseReviewSource):
    """
    Парсер отзывов с Яндекс.Карт.

    Почему парсер, а не партнёрский API:
      — публичного API на отзывы у Яндекса нет, а партнёрка
        «Яндекс Бизнес» заявок на доступ к чтению отзывов не принимает.
        Все отечественные reputation-сервисы используют тот же путь.

    Как только Яндекс поменяет формат — метод fetch() бросит исключение,
    оно будет поймано вызывающей стороной и записано в ReputationSyncState.last_error.
    """

    key = 'yandex'
    reply_deeplink_template = '{map_url}'  # открываем карточку организации как есть

    @classmethod
    def extract_external_id(cls, map_url: str) -> str | None:
        if not map_url:
            return None
        for pattern in (_OID_FROM_QUERY, _OID_FROM_PATH, _OID_FROM_PATH2):
            match = pattern.search(map_url)
            if match:
                return match.group(1)
        return None

    def fetch(self, external_id: str, *, limit: int = 50) -> Iterable[FetchedReview]:
        session = make_session()
        timeout = getattr(session, 'request_timeout', 20)
        params = {
            'ajax':       '1',
            'businessId': external_id,
            'page':       0,
            'pageSize':   min(limit, 50),
            'ranking':    'by_time',
        }
        response = session.get(_FETCH_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()

        reviews = _extract_reviews_list(payload)
        for item in reviews[:limit]:
            parsed = _parse_review(item)
            if parsed is not None:
                yield parsed


# ── парсинг JSON (выделено как pure-функции — тестируем без сети) ────────────

def _extract_reviews_list(payload: dict) -> list[dict]:
    """
    Форматы, которые встречаются в ответе:
      - {'data': {'reviews': [...]}}
      - {'reviews': [...]}
      - {'view': {'reviews': [...]}}
    Берём первый непустой — не падаем при смене обёртки.
    """
    if not isinstance(payload, dict):
        return []
    for path in (('data', 'reviews'), ('reviews',), ('view', 'reviews')):
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
    external_id = str(
        item.get('reviewId')
        or item.get('id')
        or item.get('publicId')
        or ''
    ).strip()
    if not external_id:
        return None

    author = item.get('author') or {}
    author_name = ''
    if isinstance(author, dict):
        author_name = str(author.get('name') or author.get('publicName') or '').strip()
    elif isinstance(author, str):
        author_name = author.strip()

    rating_raw = item.get('rating') or item.get('stars')
    rating: int | None
    try:
        rating = int(rating_raw) if rating_raw is not None else None
    except (TypeError, ValueError):
        rating = None
    if rating is not None and not 1 <= rating <= 5:
        rating = None

    text = str(item.get('text') or item.get('comment') or '').strip()

    published_at = _parse_ts(item.get('updatedTime') or item.get('time') or item.get('createdAt'))

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
    # Миллисекундные unix-таймстемпы (Яндекс отдаёт именно так)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        value = value.strip()
        # ISO-8601: попытка разобрать и цифровой, и ISO-формат
        if value.isdigit():
            return _parse_ts(int(value))
        try:
            # datetime.fromisoformat в 3.11+ принимает 'Z'
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    return None
