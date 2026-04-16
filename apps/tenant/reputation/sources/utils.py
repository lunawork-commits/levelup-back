import random
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Небольшой пул актуальных десктопных UA. 1 запрос/сутки/филиал — этого более
# чем достаточно, чтобы не упереться в rate-limit. Ротация нужна, только чтобы
# не светиться одним и тем же UA по логам площадки.
USER_AGENTS: tuple[str, ...] = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
)


def make_session(
    *,
    timeout: int = 20,
    retries: int = 3,
    backoff_factor: float = 0.7,
    status_forcelist: Iterable[int] = (500, 502, 503, 504, 429),
) -> requests.Session:
    """
    Готовит requests.Session с ретраями и случайным UA.

    Таймаут не устанавливается на уровне сессии (requests такого не умеет),
    поэтому парсеры должны явно передавать timeout= в .get()/.post().
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    })
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=tuple(status_forcelist),
        allowed_methods=frozenset(['GET']),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    # Сохраним рекомендуемый таймаут для вызывающих как атрибут — необязательный.
    session.request_timeout = timeout  # type: ignore[attr-defined]
    return session
