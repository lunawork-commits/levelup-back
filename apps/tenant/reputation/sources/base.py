from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable


@dataclass(slots=True)
class FetchedReview:
    """
    Нормализованный отзыв, полученный с внешней площадки.

    Парсеры возвращают именно эту структуру — она не привязана к модели
    и её можно свободно сериализовать в dry-run.
    """

    external_id: str
    author_name: str = ''
    rating: int | None = None
    text: str = ''
    published_at: datetime | None = None
    raw: dict = field(default_factory=dict)


class BaseReviewSource(ABC):
    """
    Абстракция источника отзывов.

    Конкретные источники (YandexSource, GisSource) реализуют:
      - extract_external_id(url): вытащить ID организации из URL карты;
      - fetch(external_id): забрать и распарсить отзывы.

    Подклассы не должны трогать БД — они возвращают FetchedReview,
    а запись в базу делает вызывающая сторона (task / команда), это
    позволяет тестировать парсеры без Django.

    Когда мы получим ключ partner.api.2gis — добавим метод post_reply()
    сюда же и реализуем только в GisSource.
    """

    # Строковый ключ источника — совпадает с ReviewSource.* в models.py
    key: str = ''

    # URL-шаблон для кнопки «Ответить на площадке» — deep-link на карточку
    reply_deeplink_template: str = ''

    @classmethod
    @abstractmethod
    def extract_external_id(cls, map_url: str) -> str | None:
        """
        Вытаскивает ID организации из URL карты.

        Возвращает None, если URL пустой или не соответствует формату.
        Ошибку формата не бросаем — пустой результат фильтруется вызывающим.
        """

    @abstractmethod
    def fetch(self, external_id: str, *, limit: int = 50) -> Iterable[FetchedReview]:
        """
        Забирает отзывы для организации и отдаёт итерируемое FetchedReview.

        Подклассы реализуют все сетевые/парсинговые аспекты и ловят
        собственные ожидаемые ошибки; неожиданные — пусть всплывают,
        их перехватит вызывающая сторона и запишет в ReputationSyncState.last_error.
        """
