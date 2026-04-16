from django.db import models

from apps.shared.base import TimeStampedModel


# ── Sources / statuses ────────────────────────────────────────────────────────

class ReviewSource(models.TextChoices):
    YANDEX = 'yandex', 'Яндекс.Карты'
    GIS    = 'gis',    '2ГИС'


class ReviewStatus(models.TextChoices):
    NEW      = 'new',      'Новый'
    SEEN     = 'seen',     'Просмотрен'
    ANSWERED = 'answered', 'Есть ответ'
    IGNORED  = 'ignored',  'Проигнорирован'


# ── ExternalReview ────────────────────────────────────────────────────────────

class ExternalReview(TimeStampedModel):
    """
    Отзыв, распарсенный с внешней площадки (Яндекс.Карты / 2ГИС).

    Уникальность гарантируется парой (source, external_id) — повторный fetch
    той же страницы не создаёт дубликатов.

    Ответ пишется в админке и хранится тут же (reply_text, replied_at).
    Публикация на площадке выполняется вручную через deep-link «Ответить на …»
    пока не подключён партнёрский API.
    """

    branch = models.ForeignKey(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='external_reviews',
        verbose_name='Торговая точка',
    )
    source = models.CharField(
        max_length=10,
        choices=ReviewSource.choices,
        verbose_name='Источник',
    )
    external_id = models.CharField(
        max_length=255,
        verbose_name='ID на площадке',
        help_text='Идентификатор отзыва из ответа API площадки. Используется для дедупликации.',
    )

    # ── Содержимое отзыва ─────────────────────────────────────────────────────

    author_name = models.CharField(
        max_length=255,
        blank=True,
        verbose_name='Автор',
    )
    rating = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        verbose_name='Оценка (1–5)',
    )
    text = models.TextField(
        blank=True,
        verbose_name='Текст отзыва',
    )
    published_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Опубликован',
        help_text='Дата отзыва на площадке (если удалось распарсить).',
    )

    # ── Служебное ─────────────────────────────────────────────────────────────

    fetched_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name='Импортирован',
    )
    raw = models.JSONField(
        default=dict,
        blank=True,
        verbose_name='Сырые данные',
        help_text='JSON, полученный с площадки. На случай ручного разбора при изменении формата.',
    )

    # ── Состояние в админке ───────────────────────────────────────────────────

    status = models.CharField(
        max_length=10,
        choices=ReviewStatus.choices,
        default=ReviewStatus.NEW,
        verbose_name='Статус',
    )
    reply_text = models.TextField(
        blank=True,
        verbose_name='Ответ',
        help_text='Подготовленный в админке текст ответа.',
    )
    replied_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Дата ответа',
    )
    replied_by = models.ForeignKey(
        'users.User',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='+',
        verbose_name='Кто ответил',
    )

    def __str__(self):
        rating = f'★{self.rating}' if self.rating else '—'
        return f'{self.get_source_display()} {rating} | {self.branch.name} | {self.author_name or "?"}'

    class Meta:
        verbose_name = 'Внешний отзыв'
        verbose_name_plural = 'Внешние отзывы'
        ordering = ['-published_at', '-fetched_at']
        constraints = [
            models.UniqueConstraint(
                fields=['source', 'external_id'],
                name='extreview_source_extid_uniq',
            ),
        ]
        indexes = [
            models.Index(fields=['branch', 'status'],      name='extreview_branch_status_idx'),
            models.Index(fields=['branch', 'source'],      name='extreview_branch_src_idx'),
            models.Index(fields=['status', '-fetched_at'], name='extreview_status_time_idx'),
        ]


# ── ReputationSyncState ───────────────────────────────────────────────────────

class ReputationSyncState(TimeStampedModel):
    """
    Состояние последней синхронизации для пары (branch, source).

    Одна запись на связку точка+источник. Обновляется задачей
    fetch_external_reviews после каждой попытки. Видна в админке —
    последняя ошибка не валит всю задачу, а остаётся тут для диагностики.
    """

    branch = models.ForeignKey(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='reputation_sync_states',
        verbose_name='Торговая точка',
    )
    source = models.CharField(
        max_length=10,
        choices=ReviewSource.choices,
        verbose_name='Источник',
    )
    last_run_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Последний запуск',
    )
    last_ok_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Последний успех',
    )
    last_error = models.TextField(
        blank=True,
        verbose_name='Последняя ошибка',
        help_text='Стек-трейс или короткое описание. Пусто — последний запуск был успешным.',
    )
    reviews_fetched = models.PositiveIntegerField(
        default=0,
        verbose_name='Получено (всего)',
        help_text='Накопительно: суммарное число распарсенных отзывов с момента включения.',
    )

    def __str__(self):
        return f'{self.branch.name} / {self.get_source_display()}'

    class Meta:
        verbose_name = 'Состояние синхронизации'
        verbose_name_plural = 'Состояния синхронизации'
        constraints = [
            models.UniqueConstraint(
                fields=['branch', 'source'],
                name='repsync_branch_source_uniq',
            ),
        ]
        ordering = ['branch__name', 'source']
