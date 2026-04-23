from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from colorfield.fields import ColorField

from apps.shared.base import TimeStampedModel


# ── KnowledgeBase ─────────────────────────────────────────────────────────────

class KnowledgeBaseDocument(TimeStampedModel):
    """
    Документ базы знаний для ИИ-анализа отзывов.

    Загружается Word (.docx) или текстовый файл с инструкциями по анализу.
    Текст автоматически извлекается и используется как system-контекст
    при каждом обращении к Claude для анализа тональности отзывов.
    """

    title = models.CharField('Название', max_length=255)
    file  = models.FileField(
        'Файл (.docx / .txt)',
        upload_to='knowledge_base/',
        help_text='Загрузите Word-документ (.docx) или текстовый файл (.txt) с инструкциями.',
    )
    extracted_text = models.TextField(
        'Извлечённый текст',
        blank=True,
        help_text='Заполняется автоматически при сохранении файла.',
    )
    is_active = models.BooleanField(
        'Используется в анализе',
        default=True,
        help_text='Только активные документы передаются ИИ в качестве инструкций.',
    )

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        # Extract text after file is saved
        if self.file and not self.extracted_text:
            try:
                self.extracted_text = _extract_document_text(self.file.path)
                type(self).objects.filter(pk=self.pk).update(extracted_text=self.extracted_text)
            except Exception:
                pass

    def __str__(self):
        return self.title

    class Meta:
        verbose_name = 'Документ базы знаний'
        verbose_name_plural = 'База знаний (инструкции для ИИ)'
        ordering = ['-created_at']


def _extract_document_text(file_path: str) -> str:
    """Extract plain text from .docx or .txt file."""
    if file_path.endswith('.docx'):
        import docx
        doc = docx.Document(file_path)
        return '\n'.join(p.text for p in doc.paragraphs if p.text.strip())
    with open(file_path, encoding='utf-8', errors='ignore') as f:
        return f.read()


# ── RFSegment ─────────────────────────────────────────────────────────────────

class RFSegment(TimeStampedModel):
    """
    Справочник RF-сегментов — обычно 12 штук с разными границами R и F.

    Каждый сегмент задаёт диапазоны:
      recency_min/max — дни с последнего визита
      frequency_min/max — кол-во визитов за период анализа

    Гость попадает в сегмент, если его recency_days и frequency
    оба попадают в соответствующие диапазоны.
    """

    code = models.CharField(max_length=10, unique=True, verbose_name='Код')
    name = models.CharField(max_length=100, verbose_name='Название')

    # ── Recency boundaries (days since last visit) ────────────────────────────

    recency_min = models.IntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        verbose_name='Давность: от (дней)',
        help_text='Нижняя граница дней с последнего визита (включительно).',
    )
    recency_max = models.IntegerField(
        default=9999,
        validators=[MinValueValidator(0)],
        verbose_name='Давность: до (дней)',
        help_text='Верхняя граница дней с последнего визита (включительно).',
    )

    # ── Frequency boundaries (number of visits) ───────────────────────────────

    frequency_min = models.IntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        verbose_name='Частота: от (визитов)',
        help_text='Минимальное кол-во визитов за период (включительно).',
    )
    frequency_max = models.IntegerField(
        default=9999,
        validators=[MinValueValidator(0)],
        verbose_name='Частота: до (визитов)',
        help_text='Максимальное кол-во визитов за период (включительно).',
    )

    # ── Display ───────────────────────────────────────────────────────────────

    emoji = models.CharField(max_length=10, verbose_name='Эмодзи')
    color = ColorField(default='#417690', verbose_name='Цвет')
    strategy = models.TextField(verbose_name='Маркетинговая стратегия')
    hint = models.TextField(
        blank=True,
        verbose_name='Подсказка для персонала',
        help_text='Краткая инструкция менеджеру. Отображается в таблице сегментов.',
    )

    # ── Campaign tracking ─────────────────────────────────────────────────────

    last_campaign_date = models.DateTimeField(
        blank=True,
        null=True,
        verbose_name='Дата последней рассылки',
    )

    def clean(self):
        errors = {}
        if self.recency_min > self.recency_max:
            errors['recency_min'] = 'Нижняя граница давности не может быть больше верхней.'
        if self.frequency_min > self.frequency_max:
            errors['frequency_min'] = 'Нижняя граница частоты не может быть больше верхней.'
        if errors:
            raise ValidationError(errors)

    def __str__(self):
        return f'{self.emoji} {self.name} ({self.code})'

    class Meta:
        verbose_name = 'RF-сегмент'
        verbose_name_plural = 'RF-сегменты'
        ordering = ['recency_min', 'frequency_min']


# ── GuestRFScore ──────────────────────────────────────────────────────────────

class GuestRFScore(models.Model):
    """
    RF-метрика гостя в конкретной торговой точке.

    Одна запись на ClientBranch — перезаписывается при каждом пересчёте.
    Пересчёт запускается через Celery (периодически или вручную).

    r_score / f_score — нормализованные баллы (1 = плохой, N = хороший).
    segment — ссылка на RFSegment, подобранный по диапазонам.
    """

    client = models.OneToOneField(
        'guest.Client',
        on_delete=models.CASCADE,
        related_name='rf_score',
        verbose_name='Гость',
    )
    recency_days = models.PositiveIntegerField(
        verbose_name='Давность (дней)',
        help_text='Дней с последнего визита на момент расчёта.',
    )
    frequency = models.PositiveIntegerField(
        verbose_name='Частота (визитов)',
        help_text='Кол-во визитов за период анализа.',
    )
    r_score = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1)],
        verbose_name='R-балл',
        help_text='Балл давности (1 = давно, выше = недавнее).',
    )
    f_score = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1)],
        verbose_name='F-балл',
        help_text='Балл частоты (1 = редко, выше = чаще).',
    )
    segment = models.ForeignKey(
        RFSegment,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='guests',
        verbose_name='Сегмент',
    )
    calculated_at = models.DateTimeField(
        auto_now=True,
        verbose_name='Рассчитано',
    )

    def __str__(self):
        seg = self.segment.code if self.segment else '—'
        return f'{self.client} [R{self.r_score} F{self.f_score} / {seg}]'

    class Meta:
        verbose_name = 'RF-метрика гостя'
        verbose_name_plural = 'RF-метрики гостей'
        indexes = [
            models.Index(fields=['r_score', 'f_score'], name='rf_score_rf_idx'),
            models.Index(fields=['segment'], name='rf_score_segment_idx'),
            models.Index(fields=['calculated_at'], name='rf_score_calc_idx'),
        ]


# ── RFMigrationLog ────────────────────────────────────────────────────────────

class RFMigrationLog(models.Model):
    """
    Журнал перемещения гостя между сегментами.

    Запись создаётся при каждом пересчёте, если сегмент изменился.
    SET_NULL на FK — чтобы не терять историю при удалении/переименовании сегмента.
    Не наследует TimeStampedModel: лог-записи никогда не обновляются,
    updated_at был бы бессмысленным полем.
    """

    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Мигрировал')

    client = models.ForeignKey(
        'guest.Client',
        on_delete=models.CASCADE,
        related_name='rf_migrations',
        verbose_name='Гость',
    )
    from_segment = models.ForeignKey(
        RFSegment,
        related_name='migrations_from',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        verbose_name='Из сегмента',
    )
    to_segment = models.ForeignKey(
        RFSegment,
        related_name='migrations_to',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        verbose_name='В сегмент',
    )

    def __str__(self):
        return f'{self.client}: {self.from_segment} → {self.to_segment}'

    class Meta:
        verbose_name = 'RF-миграция'
        verbose_name_plural = 'RF-миграции'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['client', 'created_at'], name='rf_mig_client_idx'),
            models.Index(fields=['to_segment', 'created_at'], name='rf_mig_to_seg_idx'),
        ]


# ── RFSettings ────────────────────────────────────────────────────────────────

class RFSettings(TimeStampedModel):
    """
    Настройки RF-анализа для конкретной торговой точки.
    Одна запись на Branch.
    """

    branch = models.OneToOneField(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='rf_settings',
        verbose_name='Торговая точка',
    )
    analysis_period = models.PositiveIntegerField(
        default=365,
        verbose_name='Период анализа (дней)',
        help_text='Учитываются визиты за последние N дней.',
    )
    stats_reset_date = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Дата обнуления статистики',
        help_text=(
            'Если задана — RF-анализ и общая статистика учитывают '
            'ТОЛЬКО данные после этой даты. '
            'Балансы монет, задания и инвентарь НЕ затрагиваются.'
        ),
    )

    def __str__(self):
        return f'RF-настройки: {self.branch}'

    class Meta:
        verbose_name = 'RF-настройки'
        verbose_name_plural = 'RF-настройки'


# ── BranchSegmentSnapshot ─────────────────────────────────────────────────────

class BranchSegmentSnapshot(TimeStampedModel):
    """
    Ежедневный снапшот: сколько гостей в каждом сегменте по каждой точке.

    Хранит историю для построения трендов без пересчёта каждый раз.
    Обновляется через:
        BranchSegmentSnapshot.objects.update_or_create(
            branch=branch, segment=segment, date=today,
            defaults={'guests_count': count}
        )

    date — явно проставляется кодом (не auto_now_add) для возможности
    ретроспективного заполнения.
    """

    branch = models.ForeignKey(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='segment_snapshots',
        verbose_name='Торговая точка',
    )
    segment = models.ForeignKey(
        RFSegment,
        on_delete=models.CASCADE,
        related_name='snapshots',
        verbose_name='Сегмент',
    )
    guests_count = models.PositiveIntegerField(
        default=0,
        verbose_name='Кол-во гостей',
    )
    date = models.DateField(
        db_index=True,
        verbose_name='Дата',
        help_text='Дата расчёта снапшота.',
    )

    def __str__(self):
        return f'{self.date} | {self.branch.name} | {self.segment.code}: {self.guests_count}'

    class Meta:
        unique_together = ('branch', 'segment', 'date')
        ordering = ['-date', 'branch', 'segment']
        verbose_name = 'Снапшот сегмента'
        verbose_name_plural = 'Снапшоты сегментов'
        indexes = [
            models.Index(fields=['branch', 'date'], name='snapshot_branch_date_idx'),
            models.Index(fields=['segment', 'date'], name='snapshot_segment_date_idx'),
        ]


# ── Delivery RF (separate metrics, same RFSegment definitions) ─────────────────

class GuestRFScoreDelivery(models.Model):
    """
    RF-метрика гостя по активациям доставки.

    Аналог GuestRFScore, но recency/frequency считается по Delivery.activated_by,
    а не по ClientBranchVisit. Использует те же RFSegment, что и ресторанный RF.
    """

    client = models.OneToOneField(
        'guest.Client',
        on_delete=models.CASCADE,
        related_name='rf_score_delivery',
        verbose_name='Гость',
    )
    recency_days = models.PositiveIntegerField(
        verbose_name='Давность (дней)',
        help_text='Дней с последней активации доставки.',
    )
    frequency = models.PositiveIntegerField(
        verbose_name='Частота (заказов)',
        help_text='Кол-во активаций доставки за период анализа.',
    )
    r_score = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1)],
        verbose_name='R-балл',
    )
    f_score = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1)],
        verbose_name='F-балл',
    )
    segment = models.ForeignKey(
        RFSegment,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='delivery_guests',
        verbose_name='Сегмент',
    )
    calculated_at = models.DateTimeField(auto_now=True, verbose_name='Рассчитано')

    def __str__(self):
        seg = self.segment.code if self.segment else '—'
        return f'[Доставка] {self.client} [R{self.r_score} F{self.f_score} / {seg}]'

    class Meta:
        verbose_name = 'RF-метрика гостя (доставка)'
        verbose_name_plural = 'RF-метрики гостей (доставка)'
        indexes = [
            models.Index(fields=['r_score', 'f_score'], name='rf_del_score_rf_idx'),
            models.Index(fields=['segment'],             name='rf_del_segment_idx'),
            models.Index(fields=['calculated_at'],       name='rf_del_calc_idx'),
        ]


class RFMigrationLogDelivery(models.Model):
    """
    Журнал смены RF-сегментов по доставке.
    Аналог RFMigrationLog для доставочного RF.
    """

    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Мигрировал')

    client = models.ForeignKey(
        'guest.Client',
        on_delete=models.CASCADE,
        related_name='rf_migrations_delivery',
        verbose_name='Гость',
    )
    from_segment = models.ForeignKey(
        RFSegment,
        related_name='delivery_migrations_from',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        verbose_name='Из сегмента',
    )
    to_segment = models.ForeignKey(
        RFSegment,
        related_name='delivery_migrations_to',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        verbose_name='В сегмент',
    )

    def __str__(self):
        return f'[Доставка] {self.client}: {self.from_segment} → {self.to_segment}'

    class Meta:
        verbose_name = 'RF-миграция (доставка)'
        verbose_name_plural = 'RF-миграции (доставка)'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['client', 'created_at'],     name='rf_del_mig_client_idx'),
            models.Index(fields=['to_segment', 'created_at'], name='rf_del_mig_to_seg_idx'),
        ]


class POSGuestCache(models.Model):
    """
    Кэш количества гостей из POS-системы (IIKO / Dooglys).

    Одна запись на (branch, date) — перезаписывается Celery-задачей
    fetch_pos_data_all_tenants_task, которая запускается ежедневно.

    get_pos_guests_count() суммирует эти записи за нужный диапазон дат
    вместо прямого обращения к POS API при каждом запросе дашборда.
    """

    branch = models.ForeignKey(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='pos_guest_cache',
        verbose_name='Торговая точка',
    )
    date = models.DateField(db_index=True, verbose_name='Дата')
    guest_count = models.PositiveIntegerField(default=0, verbose_name='Кол-во гостей')
    fetched_at = models.DateTimeField(auto_now=True, verbose_name='Обновлено')

    class Meta:
        unique_together = ('branch', 'date')
        verbose_name = 'Кэш POS гостей'
        verbose_name_plural = 'Кэш POS гостей'
        indexes = [
            models.Index(fields=['branch', 'date'], name='pos_cache_branch_date_idx'),
        ]

    def __str__(self):
        return f'{self.date} | {self.branch.name}: {self.guest_count}'


class BranchSegmentSnapshotDelivery(TimeStampedModel):
    """
    Ежедневный снапшот распределения гостей по сегментам доставки.
    Аналог BranchSegmentSnapshot для доставочного RF.
    """

    branch = models.ForeignKey(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='segment_snapshots_delivery',
        verbose_name='Торговая точка',
    )
    segment = models.ForeignKey(
        RFSegment,
        on_delete=models.CASCADE,
        related_name='delivery_snapshots',
        verbose_name='Сегмент',
    )
    guests_count = models.PositiveIntegerField(default=0, verbose_name='Кол-во гостей')
    date = models.DateField(db_index=True, verbose_name='Дата')

    def __str__(self):
        return f'[Дост.] {self.date} | {self.branch.name} | {self.segment.code}: {self.guests_count}'

    class Meta:
        unique_together = ('branch', 'segment', 'date')
        ordering = ['-date', 'branch', 'segment']
        verbose_name = 'Снапшот сегмента (доставка)'
        verbose_name_plural = 'Снапшоты сегментов (доставка)'
        indexes = [
            models.Index(fields=['branch', 'date'],   name='snap_del_branch_date_idx'),
            models.Index(fields=['segment', 'date'],  name='snap_del_segment_date_idx'),
        ]
