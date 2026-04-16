from datetime import timedelta

from django.db import models
from django.utils import timezone

from apps.shared.base import TimeStampedModel


# ── OfflinePromoScan ─────────────────────────────────────────────────────────

class OfflinePromoScan(TimeStampedModel):
    """
    Факт сканирования QR-кода из офлайн-рекламы.

    Записывается при каждом переходе по QR-коду (даже повторном).
    Используется для подсчёта общего числа сканирований и аналитики
    по рекламным источникам.

    ВАЖНО: эта таблица НЕ участвует в основной аналитике приложения
    (RF-анализ, сегментация, визиты). Данные изолированы в разделе
    «Новые клиенты» админ-панели.
    """

    client = models.ForeignKey(
        'branch.ClientBranch',
        on_delete=models.CASCADE,
        related_name='offline_promo_scans',
        verbose_name='Гость',
    )

    # Рекламный источник — маркировка QR-кода
    source = models.CharField(
        max_length=100,
        blank=True,
        default='',
        verbose_name='Источник трафика',
        help_text=(
            'Рекламный носитель: листовка, баннер, стойка и т.д. '
            'Передаётся через параметр ?promo_source= в QR-коде.'
        ),
        db_index=True,
    )

    # IP / user-agent для дедупликации (опционально)
    ip_address = models.GenericIPAddressField(
        null=True, blank=True,
        verbose_name='IP-адрес',
    )

    def __str__(self):
        src = f' ({self.source})' if self.source else ''
        return f'Скан {self.client}{src} @ {self.created_at:%d.%m.%Y %H:%M}'

    class Meta:
        verbose_name = 'Сканирование (офлайн)'
        verbose_name_plural = 'Сканирования (офлайн)'
        ordering = ['-created_at']
        indexes = [
            models.Index(
                fields=['client', 'created_at'],
                name='opromo_scan_client_idx',
            ),
            models.Index(
                fields=['source', 'created_at'],
                name='opromo_scan_source_idx',
            ),
            models.Index(
                fields=['created_at'],
                name='opromo_scan_time_idx',
            ),
        ]


# ── OfflinePromoGift ─────────────────────────────────────────────────────────

class GiftStatus(models.TextChoices):
    RECEIVED  = 'received',  'Получен'
    ACTIVE    = 'active',    'Активен'
    ACTIVATED = 'activated', 'Активирован в кафе'
    EXPIRED   = 'expired',  'Истёк срок действия'


GIFT_VALIDITY_DAYS = 10


class OfflinePromoGift(TimeStampedModel):
    """
    Подарок, выданный гостю через офлайн-рекламную механику.

    Жизненный цикл:
      received  → гость сыграл, подарок начислен
      active    → подарок доступен к использованию (10 дней)
      activated → подарок использован в кафе (staff подтвердил)
      expired   → прошло 10 дней, подарок стал недоступен

    Один гость = один подарок через офлайн-механику (enforced в сервисе).

    ВАЖНО: эта таблица НЕ участвует в основной аналитике приложения.
    Подарки из офлайн-рекламы не попадают в SuperPrizeEntry, InventoryItem
    и прочие таблицы основной игровой механики.
    """

    client = models.OneToOneField(
        'branch.ClientBranch',
        on_delete=models.CASCADE,
        related_name='offline_promo_gift',
        verbose_name='Гость',
    )

    # Выбранный приз из каталога
    product = models.ForeignKey(
        'catalog.Product',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='offline_promo_gifts',
        verbose_name='Выбранный подарок',
        help_text='Приз, который гость выбрал после игры.',
    )

    # Рекламный источник (копируется из OfflinePromoScan для удобства отчётов)
    source = models.CharField(
        max_length=100,
        blank=True,
        default='',
        verbose_name='Источник трафика',
        db_index=True,
    )

    # Настраиваемый текст поздравления (задаётся в админке на уровне конфигурации)
    congratulation_text = models.TextField(
        blank=True,
        default='',
        verbose_name='Текст поздравления',
        help_text='Если пусто, используется текст по умолчанию.',
    )

    # ── Lifecycle timestamps ─────────────────────────────────────────────────

    received_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name='Дата получения',
    )
    expires_at = models.DateTimeField(
        verbose_name='Действителен до',
        help_text='Автоматически: received_at + 10 дней.',
    )
    activated_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name='Активирован в кафе',
        help_text='Дата и время, когда гость забрал подарок в кафе.',
    )

    # Точка, где подарок был активирован (может отличаться от точки в QR)
    activated_branch = models.ForeignKey(
        'branch.Branch',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='offline_gift_activations',
        verbose_name='Точка активации',
        help_text='Кафе, в котором гость забрал подарок.',
    )

    # ── Computed state ───────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        """Вычисляемый статус подарка."""
        if self.activated_at:
            return GiftStatus.ACTIVATED
        if timezone.now() >= self.expires_at:
            return GiftStatus.EXPIRED
        return GiftStatus.ACTIVE

    @property
    def is_usable(self) -> bool:
        """True — подарок можно использовать прямо сейчас."""
        return self.status == GiftStatus.ACTIVE

    @property
    def days_remaining(self) -> int:
        """Сколько полных дней осталось до истечения."""
        if not self.is_usable:
            return 0
        delta = self.expires_at - timezone.now()
        return max(0, delta.days)

    # ── Business methods ─────────────────────────────────────────────────────

    def activate(self, branch=None) -> bool:
        """
        Активировать подарок в кафе.

        Returns False если подарок уже активирован или истёк.
        """
        if not self.is_usable:
            return False
        self.activated_at = timezone.now()
        if branch:
            self.activated_branch = branch
        self.save(update_fields=['activated_at', 'activated_branch'])
        return True

    def save(self, *args, **kwargs):
        # Автоматически устанавливаем expires_at при создании
        if not self.expires_at:
            base = self.received_at or timezone.now()
            self.expires_at = base + timedelta(days=GIFT_VALIDITY_DAYS)
        super().save(*args, **kwargs)

    # ── Meta ─────────────────────────────────────────────────────────────────

    def __str__(self):
        product_name = self.product.name if self.product else '(не выбран)'
        return f'Офлайн-подарок: {product_name} — {self.client} [{self.status}]'

    class Meta:
        verbose_name = 'Подарок (офлайн)'
        verbose_name_plural = 'Подарки (офлайн)'
        ordering = ['-created_at']
        indexes = [
            models.Index(
                fields=['expires_at'],
                name='opromo_gift_expires_idx',
            ),
            models.Index(
                fields=['activated_at'],
                name='opromo_gift_activated_idx',
            ),
            models.Index(
                fields=['source', 'created_at'],
                name='opromo_gift_source_idx',
            ),
        ]


# ── OfflinePromoConfig ───────────────────────────────────────────────────────

class OfflinePromoConfigModel(TimeStampedModel):
    """
    Конфигурация офлайн-промо для точки.

    Позволяет администратору настроить текст поздравления, минимальную сумму
    заказа и другие параметры.
    """

    branch = models.OneToOneField(
        'branch.Branch',
        on_delete=models.CASCADE,
        related_name='offline_promo_config',
        verbose_name='Торговая точка',
    )

    is_enabled = models.BooleanField(
        default=True,
        verbose_name='Офлайн-промо включено',
    )

    congratulation_text = models.TextField(
        blank=True,
        default='Поздравляем, вы выиграли! Выберите свой приз. '
                'Подарок будет доступен в течение 10 дней.',
        verbose_name='Текст поздравления',
        help_text='Показывается гостю после игры. Можно использовать эмодзи.',
    )

    min_order_text = models.CharField(
        max_length=255,
        blank=True,
        default='',
        verbose_name='Условие (мин. заказ)',
        help_text='Например: «при заказе от 500 ₽». Показывается под текстом поздравления.',
    )

    gift_validity_days = models.PositiveIntegerField(
        default=GIFT_VALIDITY_DAYS,
        verbose_name='Срок действия подарка (дней)',
        help_text='По умолчанию 10 дней.',
    )

    def __str__(self):
        state = '✅' if self.is_enabled else '❌'
        return f'{state} Офлайн-промо: {self.branch.name}'

    class Meta:
        verbose_name = 'Настройки офлайн-промо'
        verbose_name_plural = 'Настройки офлайн-промо'
