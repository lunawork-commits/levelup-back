from datetime import timedelta

from django.db import models
from django.utils import timezone

from apps.shared.base import TimeStampedModel


# ── SuperPrizeEntry ───────────────────────────────────────────────────────────

class SuperPrizeTrigger(models.TextChoices):
    GAME     = 'game',     'Игра'
    MANUAL   = 'manual',   'В ручную'
    BIRTHDAY = 'birthday', 'День Рождения'


class SuperPrizeEntry(TimeStampedModel):
    """
    Ваучер на суперприз — право гостя выбрать один приз из пула is_super_prize.

    Жизненный цикл:
      pending → claimed  (гость выбирает приз: product заполняется, claimed_at фиксируется)
      claimed → issued   (официант подтверждает выдачу)
      pending → expired  (гость не сделал выбор до expires_at)

    До момента выбора product=NULL.
    """

    client_branch = models.ForeignKey(
        'branch.ClientBranch',
        on_delete=models.CASCADE,
        related_name='super_prizes',
        verbose_name='Гость',
    )
    acquired_from = models.CharField(
        max_length=20,
        choices=SuperPrizeTrigger,
        verbose_name='Источник получения',
    )
    description = models.TextField(
        blank=True,
        verbose_name='Заметка',
        help_text='Например: «Достигнуто 10 посещений». Только для внутреннего использования.',
    )

    # ── Prize selection ───────────────────────────────────────────────────────

    product = models.ForeignKey(
        'catalog.Product',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='super_prize_claims',
        verbose_name='Выбранный приз',
        help_text='Заполняется когда гость делает выбор из пула суперпризов.',
    )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    expires_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Действителен до',
        help_text='Срок, до которого гость должен сделать выбор. Пусто — бессрочно.',
    )
    claimed_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name='Приз выбран',
    )
    issued_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name='Выдан',
    )

    # ── Computed state ────────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        if self.issued_at:
            return 'issued'
        if self.claimed_at:
            return 'claimed'
        if self.expires_at and timezone.now() >= self.expires_at:
            return 'expired'
        return 'pending'

    @property
    def is_claimable(self) -> bool:
        return self.status == 'pending'

    # ── Business methods ──────────────────────────────────────────────────────

    def claim(self, product) -> bool:
        """Guest selects a product from the super prize pool."""
        if self.status != 'pending':
            return False
        self.product = product
        self.claimed_at = timezone.now()
        self.save(update_fields=['product', 'claimed_at'])
        return True

    def mark_issued(self) -> bool:
        """Staff confirms the prize was handed out."""
        if self.status != 'claimed':
            return False
        self.issued_at = timezone.now()
        self.save(update_fields=['issued_at'])
        return True

    def __str__(self):
        prize = self.product.name if self.product else '(не выбран)'
        return f'Суперприз {prize} — {self.client_branch}'

    class Meta:
        verbose_name = 'Суперприз'
        verbose_name_plural = 'Суперпризы'
        ordering = ['-created_at']
        indexes = [
            models.Index(
                fields=['client_branch', 'issued_at'],
                name='sp_client_issued_idx',
            ),
            models.Index(
                fields=['client_branch', 'claimed_at'],
                name='sp_client_claimed_idx',
            ),
            models.Index(
                fields=['expires_at'],
                name='sp_expires_idx',
            ),
            models.Index(
                fields=['product', 'acquired_from'],
                name='sp_product_trigger_idx',
            ),
        ]


class AcquisitionSource(models.TextChoices):
    PURCHASE    = 'purchase',    'Покупка за баллы'
    SUPER_PRIZE = 'super_prize', 'Суперприз'
    BIRTHDAY    = 'birthday',    'Подарок на ДР'
    MANUAL      = 'manual',      'Выдано вручную'


class ItemStatus(models.TextChoices):
    PENDING = 'pending', 'Ожидает активации'
    ACTIVE  = 'active',  'Активирован'
    EXPIRED = 'expired', 'Истёк'
    USED    = 'used',    'Использован'


class InventoryItem(TimeStampedModel):
    """
    Приз, выданный гостю.

    Жизненный цикл:
      pending → active   (гость нажимает «Активировать» в приложении)
      active  → used     (официант подтверждает выдачу)
      active  → expired  (срок действия вышел до подтверждения)

    Поле duration задаёт окно (в минутах), в течение которого активированный
    приз считается действительным. 0 — без ограничения времени.
    """

    client_branch = models.ForeignKey(
        'branch.ClientBranch',
        on_delete=models.CASCADE,
        related_name='inventory',
        verbose_name='Гость',
    )
    product = models.ForeignKey(
        'catalog.Product',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='inventory_items',
        verbose_name='Приз',
    )

    acquired_from = models.CharField(
        max_length=20,
        choices=AcquisitionSource,
        verbose_name='Способ получения',
    )
    description = models.TextField(
        blank=True,
        verbose_name='Заметка',
        help_text='Только для внутреннего использования.',
    )

    # Duration in minutes; 0 means the prize never expires after activation
    duration = models.PositiveIntegerField(
        default=40,
        verbose_name='Длительность (мин)',
        help_text='Сколько минут действителен приз после активации. 0 — без ограничения.',
    )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    activated_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name='Активирован',
    )
    expires_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name='Истекает',
        help_text='Устанавливается автоматически при активации.',
    )
    used_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name='Использован',
    )

    # ── Computed state ────────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        if self.used_at:
            return ItemStatus.USED
        if self.activated_at:
            if self.expires_at and timezone.now() >= self.expires_at:
                return ItemStatus.EXPIRED
            return ItemStatus.ACTIVE
        return ItemStatus.PENDING

    @property
    def is_valid(self) -> bool:
        """True only when the prize is active (usable right now)."""
        return self.status == ItemStatus.ACTIVE

    # ── Business methods ──────────────────────────────────────────────────────

    def activate(self) -> bool:
        """Mark the prize as activated. Returns False if already activated."""
        if self.status != ItemStatus.PENDING:
            return False
        self.activated_at = timezone.now()
        self.expires_at = (
            self.activated_at + timedelta(minutes=self.duration)
            if self.duration
            else None
        )
        self.save(update_fields=['activated_at', 'expires_at'])
        return True

    def mark_used(self) -> bool:
        """Confirm prize was issued by staff. Returns False if not active."""
        if self.status != ItemStatus.ACTIVE:
            return False
        self.used_at = timezone.now()
        self.save(update_fields=['used_at'])
        return True

    # ── Meta ──────────────────────────────────────────────────────────────────

    def __str__(self):
        return f'{self.product.name} — {self.client_branch}'

    class Meta:
        verbose_name = 'Приз гостя'
        verbose_name_plural = 'Призы гостей'
        ordering = ['-created_at']
        indexes = [
            models.Index(
                fields=['client_branch', 'used_at'],
                name='inventory_client_used_idx',
            ),
            models.Index(
                fields=['client_branch', 'activated_at'],
                name='inventory_client_act_idx',
            ),
            models.Index(
                fields=['product', 'acquired_from'],
                name='inventory_prod_source_idx',
            ),
            models.Index(
                fields=['expires_at'],
                name='inventory_expires_idx',
            ),
        ]
