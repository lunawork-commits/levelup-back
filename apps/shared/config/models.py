from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models


def validate_video_size(f):
    """Ограничение на размер загружаемого видео (берётся из settings.MAX_UPLOAD_MB)."""
    max_mb = getattr(settings, 'MAX_UPLOAD_MB', 50)
    if f.size > max_mb * 1024 * 1024:
        raise ValidationError(f'Файл больше {max_mb} МБ — загрузите меньший.')


class POSType(models.TextChoices):
    NONE = 'none', 'Не подключено'
    IIKO = 'iiko', 'iiko'
    DOOGLYS = 'dooglys', 'Dooglys'


class ClientConfig(models.Model):
    company = models.OneToOneField(
        'clients.Company',
        on_delete=models.CASCADE,
        related_name='config',
        verbose_name='Компания',
    )

    # --- Брендинг (платный) ---
    logotype_image = models.ImageField(
        upload_to='config/logos/',
        blank=True, null=True,
        verbose_name='Логотип',
        help_text='Опционально — активируется при подключении платного брендинга',
    )
    coin_image = models.ImageField(
        upload_to='config/coins/',
        blank=True, null=True,
        verbose_name='Иконка монеты',
        help_text='Опционально — активируется при подключении платного брендинга',
    )

    # --- ВКонтакте ---
    vk_group_id = models.PositiveIntegerField(
        verbose_name='VK Group ID',
        help_text='Числовой ID группы ВКонтакте. Отображается на фронте для подписки.',
    )
    vk_group_name = models.CharField(
        max_length=255,
        verbose_name='Название группы VK',
        help_text='Отображается в приложении рядом с кнопкой «Подписаться»',
    )

    # --- Кассовая система ---
    pos_type = models.CharField(
        max_length=10,
        choices=POSType.choices,
        default=POSType.NONE,
        verbose_name='Кассовая система',
        help_text='Выберите систему, которую использует клиент. Поля ниже активируются автоматически.',
    )

    # IIKO
    iiko_api_url = models.URLField(
        blank=True,
        verbose_name='IIKO API URL',
        help_text='Пример: https://iiko.biz/api/1/',
    )
    iiko_login = models.CharField(
        max_length=255,
        blank=True,
        verbose_name='IIKO Логин',
        help_text='Логин пользователя API из кабинета iiko.',
    )
    iiko_password = models.CharField(
        max_length=255,
        blank=True,
        verbose_name='IIKO Пароль',
        help_text='Пароль пользователя API из кабинета iiko.',
    )

    # Dooglys
    dooglys_api_url = models.URLField(
        blank=True,
        verbose_name='Dooglys API URL',
        help_text='Пример: https://api.dooglys.com/v1/',
    )
    dooglys_api_token = models.CharField(
        max_length=512,
        blank=True,
        verbose_name='Dooglys API Token',
        help_text='API-ключ из личного кабинета Dooglys.',
    )

    def __str__(self):
        return f'Настройки — {self.company.name}'

    class Meta:
        verbose_name = 'Настройки клиента'
        verbose_name_plural = 'Настройки клиентов'


class LandingSettings(models.Model):
    """
    Глобальные настройки «посадочного» видео, которое показывается в модалке
    на кнопке «ХОЧУ ЛЕВЕЛUP В СВОЁ КАФЕ» в профиле VK Mini App и веб-SPA.

    Синглтон: всегда одна строка с pk=1. Редактируется только суперадмином
    в публичной схеме. Фронт читает публичный эндпоинт
    GET /api/v1/public/landing-settings/ с 5-минутным кэшированием.
    """

    is_enabled = models.BooleanField(
        default=False,
        verbose_name='Модалка включена',
        help_text='Если выключено — кнопка ведёт по cta_url (старый редирект в Telegram).',
    )
    button_label = models.CharField(
        max_length=120,
        default='ХОЧУ ЛЕВЕЛUP В СВОЁ КАФЕ',
        verbose_name='Текст кнопки',
    )
    title = models.CharField(
        max_length=200,
        blank=True,
        default='LevelUP для вашего кафе',
        verbose_name='Заголовок модалки',
    )
    description = models.TextField(
        blank=True,
        default='',
        verbose_name='Описание',
        help_text='Короткий текст под видео. Пустое поле — описание не показывается.',
    )
    video_mp4 = models.FileField(
        upload_to='landing/',
        blank=True, null=True,
        validators=[validate_video_size],
        verbose_name='Видео (MP4)',
        help_text='MP4, до MAX_UPLOAD_MB МБ. Рекомендуется ≤ 30 сек, H.264.',
    )
    video_poster = models.ImageField(
        upload_to='landing/posters/',
        blank=True, null=True,
        verbose_name='Постер',
        help_text='Картинка-заглушка до старта видео. Опционально.',
    )
    cta_url = models.URLField(
        blank=True,
        default='https://t.me/LevelUP_bot',
        verbose_name='Ссылка CTA',
        help_text='Куда ведёт кнопка под видео (например, чат в Telegram).',
    )
    cta_label = models.CharField(
        max_length=120,
        default='Написать в Telegram',
        verbose_name='Текст кнопки CTA',
    )
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return 'Настройки лендинга (видео-модалка)'

    def save(self, *args, **kwargs):
        self.pk = 1
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return None

    @classmethod
    def load(cls) -> 'LandingSettings':
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj

    @property
    def video_url(self) -> str:
        return self.video_mp4.url if self.video_mp4 else ''

    @property
    def poster_url(self) -> str:
        return self.video_poster.url if self.video_poster else ''

    class Meta:
        verbose_name = 'Настройки лендинга'
        verbose_name_plural = 'Настройки лендинга'
