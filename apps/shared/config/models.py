from django.db import models


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
