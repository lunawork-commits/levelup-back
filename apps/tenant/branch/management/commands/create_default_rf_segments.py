"""
Создаёт стандартные 12 RF-сегментов с подсказками из RF-матрицы.

Использование:
    python manage.py create_default_rf_segments --schema=<tenant_schema>

Если сегменты уже существуют — ВСЕГДА синхронизирует hint/strategy
со стандартным шаблоном для единообразия между всеми кафе.

Флаг --force перезаписывает ВСЁ, включая границы и цвета.
"""

from django.core.management.base import BaseCommand

# ── Standard segment definitions (Леван Кафе reference) ──────────────────────

DEFAULT_SEGMENTS = [
    {
        'code': 'R3F1',
        'name': 'Новички',
        'emoji': '🐣',
        'color': '#8BC34A',
        'recency_min': 0,
        'recency_max': 14,
        'frequency_min': 1,
        'frequency_max': 3,
        'strategy': 'Закрепить первый визит и привести ко второму заказу.',
        'hint': (
            'Цель: Закрепить первый визит и привести ко второму заказу\n'
            'Что отправлять: Приветственное сообщение + простой бонус на следующий заказ '
            '(подарок или небольшая выгода)\n'
            'Частота: 1–2 сообщения за 7–14 дней\n'
            'Если реакция слабая: Отправьте напоминание с тем же бонусом, '
            'затем пауза минимум 2 недели'
        ),
    },
    {
        'code': 'R3F2',
        'name': 'Растущие',
        'emoji': '⭐',
        'color': '#FFC107',
        'recency_min': 0,
        'recency_max': 14,
        'frequency_min': 4,
        'frequency_max': 5,
        'strategy': 'Сформировать привычку заказывать регулярно.',
        'hint': (
            'Цель: Сформировать привычку заказывать регулярно\n'
            'Что отправлять: Выгодное предложение или новинка с ограниченным сроком\n'
            'Частота: 1 сообщение раз в 7–10 дней\n'
            'Если реакция слабая: Смените повод (другой бонус/набор) и сделайте паузу 10–14 дней'
        ),
    },
    {
        'code': 'R3F3',
        'name': 'Суперфанаты',
        'emoji': '🏆',
        'color': '#E91E63',
        'recency_min': 0,
        'recency_max': 14,
        'frequency_min': 6,
        'frequency_max': 9999,
        'strategy': 'Удержать самых активных гостей без перегруза.',
        'hint': (
            'Цель: Удержать самых активных гостей без перегруза\n'
            'Что отправлять: Редкие «приятные» сообщения: подарок или благодарность '
            'без сложных условий\n'
            'Частота: 1 сообщение раз в 10–14 дней\n'
            'Если реакция слабая: Ничего не усиливайте — просто сделайте паузу '
            'и не учащайте рассылки'
        ),
    },
    {
        'code': 'R2F1',
        'name': 'Случайные',
        'emoji': '🎲',
        'color': '#9E9E9E',
        'recency_min': 15,
        'recency_max': 30,
        'frequency_min': 1,
        'frequency_max': 3,
        'strategy': 'Вернуть гостя на повторный заказ.',
        'hint': (
            'Цель: Вернуть гостя на повторный заказ\n'
            'Что отправлять: Простая акция или подарок с коротким сроком действия\n'
            'Частота: 1 сообщение раз в 10–14 дней\n'
            'Если реакция слабая: Через 3–5 дней отправьте другой повод, '
            'затем пауза 2–3 недели'
        ),
    },
    {
        'code': 'R2F2',
        'name': 'Лояльные',
        'emoji': '💛',
        'color': '#FF9800',
        'recency_min': 15,
        'recency_max': 30,
        'frequency_min': 4,
        'frequency_max': 5,
        'strategy': 'Поддерживать интерес и не дать «остыть».',
        'hint': (
            'Цель: Поддерживать интерес и не дать «остыть»\n'
            'Что отправлять: Выгодное предложение или напоминание о кафе без давления\n'
            'Частота: 1 сообщение раз в 14 дней\n'
            'Если реакция слабая: Сделайте паузу 2–3 недели, не усиливая бонус'
        ),
    },
    {
        'code': 'R2F3',
        'name': 'Чемпионы',
        'emoji': '💎',
        'color': '#673AB7',
        'recency_min': 15,
        'recency_max': 30,
        'frequency_min': 6,
        'frequency_max': 9999,
        'strategy': 'Не потерять самых ценных гостей.',
        'hint': (
            'Цель: Не потерять самых ценных гостей\n'
            'Что отправлять: Только VIP-поводы: подарок, благодарность, особое предложение\n'
            'Частота: 1 сообщение в месяц (максимум 2)\n'
            'Если реакция слабая: Ничего не отправляйте дополнительно — лучше пауза, '
            'чем лишнее сообщение'
        ),
    },
    {
        'code': 'R1F1',
        'name': 'Засыпающие',
        'emoji': '😴',
        'color': '#607D8B',
        'recency_min': 31,
        'recency_max': 60,
        'frequency_min': 1,
        'frequency_max': 3,
        'strategy': 'Вернуть до перехода в «потерянные».',
        'hint': (
            'Цель: Вернуть до перехода в «потерянные»\n'
            'Что отправлять: Мягкое напоминание + бонус на возвращение\n'
            'Частота: 2 сообщения за 7–10 дней\n'
            'Если реакция слабая: Если нет отклика — пауза 3–4 недели'
        ),
    },
    {
        'code': 'R1F2',
        'name': 'Под угрозой',
        'emoji': '🔥',
        'color': '#FF5722',
        'recency_min': 31,
        'recency_max': 60,
        'frequency_min': 4,
        'frequency_max': 5,
        'strategy': 'Срочно вернуть ранее постоянных гостей.',
        'hint': (
            'Цель: Срочно вернуть ранее постоянных гостей\n'
            'Что отправлять: Сильное, но понятное предложение с ограниченным сроком\n'
            'Частота: 2 сообщения за 10–14 дней\n'
            'Если реакция слабая: После второго сообщения — пауза минимум 1 месяц'
        ),
    },
    {
        'code': 'R1F3',
        'name': 'Остывающие VIP',
        'emoji': '❄️',
        'color': '#00BCD4',
        'recency_min': 31,
        'recency_max': 60,
        'frequency_min': 6,
        'frequency_max': 9999,
        'strategy': 'Аккуратно вернуть VIP-гостя.',
        'hint': (
            'Цель: Аккуратно вернуть VIP-гостя\n'
            'Что отправлять: Личное сообщение + подарок без условий\n'
            'Частота: 1 сообщение + напоминание через 10–14 дней\n'
            'Если реакция слабая: Дальше только пауза, не чаще 1 раза в 1–2 месяца'
        ),
    },
    {
        'code': 'R0F1',
        'name': 'Потерянные',
        'emoji': '👻',
        'color': '#795548',
        'recency_min': 61,
        'recency_max': 9999,
        'frequency_min': 1,
        'frequency_max': 3,
        'strategy': 'Попробовать реактивировать без давления.',
        'hint': (
            'Цель: Попробовать реактивировать без давления\n'
            'Что отправлять: Одно камбэк-предложение или новинка\n'
            'Частота: 1 сообщение раз в 1–2 месяца\n'
            'Если реакция слабая: Если реакции нет — увеличить паузу или исключить рассылки'
        ),
    },
    {
        'code': 'R0F2',
        'name': 'Ушедшие активные',
        'emoji': '😢',
        'color': '#F44336',
        'recency_min': 61,
        'recency_max': 9999,
        'frequency_min': 4,
        'frequency_max': 5,
        'strategy': 'Вернуть сильных гостей прошлого.',
        'hint': (
            'Цель: Вернуть сильных гостей прошлого\n'
            'Что отправлять: Персональный повод вернуться с хорошей выгодой\n'
            'Частота: 2 попытки за 6 недель\n'
            'Если реакция слабая: Дальше только редкие сообщения раз в 2–3 месяца'
        ),
    },
    {
        'code': 'R0F3',
        'name': 'Потерянные VIP',
        'emoji': '💔',
        'color': '#D32F2F',
        'recency_min': 61,
        'recency_max': 9999,
        'frequency_min': 6,
        'frequency_max': 9999,
        'strategy': 'Финальная попытка вернуть VIP.',
        'hint': (
            'Цель: Финальная попытка вернуть VIP\n'
            'Что отправлять: Очень уважительное предложение или личное обращение\n'
            'Частота: 1 сообщение раз в 6–8 недель\n'
            'Если реакция слабая: Если нет реакции — полностью прекратить рассылки'
        ),
    },
]


class Command(BaseCommand):
    help = (
        'Создаёт стандартные 12 RF-сегментов с подсказками из RF-матрицы. '
        'Существующие сегменты: hint и strategy ВСЕГДА синхронизируются со стандартом. '
        'Используйте --force для полной перезаписи (включая границы и цвета).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Перезаписать все поля существующих сегментов (включая границы и подсказки).',
        )

    def handle(self, *args, **options):
        from apps.tenant.analytics.models import RFSegment

        force = options['force']
        created = updated = skipped = 0

        for seg_data in DEFAULT_SEGMENTS:
            code = seg_data['code']
            try:
                obj = RFSegment.objects.get(code=code)
                if force:
                    for field, value in seg_data.items():
                        setattr(obj, field, value)
                    obj.save()
                    updated += 1
                    self.stdout.write(self.style.WARNING(f'  ↻ {code} — перезаписан'))
                else:
                    # Always sync hint and strategy to standard template
                    changed = False
                    standard_hint = seg_data.get('hint', '')
                    standard_strategy = seg_data.get('strategy', '')

                    if standard_hint and obj.hint != standard_hint:
                        obj.hint = standard_hint
                        changed = True
                    if standard_strategy and obj.strategy != standard_strategy:
                        obj.strategy = standard_strategy
                        changed = True

                    if changed:
                        obj.save(update_fields=['hint', 'strategy'])
                        updated += 1
                        self.stdout.write(self.style.WARNING(f'  ↻ {code} — подсказки синхронизированы со стандартом'))
                    else:
                        skipped += 1
                        self.stdout.write(f'  ✓ {code} — без изменений')

            except RFSegment.DoesNotExist:
                RFSegment.objects.create(**seg_data)
                created += 1
                self.stdout.write(self.style.SUCCESS(f'  + {code} — создан'))

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(
            f'Готово: {created} создано, {updated} обновлено, {skipped} без изменений.'
        ))