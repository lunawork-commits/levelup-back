"""
AI-сервис генерации ответов на внешние отзывы (Яндекс.Карты / 2ГИС).

Использует тот же Claude-клиент и тот же ANTHROPIC_API_KEY, что
apps.tenant.analytics.ai_service. База знаний (KnowledgeBaseDocument)
подключается как system-контекст — чтобы стиль ответа совпадал с тем,
как ресторан общается с гостями в мини-приложении.

Вызывается по клику кнопки «Сгенерировать ИИ-ответ» в админке.
На автоматический постинг НЕ завязан — ответ редактируется менеджером.
"""
from __future__ import annotations

import logging
import os

from django.conf import settings

logger = logging.getLogger(__name__)


_BASE_SYSTEM_PROMPT = """Ты — менеджер ресторана. Тебе нужно коротко и по-человечески
ответить на публичный отзыв гостя с Яндекс.Карт или 2ГИС.

Правила:
1. Отвечай на русском языке, обращайся на «Вы».
2. Максимум 4–5 предложений. Никаких списков, эмодзи, хештегов.
3. Если отзыв негативный — извинись за конкретную проблему (не общими фразами),
   предложи связаться напрямую (телефон / личное сообщение).
4. Если отзыв позитивный — поблагодари адресно, упомянув то, что гость отметил.
5. Никаких обещаний, которые ты не можешь гарантировать (скидки, компенсации,
   персональные бонусы) — менеджер сам решит, что добавить.
6. Не здоровайся с «Добрый день!» — начинай сразу с обращения по имени, если
   имя есть, иначе со слов благодарности / извинения.

Формат ответа — ровно одна реплика, без кавычек, без подписи, без markdown."""


def _get_knowledge_base_text() -> str:
    """Склеивает текст активных KnowledgeBaseDocument — тот же источник, что и для analytics."""
    try:
        from apps.tenant.analytics.models import KnowledgeBaseDocument
        docs = KnowledgeBaseDocument.objects.filter(is_active=True).exclude(extracted_text='')
        texts = [f'=== {doc.title} ===\n{doc.extracted_text}' for doc in docs]
        return '\n\n'.join(texts) if texts else ''
    except Exception as exc:
        logger.warning('Failed to load KnowledgeBase for reputation: %s', exc)
        return ''


def _build_system_prompt() -> str:
    kb_text = _get_knowledge_base_text()
    if not kb_text:
        return _BASE_SYSTEM_PROMPT
    return (
        _BASE_SYSTEM_PROMPT
        + '\n\n--- Инструкции ресторана из базы знаний ---\n'
        + kb_text
    )


def suggest_reply(*, text: str, rating: int | None = None, author_name: str = '', source: str = '') -> str:
    """
    Возвращает предложенный текст ответа на отзыв.

    Raises:
        RuntimeError — если ANTHROPIC_API_KEY не задан.
    """
    api_key = getattr(settings, 'ANTHROPIC_API_KEY', None)
    if not api_key:
        raise RuntimeError(
            'ANTHROPIC_API_KEY не задан. Добавьте его в .env или settings.py.'
        )

    import anthropic
    proxy_url = os.getenv('AI_PROXY_URL', '')
    client = (
        anthropic.Anthropic(api_key=api_key, base_url=proxy_url)
        if proxy_url else
        anthropic.Anthropic(api_key=api_key)
    )

    meta_parts = []
    if source:
        meta_parts.append(f'Источник: {source}')
    if rating is not None:
        meta_parts.append(f'Оценка: {rating}/5')
    if author_name:
        meta_parts.append(f'Имя гостя: {author_name}')
    meta = '\n'.join(meta_parts)
    user_message = (f'{meta}\n\nТекст отзыва:\n{text}').strip()

    message = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=400,
        system=_build_system_prompt(),
        messages=[{'role': 'user', 'content': user_message}],
    )

    raw = message.content[0].text.strip()
    # На всякий случай — срезаем markdown-обрамление, если модель всё-таки его добавила
    if raw.startswith('```'):
        lines = [ln for ln in raw.splitlines() if not ln.startswith('```')]
        raw = '\n'.join(lines).strip()
    return raw.strip('"').strip("'").strip()
