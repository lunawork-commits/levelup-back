"""
Тонкая бизнес-прослойка для reputation API.

Удерживаем здесь всё, что не должно жить во views — мутации моделей,
транзакции и вызов AI-сервиса. Views остаются request/response.
"""
from __future__ import annotations

from django.db import transaction
from django.utils import timezone

from apps.tenant.reputation.models import ExternalReview, ReviewStatus


@transaction.atomic
def mark_seen(review: ExternalReview) -> ExternalReview:
    """NEW → SEEN. Идемпотентно."""
    if review.status == ReviewStatus.NEW:
        review.status = ReviewStatus.SEEN
        review.save(update_fields=['status', 'updated_at'])
    return review


@transaction.atomic
def ignore(review: ExternalReview) -> ExternalReview:
    review.status = ReviewStatus.IGNORED
    review.save(update_fields=['status', 'updated_at'])
    return review


@transaction.atomic
def save_reply(review: ExternalReview, *, reply_text: str, user) -> ExternalReview:
    """
    Сохраняет подготовленный менеджером текст ответа.
    Не публикует его на площадке — публикация вручную через deep-link
    (до того как подключим partner.api.2gis).
    """
    review.reply_text = reply_text
    review.replied_at = timezone.now()
    review.replied_by = user if getattr(user, 'is_authenticated', False) else None
    review.status = ReviewStatus.ANSWERED
    review.save(update_fields=['reply_text', 'replied_at', 'replied_by', 'status', 'updated_at'])
    return review


def generate_reply_suggestion(review: ExternalReview) -> str:
    """Вызов AI-сервиса. Исключения пробрасываются — view превратит их в 502."""
    from apps.tenant.reputation.ai_service import suggest_reply

    return suggest_reply(
        text=review.text,
        rating=review.rating,
        author_name=review.author_name,
        source=review.get_source_display(),
    )
