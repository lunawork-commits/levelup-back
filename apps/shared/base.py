from django.db import models


class TimeStampedModel(models.Model):
    """Abstract base model that adds created_at / updated_at to every subclass."""

    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Создано')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='Обновлено')

    class Meta:
        abstract = True
