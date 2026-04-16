from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender='clients.Company')
def create_client_config(sender, instance, created, **kwargs):
    """Автоматически создаёт пустой ClientConfig при создании новой компании."""
    if created:
        from .models import ClientConfig
        ClientConfig.objects.get_or_create(
            company=instance,
            defaults={'vk_group_id': 0, 'vk_group_name': ''},
        )
