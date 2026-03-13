from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import Branch, BranchConfig


@receiver(post_save, sender=Branch)
def create_branch_config(sender, instance, created, **kwargs):
    """Auto-creates an empty BranchConfig whenever a Branch is first saved."""
    if created:
        BranchConfig.objects.get_or_create(branch=instance)
