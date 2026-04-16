from django.utils import timezone
from rest_framework import serializers


# ── Request ───────────────────────────────────────────────────────────────────

class QuestRequestSerializer(serializers.Serializer):
    vk_id     = serializers.IntegerField()
    branch_id = serializers.IntegerField()


class QuestActivateSerializer(serializers.Serializer):
    vk_id     = serializers.IntegerField()
    branch_id = serializers.IntegerField()
    quest_id  = serializers.IntegerField()


class QuestSubmitInputSerializer(serializers.Serializer):
    vk_id       = serializers.IntegerField()
    branch_id   = serializers.IntegerField()
    quest_id    = serializers.IntegerField()
    code        = serializers.CharField()
    employee_id = serializers.IntegerField(required=False, allow_null=True, default=None)


# ── Response ──────────────────────────────────────────────────────────────────

class QuestListItemSerializer(serializers.Serializer):
    """Quest with a per-guest completion flag (item in get_quests list)."""
    id          = serializers.IntegerField(source='quest.pk')
    name        = serializers.CharField(source='quest.name')
    description = serializers.CharField(source='quest.description')
    reward      = serializers.IntegerField(source='quest.reward')
    completed   = serializers.BooleanField()


class QuestSubmitSerializer(serializers.Serializer):
    """Active quest submit state returned to the frontend."""
    id                = serializers.IntegerField()
    quest_id          = serializers.IntegerField(source='quest.pk')
    quest_name        = serializers.CharField(source='quest.name')
    quest_description = serializers.CharField(source='quest.description')
    quest_reward      = serializers.IntegerField(source='quest.reward')
    status            = serializers.CharField()
    activated_at      = serializers.DateTimeField()
    duration          = serializers.IntegerField()
    expires_at        = serializers.DateTimeField()
    seconds_remaining = serializers.SerializerMethodField()

    def get_seconds_remaining(self, obj) -> int:
        if obj.status != 'pending':
            return 0
        remaining = obj.expires_at - timezone.now()
        return max(0, int(remaining.total_seconds()))


class QuestCooldownSerializer(serializers.Serializer):
    """QUEST cooldown state for a guest."""
    is_active         = serializers.BooleanField()
    expires_at        = serializers.DateTimeField()
    seconds_remaining = serializers.SerializerMethodField()

    def get_seconds_remaining(self, obj) -> int:
        remaining = obj.remaining
        return int(remaining.total_seconds()) if remaining else 0
