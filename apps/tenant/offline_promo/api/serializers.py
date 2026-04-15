from datetime import date, timedelta

from rest_framework import serializers


# ── Request ───────────────────────────────────────────────────────────────────

class OfflinePromoStartSerializer(serializers.Serializer):
    """Phase 1: guest scans QR from offline ad and starts the game."""
    vk_id     = serializers.IntegerField()
    branch_id = serializers.IntegerField()
    source    = serializers.CharField(
        required=False, allow_blank=True, default='',
        help_text='Рекламный источник (листовка, баннер и т.д.)',
    )


class OfflinePromoClaimSerializer(serializers.Serializer):
    """Phase 2: guest claims the gift after the animation."""
    session_token = serializers.CharField()
    product_id    = serializers.IntegerField(required=False, allow_null=True)


class OfflinePromoCheckSerializer(serializers.Serializer):
    """Check if the guest already participated."""
    vk_id     = serializers.IntegerField()
    branch_id = serializers.IntegerField()


class OfflineGiftSelectSerializer(serializers.Serializer):
    """Guest selects a specific product for their gift."""
    vk_id      = serializers.IntegerField()
    branch_id  = serializers.IntegerField()
    product_id = serializers.IntegerField()


class OfflineGiftActivateSerializer(serializers.Serializer):
    """Staff activates the gift in café."""
    vk_id               = serializers.IntegerField()
    branch_id            = serializers.IntegerField()
    activation_branch_id = serializers.IntegerField(required=False, allow_null=True)


class OfflineGiftStatusSerializer(serializers.Serializer):
    """Request gift status."""
    vk_id     = serializers.IntegerField()
    branch_id = serializers.IntegerField()


# ── Analytics request ─────────────────────────────────────────────────────────

class NewClientsStatsQuerySerializer(serializers.Serializer):
    """
    Query params for the «Новые клиенты» analytics endpoint.

    Same period presets as the main analytics.
    """
    PERIOD_CHOICES = ['today', '7d', '30d', '90d', 'year', 'all']

    branch_ids = serializers.CharField(required=False, allow_blank=True, default='')
    period     = serializers.ChoiceField(choices=PERIOD_CHOICES, required=False, default='30d')
    start      = serializers.DateField(required=False)
    end        = serializers.DateField(required=False)
    source     = serializers.CharField(required=False, allow_blank=True, default='')

    def validate_branch_ids(self, value: str) -> list[int]:
        if not value:
            return []
        try:
            return [int(x.strip()) for x in value.split(',') if x.strip()]
        except ValueError:
            raise serializers.ValidationError('branch_ids must be comma-separated integers.')

    def validate(self, attrs):
        today = date.today()

        if 'start' in attrs and 'end' in attrs:
            if attrs['start'] > attrs['end']:
                raise serializers.ValidationError('start must be before end.')
            return attrs

        period = attrs.get('period', '30d')
        if period == 'today':
            attrs['start'] = attrs['end'] = today
        elif period == '7d':
            attrs['start'] = today - timedelta(days=6)
            attrs['end']   = today
        elif period == '30d':
            attrs['start'] = today - timedelta(days=29)
            attrs['end']   = today
        elif period == '90d':
            attrs['start'] = today - timedelta(days=89)
            attrs['end']   = today
        elif period == 'year':
            attrs['start'] = today.replace(month=1, day=1)
            attrs['end']   = today
        elif period == 'all':
            attrs['start'] = date(2000, 1, 1)
            attrs['end']   = today

        return attrs


# ── Response ──────────────────────────────────────────────────────────────────

class OfflinePromoSessionSerializer(serializers.Serializer):
    """Phase 1 response."""
    session_token      = serializers.CharField()
    score              = serializers.IntegerField()
    congratulation_text = serializers.CharField(allow_blank=True)
    min_order_text     = serializers.CharField(allow_blank=True, required=False)


class OfflineGiftResponseSerializer(serializers.Serializer):
    """Gift data in API responses."""
    id             = serializers.IntegerField()
    product_name   = serializers.CharField(allow_null=True)
    product_image  = serializers.CharField(allow_null=True, required=False)
    status         = serializers.CharField()
    days_remaining = serializers.IntegerField()
    received_at    = serializers.CharField()
    expires_at     = serializers.CharField()
    activated_at   = serializers.CharField(allow_null=True)
    source         = serializers.CharField(allow_blank=True, required=False)
