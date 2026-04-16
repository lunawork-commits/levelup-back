from rest_framework import serializers

from apps.shared.config.models import LandingSettings


class LandingSettingsSerializer(serializers.ModelSerializer):
    """Публичное представление LandingSettings — только URL'ы файлов, без file-объектов."""

    video_url  = serializers.CharField(read_only=True)
    poster_url = serializers.CharField(read_only=True)

    class Meta:
        model = LandingSettings
        fields = (
            'is_enabled',
            'button_label',
            'title',
            'description',
            'video_url',
            'poster_url',
            'cta_label',
            'cta_url',
            'updated_at',
        )
        read_only_fields = fields
