from rest_framework import serializers

from apps.tenant.reputation.models import ExternalReview, ReputationSyncState, ReviewSource


class ExternalReviewSerializer(serializers.ModelSerializer):
    branch_name = serializers.CharField(source='branch.name', read_only=True)
    source_label = serializers.CharField(source='get_source_display', read_only=True)
    status_label = serializers.CharField(source='get_status_display', read_only=True)
    reply_deeplink = serializers.SerializerMethodField()

    class Meta:
        model = ExternalReview
        fields = (
            'id',
            'branch', 'branch_name',
            'source', 'source_label',
            'external_id',
            'author_name', 'rating', 'text', 'published_at',
            'status', 'status_label',
            'reply_text', 'replied_at',
            'reply_deeplink',
            'fetched_at',
        )
        read_only_fields = fields

    def get_reply_deeplink(self, obj: ExternalReview) -> str:
        """Ссылка, которую открывает кнопка «Ответить на Яндекс/2ГИС» в новой вкладке."""
        config = getattr(obj.branch, 'config', None)
        if config is None:
            return ''
        if obj.source == ReviewSource.YANDEX:
            return config.yandex_map or ''
        if obj.source == ReviewSource.GIS:
            base = (config.gis_map or '').rstrip('/')
            return f'{base}/tab/reviews' if base else ''
        return ''


class SaveReplySerializer(serializers.Serializer):
    reply_text = serializers.CharField(max_length=4000, allow_blank=False)


class SyncSerializer(serializers.Serializer):
    branch_id = serializers.IntegerField()
    source = serializers.ChoiceField(
        choices=ReviewSource.choices,
        required=False,
        help_text='Если не указан — синхронизируются оба источника.',
    )


class ReputationSyncStateSerializer(serializers.ModelSerializer):
    branch_name = serializers.CharField(source='branch.name', read_only=True)
    source_label = serializers.CharField(source='get_source_display', read_only=True)

    class Meta:
        model = ReputationSyncState
        fields = (
            'id', 'branch', 'branch_name', 'source', 'source_label',
            'last_run_at', 'last_ok_at', 'last_error', 'reviews_fetched',
        )
        read_only_fields = fields
