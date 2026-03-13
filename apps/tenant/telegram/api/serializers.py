from rest_framework import serializers


class TelegramChatSerializer(serializers.Serializer):
    id = serializers.IntegerField()


class TelegramMessageSerializer(serializers.Serializer):
    message_id = serializers.IntegerField()
    chat = TelegramChatSerializer()
    text = serializers.CharField(required=False, default='', allow_blank=True)


class TelegramUpdateSerializer(serializers.Serializer):
    """
    Validates the top-level Telegram Update object.
    Only declares fields we care about; extra fields are ignored by DRF.
    Both message and edited_message are optional — other update types
    (callback_query, inline_query, etc.) are silently skipped.
    """

    update_id = serializers.IntegerField()
    message = TelegramMessageSerializer(required=False, allow_null=True, default=None)
    edited_message = TelegramMessageSerializer(required=False, allow_null=True, default=None)
