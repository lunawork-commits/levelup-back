from rest_framework import serializers


# ── Request ────────────────────────────────────────────────────────────────────

class CatalogRequestSerializer(serializers.Serializer):
    branch_id = serializers.IntegerField()


class CooldownRequestSerializer(serializers.Serializer):
    vk_id     = serializers.IntegerField()
    branch_id = serializers.IntegerField()


class BuyRequestSerializer(serializers.Serializer):
    vk_id      = serializers.IntegerField()
    branch_id  = serializers.IntegerField()
    product_id = serializers.IntegerField()


# ── Response ───────────────────────────────────────────────────────────────────

class ProductSerializer(serializers.Serializer):
    """Single product in the catalog."""
    id               = serializers.IntegerField()
    name             = serializers.CharField()
    description      = serializers.CharField()
    image_url        = serializers.SerializerMethodField()
    price            = serializers.IntegerField()
    is_super_prize   = serializers.BooleanField()
    is_birthday_prize = serializers.BooleanField()
    category_id      = serializers.IntegerField(source='category.id', allow_null=True)
    category_name    = serializers.SerializerMethodField()

    def get_image_url(self, obj) -> str | None:
        return obj.image.url if (obj.image and obj.image.name) else None

    def get_category_name(self, obj) -> str | None:
        return obj.category.name if obj.category else None


class CooldownResponseSerializer(serializers.Serializer):
    """SHOP cooldown status for a guest."""
    is_active         = serializers.BooleanField()
    expires_at        = serializers.DateTimeField()
    seconds_remaining = serializers.SerializerMethodField()

    def get_seconds_remaining(self, obj) -> int:
        remaining = obj.remaining
        return int(remaining.total_seconds()) if remaining else 0


class BuyResponseSerializer(serializers.Serializer):
    """InventoryItem created after a successful purchase."""
    id                = serializers.IntegerField()
    product_id        = serializers.IntegerField(source='product.pk')
    product_name      = serializers.CharField(source='product.name')
    product_image_url = serializers.SerializerMethodField()
    price             = serializers.IntegerField(source='product.price')
    acquired_from     = serializers.CharField()
    status            = serializers.CharField()
    duration          = serializers.IntegerField()
    activated_at      = serializers.DateTimeField(allow_null=True)
    expires_at        = serializers.DateTimeField(allow_null=True)

    def get_product_image_url(self, obj) -> str | None:
        img = obj.product.image
        return img.url if (img and img.name) else None
