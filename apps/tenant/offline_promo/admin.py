from django.contrib import admin
from django.utils.html import format_html

from apps.shared.config.admin_sites import tenant_admin

from .models import (
    OfflinePromoScan,
    OfflinePromoGift,
    OfflinePromoConfigModel,
    GiftStatus,
)


# ── OfflinePromoScan ─────────────────────────────────────────────────────────

@admin.register(OfflinePromoScan, site=tenant_admin)
class OfflinePromoScanAdmin(admin.ModelAdmin):
    list_display = (
        'client',
        'source_display',
        'ip_address',
        'created_at',
    )
    list_filter = ('source', 'created_at')
    search_fields = (
        'client__client__first_name',
        'client__client__last_name',
        'client__client__vk_id',
        'source',
    )
    readonly_fields = ('client', 'source', 'ip_address', 'created_at')
    date_hierarchy = 'created_at'
    ordering = ('-created_at',)

    def source_display(self, obj):
        return obj.source or '—'
    source_display.short_description = 'Источник'

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


# ── OfflinePromoGift ─────────────────────────────────────────────────────────

@admin.register(OfflinePromoGift, site=tenant_admin)
class OfflinePromoGiftAdmin(admin.ModelAdmin):
    list_display = (
        'client',
        'product',
        'status_badge',
        'source_display',
        'received_at',
        'expires_at',
        'activated_at',
        'activated_branch',
    )
    list_filter = ('source', 'activated_branch')
    search_fields = (
        'client__client__first_name',
        'client__client__last_name',
        'client__client__vk_id',
        'product__name',
        'source',
    )
    readonly_fields = (
        'client', 'product', 'source', 'congratulation_text',
        'received_at', 'expires_at', 'activated_at', 'activated_branch',
        'status_badge',
    )
    date_hierarchy = 'received_at'
    ordering = ('-created_at',)

    fieldsets = (
        ('Гость и подарок', {
            'fields': ('client', 'product', 'source'),
        }),
        ('Статус', {
            'fields': ('status_badge', 'received_at', 'expires_at', 'activated_at', 'activated_branch'),
        }),
        ('Настройки', {
            'fields': ('congratulation_text',),
            'classes': ('collapse',),
        }),
    )

    def status_badge(self, obj):
        status_val = obj.status
        colors = {
            GiftStatus.ACTIVE: '#28a745',
            GiftStatus.ACTIVATED: '#007bff',
            GiftStatus.EXPIRED: '#dc3545',
            GiftStatus.RECEIVED: '#ffc107',
        }
        labels = {
            GiftStatus.ACTIVE: '🟢 Активен',
            GiftStatus.ACTIVATED: '✅ Активирован',
            GiftStatus.EXPIRED: '🔴 Истёк',
            GiftStatus.RECEIVED: '🟡 Получен',
        }
        color = colors.get(status_val, '#6c757d')
        label = labels.get(status_val, status_val)
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color, label,
        )
    status_badge.short_description = 'Статус'

    def source_display(self, obj):
        return obj.source or '—'
    source_display.short_description = 'Источник'

    def has_add_permission(self, request):
        return False


# ── OfflinePromoConfig ───────────────────────────────────────────────────────

@admin.register(OfflinePromoConfigModel, site=tenant_admin)
class OfflinePromoConfigAdmin(admin.ModelAdmin):
    list_display = ('branch', 'is_enabled', 'gift_validity_days')
    list_filter = ('is_enabled',)
    search_fields = ('branch__name',)

    fieldsets = (
        ('Точка', {
            'fields': ('branch', 'is_enabled'),
        }),
        ('Текст поздравления', {
            'fields': ('congratulation_text', 'min_order_text'),
        }),
        ('Настройки подарка', {
            'fields': ('gift_validity_days',),
        }),
    )
