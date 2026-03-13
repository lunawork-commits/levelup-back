from django.contrib import admin
from django.db.models import Count
from django.utils.html import format_html, mark_safe

from apps.shared.config.admin_sites import tenant_admin

from .models import Product, ProductCategory


# ── Style constants ───────────────────────────────────────────────────────────

_BADGE = (
    'display:inline-block;padding:2px 8px;border-radius:10px;'
    'font-size:11px;font-weight:600;white-space:nowrap;'
)
_SUPER_STYLE = _BADGE + 'background:#fff3cd;color:#856404;border:1px solid #ffe08a;'
_BDAY_STYLE  = _BADGE + 'background:#fce4ec;color:#880e4f;border:1px solid #f8bbd0;'
_PRICE_STYLE = _BADGE + 'background:#e3f2fd;color:#0d47a1;border:1px solid #bbdefb;'
_FREE_STYLE  = _BADGE + 'background:#e8f5e9;color:#1b5e20;border:1px solid #c8e6c9;'


# ── ProductCategory admin ─────────────────────────────────────────────────────

class ProductInline(admin.TabularInline):
    model = Product
    extra = 0
    fields = ('name', 'price', 'is_active', 'is_super_prize', 'is_birthday_prize', 'ordering')
    ordering = ('ordering', 'name')
    show_change_link = True


@admin.register(ProductCategory, site=tenant_admin)
class ProductCategoryAdmin(admin.ModelAdmin):
    inlines = [ProductInline]
    list_display = ('name', 'branch', 'products_count', 'ordering', 'updated_at')
    list_filter = ('branch',)
    search_fields = ('name',)
    list_select_related = ('branch',)

    def get_queryset(self, request):
        return super().get_queryset(request).annotate(
            products_count=Count('products'),
        )

    @admin.display(description='Товаров', ordering='products_count')
    def products_count(self, obj):
        return obj.products_count or '—'


# ── Product admin ─────────────────────────────────────────────────────────────

@admin.register(Product, site=tenant_admin)
class ProductAdmin(admin.ModelAdmin):
    list_display = (
        'image_thumb', 'name', 'branch', 'category',
        'price_badge', 'flags_badges', 'is_active', 'ordering', 'updated_at',
    )
    list_display_links = ('image_thumb', 'name')
    list_filter = ('branch', 'category', 'is_active', 'is_super_prize', 'is_birthday_prize')
    search_fields = ('name', 'description')
    list_select_related = ('branch', 'category')
    list_editable = ('is_active', 'ordering')
    actions = [
        'activate_products', 'deactivate_products',
        'mark_super_prize', 'unmark_super_prize',
        'mark_birthday_prize', 'unmark_birthday_prize',
    ]
    readonly_fields = ('image_preview', 'created_at', 'updated_at')

    fieldsets = (
        (None, {
            'fields': ('branch', 'category', 'name', 'description'),
        }),
        ('Изображение', {
            'fields': ('image', 'image_preview'),
        }),
        ('Параметры', {
            'fields': ('price', 'ordering', 'is_active'),
        }),
        ('Сценарии выдачи', {
            'fields': ('is_super_prize', 'is_birthday_prize'),
            'description': (
                'Флаги не взаимоисключающие. '
                'Товар без флагов доступен только для покупки за баллы.'
            ),
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )

    # ── Queryset ──────────────────────────────────────────────────────────

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('branch', 'category')

    # ── List columns ──────────────────────────────────────────────────────

    @admin.display(description='')
    def image_thumb(self, obj):
        if obj.image:
            return format_html(
                '<img src="{}" style="width:44px;height:44px;'
                'object-fit:cover;border-radius:6px;'
                'border:1px solid var(--border-color,#ddd);" />',
                obj.image.url,
            )
        return mark_safe(
            '<div style="width:44px;height:44px;border-radius:6px;'
            'background:var(--darkened-bg,#f0f0f0);'
            'border:1px solid var(--border-color,#ddd);'
            'display:flex;align-items:center;justify-content:center;'
            'font-size:20px;">🎁</div>'
        )

    @admin.display(description='Цена', ordering='price')
    def price_badge(self, obj):
        if obj.price == 0:
            return format_html('<span style="{}">Бесплатно</span>', _FREE_STYLE)
        return format_html('<span style="{}">{} ★</span>', _PRICE_STYLE, obj.price)

    @admin.display(description='Флаги')
    def flags_badges(self, obj):
        badges = []
        if obj.is_super_prize:
            badges.append(f'<span style="{_SUPER_STYLE}">🏆 Суперприз</span>')
        if obj.is_birthday_prize:
            badges.append(f'<span style="{_BDAY_STYLE}">🎂 День рождения</span>')
        if badges:
            return mark_safe('&nbsp;'.join(badges))
        return mark_safe(
            '<span style="color:var(--body-quiet-color,#aaa);font-size:12px;">—</span>'
        )

    @admin.display(description='Фото')
    def image_preview(self, obj):
        if obj.image:
            return format_html(
                '<img src="{}" style="max-width:280px;max-height:280px;'
                'border-radius:8px;border:1px solid var(--border-color,#ddd);" />',
                obj.image.url,
            )
        return '—'

    # ── Actions ───────────────────────────────────────────────────────────

    @admin.action(description='Активировать')
    def activate_products(self, request, queryset):
        self.message_user(request, f'Активировано: {queryset.update(is_active=True)}')

    @admin.action(description='Деактивировать')
    def deactivate_products(self, request, queryset):
        self.message_user(request, f'Деактивировано: {queryset.update(is_active=False)}')

    @admin.action(description='Добавить флаг «Суперприз»')
    def mark_super_prize(self, request, queryset):
        self.message_user(request, f'Суперприз: {queryset.update(is_super_prize=True)}')

    @admin.action(description='Убрать флаг «Суперприз»')
    def unmark_super_prize(self, request, queryset):
        self.message_user(request, f'Флаг снят: {queryset.update(is_super_prize=False)}')

    @admin.action(description='Добавить флаг «Подарок на ДР»')
    def mark_birthday_prize(self, request, queryset):
        self.message_user(request, f'Подарок на ДР: {queryset.update(is_birthday_prize=True)}')

    @admin.action(description='Убрать флаг «Подарок на ДР»')
    def unmark_birthday_prize(self, request, queryset):
        self.message_user(request, f'Флаг снят: {queryset.update(is_birthday_prize=False)}')
