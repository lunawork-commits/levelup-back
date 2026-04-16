from django.contrib import admin
from django.utils.html import format_html

from apps.shared.config.admin_sites import tenant_admin

from .models import Quest, QuestSubmit


# ── Quest admin ────────────────────────────────────────────────────────────────

@admin.register(Quest, site=tenant_admin)
class QuestAdmin(admin.ModelAdmin):
    list_display  = ('name', 'branch', 'reward_badge', 'is_active', 'ordering', 'updated_at')
    list_display_links = ('name',)
    list_filter   = ('branch', 'is_active')
    search_fields = ('name', 'description')
    list_editable = ('is_active', 'ordering')
    list_select_related = ('branch',)
    ordering = ('ordering', 'name')

    fieldsets = (
        (None, {
            'fields': ('branch', 'name', 'description'),
        }),
        ('Условия', {
            'fields': ('reward', 'is_active', 'ordering'),
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )
    readonly_fields = ('created_at', 'updated_at')

    @admin.display(description='Награда', ordering='reward')
    def reward_badge(self, obj):
        return format_html(
            '<span style="display:inline-block;padding:2px 8px;border-radius:10px;'
            'font-size:11px;font-weight:600;background:#e3f2fd;color:#0d47a1;'
            'border:1px solid #bbdefb;">{} ★</span>',
            obj.reward,
        )


# ── QuestSubmit admin ──────────────────────────────────────────────────────────

@admin.register(QuestSubmit, site=tenant_admin)
class QuestSubmitAdmin(admin.ModelAdmin):
    list_display  = ('client_col', 'quest', 'status_badge', 'activated_at', 'expires_at', 'served_by_col', 'completed_at')
    list_display_links = ('client_col',)
    list_filter   = ('quest__branch', 'quest')
    search_fields = ('client__client__first_name', 'client__client__last_name', 'quest__name')
    list_select_related = ('client__client', 'quest__branch', 'served_by__client')
    date_hierarchy = 'activated_at'
    readonly_fields = (
        'client', 'quest', 'served_by',
        'activated_at', 'duration', 'expires_at', 'completed_at',
        'created_at', 'updated_at',
    )

    fieldsets = (
        (None, {
            'fields': ('client', 'quest', 'served_by'),
        }),
        ('Таймер', {
            'fields': ('activated_at', 'duration', 'expires_at', 'completed_at'),
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    # ── List columns ──────────────────────────────────────────────────────────

    @admin.display(description='Гость', ordering='client__client__first_name')
    def client_col(self, obj):
        c = obj.client.client
        full = f'{c.first_name} {c.last_name}'.strip()
        return full or f'vk{c.vk_id}'

    @admin.display(description='Официант')
    def served_by_col(self, obj):
        if not obj.served_by:
            return '—'
        c = obj.served_by.client
        return f'{c.first_name} {c.last_name}'.strip() or f'vk{c.vk_id}'

    @admin.display(description='Статус')
    def status_badge(self, obj):
        s = obj.status
        styles = {
            'pending':  ('background:#fff3cd;color:#856404;border:1px solid #ffe08a;', 'В процессе'),
            'complete': ('background:#e8f5e9;color:#1b5e20;border:1px solid #c8e6c9;', 'Выполнен'),
            'expired':  ('background:#fce4ec;color:#880e4f;border:1px solid #f8bbd0;', 'Истёк'),
        }
        style, label = styles.get(s, ('', s))
        return format_html(
            '<span style="display:inline-block;padding:2px 8px;border-radius:10px;'
            'font-size:11px;font-weight:600;white-space:nowrap;{}">{}</span>',
            style, label,
        )
