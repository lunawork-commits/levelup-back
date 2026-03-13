from django.contrib import admin
from django.utils.html import format_html, mark_safe

from apps.shared.config.admin_sites import tenant_admin

from .models import ClientAttempt

# ── Style constants ───────────────────────────────────────────────────────────

_BADGE = (
    'display:inline-block;padding:2px 8px;border-radius:10px;'
    'font-size:11px;font-weight:600;white-space:nowrap;'
)
_EMPLOYEE_STYLE = _BADGE + 'background:#e8f5e9;color:#1b5e20;border:1px solid #c8e6c9;'
_NONE_STYLE = 'color:var(--body-quiet-color,#aaa);font-size:12px;'


# ── Filters ───────────────────────────────────────────────────────────────────

class ServedByFilter(admin.SimpleListFilter):
    title = 'Официант'
    parameter_name = 'has_served_by'

    def lookups(self, request, model_admin):
        return [
            ('yes', 'Указан'),
            ('no',  'Не указан'),
        ]

    def queryset(self, request, queryset):
        if self.value() == 'yes':
            return queryset.filter(served_by__isnull=False)
        if self.value() == 'no':
            return queryset.filter(served_by__isnull=True)
        return queryset


# ── ClientAttempt admin ───────────────────────────────────────────────────────

@admin.register(ClientAttempt, site=tenant_admin)
class ClientAttemptAdmin(admin.ModelAdmin):
    list_display = (
        'client_col', 'branch_col', 'served_by_col', 'created_at',
    )
    list_display_links = ('client_col',)
    list_filter = (
        ServedByFilter,
        'client__branch',
    )
    search_fields = (
        'client__client__first_name',
        'client__client__last_name',
        'served_by__client__first_name',
        'served_by__client__last_name',
    )
    list_select_related = (
        'client__client',
        'client__branch',
        'served_by__client',
    )
    date_hierarchy = 'created_at'
    readonly_fields = ('created_at', 'updated_at')

    fieldsets = (
        (None, {
            'fields': ('client', 'served_by'),
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )

    # ── Queryset ──────────────────────────────────────────────────────────

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'client__client',
            'client__branch',
            'served_by__client',
        )

    # ── List columns ──────────────────────────────────────────────────────

    @admin.display(description='Гость', ordering='client__client__first_name')
    def client_col(self, obj):
        c = obj.client.client
        return f'{c.first_name} {c.last_name}'.strip() or f'vk{c.vk_id}'

    @admin.display(description='Точка', ordering='client__branch__name')
    def branch_col(self, obj):
        return obj.client.branch.name

    @admin.display(description='Официант', ordering='served_by__client__first_name')
    def served_by_col(self, obj):
        if not obj.served_by_id:
            return mark_safe(f'<span style="{_NONE_STYLE}">—</span>')
        c = obj.served_by.client
        label = f'{c.first_name} {c.last_name}'.strip() or f'vk{c.vk_id}'
        return format_html('<span style="{}">👤 {}</span>', _EMPLOYEE_STYLE, label)
