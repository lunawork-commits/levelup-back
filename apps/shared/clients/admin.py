import re

from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError
from django.urls import reverse
from django.utils.html import format_html

from apps.shared.config.admin_sites import public_admin
from .models import Company, Domain


# ── Root domain helper ────────────────────────────────────────────────────────

def _get_root_domain() -> str:
    """Returns the primary domain of the public-schema tenant."""
    try:
        company = Company.objects.filter(schema_name='public').first()
        if company:
            domain = Domain.objects.filter(tenant=company, is_primary=True).first()
            if domain:
                return domain.domain
    except Exception:
        pass
    return 'localhost'


# ── Subdomain widget & field ──────────────────────────────────────────────────

class SubdomainWidget(forms.TextInput):
    """Renders  [subdomain input].[root_domain]  — only the subdomain is editable."""

    def __init__(self, root_domain: str, *args, **kwargs):
        self.root_domain = root_domain
        super().__init__(*args, **kwargs)

    def format_value(self, value):
        suffix = f'.{self.root_domain}'
        if value and str(value).endswith(suffix):
            return str(value)[: -len(suffix)]
        return value or ''

    def render(self, name, value, attrs=None, renderer=None):
        merged = {
            **(attrs or {}),
            'placeholder': 'поддомен',
            'class': 'vTextField subdomain-input',
        }
        input_html = super().render(name, value, merged, renderer)
        return format_html(
            '<div class="subdomain-wrapper">'
            '{}'
            '<span class="subdomain-suffix">.{}</span>'
            '</div>',
            input_html,
            self.root_domain,
        )


class SubdomainField(forms.CharField):
    def __init__(self, root_domain: str, *args, **kwargs):
        self.root_domain = root_domain
        kwargs.setdefault('widget', SubdomainWidget(root_domain))
        super().__init__(*args, **kwargs)

    def clean(self, value):
        subdomain = super().clean(value)
        if not subdomain:
            return subdomain
        subdomain = subdomain.strip().lower()
        if not re.match(r'^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?$', subdomain):
            raise ValidationError(
                'Только строчные буквы, цифры и дефисы. '
                'Не может начинаться или заканчиваться дефисом.'
            )
        return f'{subdomain}.{self.root_domain}'


# ── Domain inline ─────────────────────────────────────────────────────────────

class DomainForm(forms.ModelForm):
    class Meta:
        model = Domain
        fields = ('domain', 'is_primary')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        root = _get_root_domain()
        self.fields['domain'] = SubdomainField(root_domain=root, label='Поддомен')


class DomainInline(admin.TabularInline):
    model = Domain
    form = DomainForm
    extra = 1
    max_num = 5
    can_delete = True
    verbose_name = 'Домен'
    verbose_name_plural = 'Домены'

    class Media:
        css = {'all': ('admin/clients/css/company_admin.css',)}

    def get_extra(self, request, obj=None, **kwargs):  # noqa: ARG002
        if obj and obj.pk and Domain.objects.filter(tenant=obj).exists():
            return 0
        return 1

    def has_add_permission(self, request, obj=None):
        if getattr(request.user, 'role', None) == 'network_admin':
            return False
        return super().has_add_permission(request, obj)

    def has_change_permission(self, request, obj=None):
        if getattr(request.user, 'role', None) == 'network_admin':
            return False
        return super().has_change_permission(request, obj)

    def has_delete_permission(self, request, obj=None):
        if getattr(request.user, 'role', None) == 'network_admin':
            return False
        return super().has_delete_permission(request, obj)


# ── Company admin ─────────────────────────────────────────────────────────────

@admin.register(Company, site=public_admin)
class CompanyAdmin(admin.ModelAdmin):
    inlines = [DomainInline]
    list_display = ('name', 'client_id', 'schema_name', 'primary_domain', 'is_active', 'paid_until', 'config_link', 'admin_link')
    list_filter = ('is_active',)
    search_fields = ('name', 'schema_name')

    fieldsets = (
        (None, {
            'fields': ('client_id', 'name', 'description'),
        }),
        ('Статус', {
            'fields': ('is_active', 'paid_until'),
        }),
        ('Техническое', {
            'fields': ('schema_name',),
            'description': 'Имя схемы PostgreSQL. Задаётся один раз при создании клиента.',
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request).prefetch_related('domains')
        if getattr(request.user, 'role', None) == 'network_admin':
            return qs.filter(pk__in=request.user.companies.values_list('pk', flat=True))
        return qs

    def get_readonly_fields(self, request, obj=None):
        user = getattr(request, 'user', None)
        if getattr(user, 'role', None) == 'network_admin':
            # NETWORK_ADMIN не управляет биллингом и техническими полями
            return ('schema_name', 'client_id', 'is_active', 'paid_until')
        if obj:
            return ('schema_name',)
        return ()

    def has_add_permission(self, request):
        if getattr(request.user, 'role', None) == 'network_admin':
            return False
        return super().has_add_permission(request)

    def has_delete_permission(self, request, obj=None):
        if getattr(request.user, 'role', None) == 'network_admin':
            return False
        return super().has_delete_permission(request, obj)

    @admin.display(description='Домен')
    def primary_domain(self, obj):
        domain = next((d for d in obj.domains.all() if d.is_primary), None)
        if domain:
            return domain.domain
        return '—'

    @admin.display(description='Настройки')
    def config_link(self, obj):
        if hasattr(obj, 'config'):
            url = reverse('public_admin:config_clientconfig_change', args=[obj.config.pk])
            return format_html('<a href="{}">Настроить →</a>', url)
        return '—'

    @admin.display(description='Перейти')
    def admin_link(self, obj):
        domain = next((d for d in obj.domains.all() if d.is_primary), None)
        if domain:
            url = f'https://{domain.domain}/admin'
            return format_html('<a href="{}" target="_blank">Перейти →</a>', url)
        return '—'
