from django.contrib import admin, messages
from django.db.models import Count
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils.html import format_html, mark_safe

from apps.shared.config.admin_sites import tenant_admin

from .api.services import call_telegram
from .models import BotAdmin, TelegramBot


# ── BotAdmin inline ───────────────────────────────────────────────────────────

class BotAdminInline(admin.TabularInline):
    model = BotAdmin
    extra = 0
    fields = ('name', 'is_active', 'chat_id', 'connect_button')
    readonly_fields = ('connect_button',)

    @admin.display(description='Подключение')
    def connect_button(self, obj):
        if not obj.pk:
            return '—'
        if obj.chat_id:
            return mark_safe('<span style="color:#417690;font-weight:600;">✓ подключён</span>')
        url = reverse(
            'tenant_admin:telegram_botadmin_connect',
            args=[obj.bot_id, obj.pk],
        )
        return format_html(
            '<a href="{}" class="button"'
            ' style="padding:3px 10px;font-size:12px;white-space:nowrap;">'
            'Подключить</a>',
            url,
        )


# ── TelegramBot admin ─────────────────────────────────────────────────────────

@admin.register(TelegramBot, site=tenant_admin)
class TelegramBotAdmin(admin.ModelAdmin):
    inlines = [BotAdminInline]
    list_display = ('name', 'bot_username_link', 'branch', 'admins_count', 'updated_at')
    list_select_related = ('branch',)
    search_fields = ('name', 'bot_username')
    list_filter = ('branch',)
    actions = ['register_webhook']

    fieldsets = (
        (None, {
            'fields': ('name', 'bot_username', 'branch'),
        }),
        ('Интеграция', {
            'fields': ('api',),
            'description': 'Токен от @BotFather. Никому не передавайте.',
        }),
    )

    @admin.action(description='Зарегистрировать webhook в Telegram')
    def register_webhook(self, request, queryset):
        for bot in queryset:
            webhook_url = request.build_absolute_uri(
                reverse('telegram_webhook', args=[bot.api])
            )
            try:
                result = call_telegram(bot.api, 'setWebhook', {'url': webhook_url})
                if result.get('ok'):
                    self.message_user(
                        request,
                        f'@{bot.bot_username}: webhook зарегистрирован → {webhook_url}',
                        messages.SUCCESS,
                    )
                else:
                    self.message_user(
                        request,
                        f'@{bot.bot_username}: ошибка Telegram — {result.get("description")}',
                        messages.ERROR,
                    )
            except Exception as exc:
                self.message_user(
                    request,
                    f'@{bot.bot_username}: ошибка соединения — {exc}',
                    messages.ERROR,
                )

    def get_queryset(self, request):
        return super().get_queryset(request).annotate(
            admins_count=Count('admins'),
        )

    @admin.display(description='Username', ordering='bot_username')
    def bot_username_link(self, obj):
        return format_html(
            '<a href="https://t.me/{0}" target="_blank">@{0}</a>',
            obj.bot_username,
        )

    @admin.display(description='Администраторы', ordering='admins_count')
    def admins_count(self, obj):
        count = obj.admins_count
        if not count:
            return '—'
        url = (
            reverse('tenant_admin:telegram_botadmin_changelist')
            + f'?bot__id__exact={obj.pk}'
        )
        return format_html('<a href="{}">{} адм.</a>', url, count)

    # ── Custom URLs ───────────────────────────────────────────────────────────

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path(
                '<int:bot_pk>/connect/<int:admin_pk>/',
                self.admin_site.admin_view(self.connect_view),
                name='telegram_botadmin_connect',
            ),
        ]
        return custom + urls

    def connect_view(self, request, bot_pk, admin_pk):
        bot = get_object_or_404(TelegramBot, pk=bot_pk)
        bot_admin_obj = get_object_or_404(BotAdmin, pk=admin_pk, bot=bot)

        expected_webhook_url = request.build_absolute_uri(
            reverse('telegram_webhook', args=[bot.api])
        )

        # Handle inline webhook registration POST
        if request.method == 'POST' and 'register_webhook' in request.POST:
            try:
                result = call_telegram(bot.api, 'setWebhook', {'url': expected_webhook_url})
                if result.get('ok'):
                    self.message_user(request, 'Webhook зарегистрирован.', messages.SUCCESS)
                else:
                    self.message_user(
                        request,
                        f'Ошибка Telegram: {result.get("description")}',
                        messages.ERROR,
                    )
            except Exception as exc:
                self.message_user(request, f'Ошибка соединения: {exc}', messages.ERROR)
            return HttpResponseRedirect(request.path)

        # Fetch current webhook info from Telegram
        webhook_info = None
        try:
            webhook_info = call_telegram(bot.api, 'getWebhookInfo')
        except Exception:
            pass

        context = {
            **self.admin_site.each_context(request),
            'opts': self.model._meta,
            'bot': bot,
            'bot_admin': bot_admin_obj,
            'title': 'Подключение администратора',
            'original': bot,
            'expected_webhook_url': expected_webhook_url,
            'webhook_info': webhook_info,
        }
        return TemplateResponse(request, 'admin/telegram/connect.html', context)


# ── BotAdmin standalone (read-only, for filtering) ───────────────────────────

@admin.register(BotAdmin, site=tenant_admin)
class BotAdminAdmin(admin.ModelAdmin):
    list_display = ('name', 'bot', 'chat_id_display', 'is_active', 'created_at')
    list_filter = ('bot', 'is_active')
    search_fields = ('name', 'bot__bot_username')
    readonly_fields = ('verification_token', 'created_at', 'updated_at')

    fieldsets = (
        (None, {
            'fields': ('bot', 'name', 'is_active'),
        }),
        ('Подключение', {
            'fields': ('chat_id', 'verification_token'),
            'description': 'Chat ID заполняется автоматически после верификации.',
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )

    @admin.display(description='Chat ID')
    def chat_id_display(self, obj):
        if obj.chat_id:
            return format_html(
                '<span style="color:#417690;font-weight:600;">✓ {}</span>',
                obj.chat_id,
            )
        return mark_safe('<span style="color:#999;">не подключён</span>')
