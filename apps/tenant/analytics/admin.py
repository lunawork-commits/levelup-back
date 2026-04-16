from django.contrib import admin, messages
from django.http import HttpResponse, HttpResponseRedirect
from django.urls import path, reverse
from django.utils.html import format_html, mark_safe

from apps.shared.config.admin_sites import tenant_admin

from .models import BranchSegmentSnapshot, GuestRFScore, KnowledgeBaseDocument, RFMigrationLog, RFSegment, RFSettings

# ── Style constants ───────────────────────────────────────────────────────────

_BADGE = (
    'display:inline-block;padding:2px 8px;border-radius:10px;'
    'font-size:11px;font-weight:600;white-space:nowrap;'
)


def _segment_badge(seg):
    if not seg:
        return mark_safe('<span style="color:var(--body-quiet-color,#aaa);">—</span>')
    return format_html(
        '<span style="background:{};color:#fff;padding:2px 10px;'
        'border-radius:12px;font-size:11px;font-weight:700;">{} {}</span>',
        seg.color, seg.emoji, seg.name,
    )


# ── RFSegment admin ───────────────────────────────────────────────────────────

@admin.register(RFSegment, site=tenant_admin)
class RFSegmentAdmin(admin.ModelAdmin):
    list_display = (
        'code_badge', 'name', 'recency_range_col',
        'frequency_range_col', 'guests_count_col', 'hint_preview_col',
        'last_campaign_date',
    )
    list_display_links = ('code_badge',)
    search_fields = ('code', 'name')
    readonly_fields = ('created_at', 'updated_at', 'guests_count_col')

    fieldsets = (
        (None, {
            'fields': ('code', 'name', 'emoji', 'color'),
        }),
        ('RF-границы', {
            'fields': (('recency_min', 'recency_max'), ('frequency_min', 'frequency_max')),
            'description': (
                'Гость попадает в этот сегмент, если его давность и частота '
                'ОДНОВРЕМЕННО попадают в указанные диапазоны.'
            ),
        }),
        ('Маркетинг', {
            'fields': ('strategy', 'hint', 'last_campaign_date'),
            'description': (
                '<b>Подсказка</b> — краткая инструкция менеджеру по работе с рассылками для этого сегмента. '
                'Отображается в таблице сегментов и помогает принять правильное решение.'
            ),
        }),
        ('Статистика', {
            'fields': ('guests_count_col',),
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )

    # ── Custom URLs ───────────────────────────────────────────────────────────

    def get_urls(self):
        urls = super().get_urls()
        return [
            path(
                '<int:pk>/send-broadcast/',
                self.admin_site.admin_view(self._send_broadcast_view),
                name='analytics_rfsegment_send_broadcast',
            ),
            path(
                '<int:pk>/export-senler/',
                self.admin_site.admin_view(self._export_senler_view),
                name='analytics_rfsegment_export_senler',
            ),
        ] + urls

    def _send_broadcast_view(self, request, pk):
        """
        Создаёт рассылку по всем гостям сегмента для каждой активной торговой точки
        и перенаправляет на страницу новой рассылки для редактирования текста.
        """
        from apps.tenant.branch.models import Branch
        from apps.tenant.senler.models import Broadcast

        segment = RFSegment.objects.get(pk=pk)
        branches = Branch.objects.filter(is_active=True)

        created_ids = []
        for branch in branches:
            broadcast = Broadcast.objects.create(
                branch=branch,
                name=f'Рассылка: {segment.emoji} {segment.name} ({segment.code})',
                message_text='',
                audience_type='all',
            )
            broadcast.rf_segments.set([segment])
            created_ids.append(broadcast.pk)

        if len(created_ids) == 1:
            self.message_user(
                request,
                f'Создана рассылка для сегмента «{segment.name}». '
                f'Заполните текст сообщения и нажмите «Отправить».',
                level=messages.SUCCESS,
            )
            return HttpResponseRedirect(
                reverse('admin:senler_broadcast_change', args=[created_ids[0]])
            )
        elif created_ids:
            self.message_user(
                request,
                f'Создано {len(created_ids)} рассылок (по одной на точку) для сегмента «{segment.name}». '
                f'Перейдите в раздел Рассылки, заполните тексты и отправьте.',
                level=messages.SUCCESS,
            )
            return HttpResponseRedirect(reverse('admin:senler_broadcast_changelist'))
        else:
            self.message_user(
                request,
                'Нет активных торговых точек.',
                level=messages.WARNING,
            )
            return HttpResponseRedirect(reverse('admin:analytics_rfsegment_changelist'))

    def _export_senler_view(self, request, pk):
        """
        Выгружает .txt файл с VK ID всех гостей сегмента — по одному на строку.
        Формат для импорта в Senler (конструктор чат-ботов ВКонтакте).
        Supports ?mode=delivery and ?branches=1,2,3 query params.
        """
        from apps.tenant.analytics.models import GuestRFScoreDelivery
        from urllib.parse import quote

        segment = RFSegment.objects.get(pk=pk)
        mode = request.GET.get('mode', 'restaurant')

        ScoreModel = GuestRFScoreDelivery if mode == 'delivery' else GuestRFScore
        qs = ScoreModel.objects.filter(segment=segment)

        branch_ids = request.GET.get('branches', '')
        if branch_ids:
            try:
                ids = [int(x) for x in branch_ids.split(',') if x.strip()]
                qs = qs.filter(client__branch_id__in=ids)
            except ValueError:
                pass

        # Get all VK IDs for guests in this segment
        vk_ids = qs.select_related('client__client').values_list(
            'client__client__vk_id', flat=True,
        )

        # Filter out None values and build the file content
        lines = [str(vk_id) for vk_id in vk_ids if vk_id]
        content = '\n'.join(lines)

        safe_name = f'senler_{segment.code}.txt'
        full_name = f'senler_{segment.code}_{segment.name}.txt'
        response = HttpResponse(content, content_type='text/plain; charset=utf-8')
        response['Content-Disposition'] = (
            f'attachment; filename="{safe_name}"; '
            f"filename*=UTF-8''{quote(full_name)}"
        )
        return response

    # ── Display columns ───────────────────────────────────────────────────────

    @admin.display(description='Код')
    def code_badge(self, obj):
        return format_html(
            '<span style="background:{};color:#fff;padding:3px 12px;'
            'border-radius:12px;font-weight:700;font-size:13px;letter-spacing:1px;">'
            '{} {}</span>',
            obj.color, obj.emoji, obj.code,
        )

    @admin.display(description='Давность (дни)', ordering='recency_min')
    def recency_range_col(self, obj):
        return format_html(
            '<span style="font-family:monospace;">{}&thinsp;–&thinsp;{}</span>',
            obj.recency_min, obj.recency_max,
        )

    @admin.display(description='Частота (визиты)', ordering='frequency_min')
    def frequency_range_col(self, obj):
        return format_html(
            '<span style="font-family:monospace;">{}&thinsp;–&thinsp;{}</span>',
            obj.frequency_min, obj.frequency_max,
        )

    @admin.display(description='Гостей сейчас')
    def guests_count_col(self, obj):
        if not obj.pk:
            return '—'
        count = obj.guests.count()
        if not count:
            return mark_safe('<span style="color:var(--body-quiet-color,#aaa);">0</span>')
        return format_html('<strong>{}</strong>', count)

    @admin.display(description='Подсказка')
    def hint_preview_col(self, obj):
        if not obj.hint:
            return mark_safe('<span style="color:#aaa;">—</span>')
        # Show first line as a tooltip-enabled preview
        first_line = obj.hint.split('\n')[0]
        preview = first_line[:50] + ('…' if len(first_line) > 50 else '')
        tooltip = obj.hint.replace('\n', '&#10;')
        return format_html(
            '<span title="{}" style="cursor:help;border-bottom:1px dotted #aaa;">'
            '{}</span>',
            tooltip, preview,
        )

    @admin.display(description='Действия')
    def actions_col(self, obj):
        if not obj.pk:
            return '—'

        broadcast_url = reverse('admin:analytics_rfsegment_send_broadcast', args=[obj.pk])
        export_url = reverse('admin:analytics_rfsegment_export_senler', args=[obj.pk])

        return format_html(
            '<div style="white-space:nowrap;">'
            '<a class="button" href="{}" '
            'onclick="return confirm(\'Создать рассылку для сегмента «{}»?\');" '
            'style="font-size:11px;padding:4px 10px;margin-right:4px;'
            'background:#4a76a8;color:#fff;border:none;border-radius:4px;'
            'text-decoration:none;display:inline-block;">'
            '📨 Рассылка</a>'
            '<a class="button" href="{}" '
            'style="font-size:11px;padding:4px 10px;'
            'background:#5181b8;color:#fff;border:none;border-radius:4px;'
            'text-decoration:none;display:inline-block;">'
            '📥 Senler</a>'
            '</div>',
            broadcast_url, obj.name,
            export_url,
        )


# ── GuestRFScore filters ──────────────────────────────────────────────────────

class RScoreFilter(admin.SimpleListFilter):
    title = 'R-балл'
    parameter_name = 'r_score'

    def lookups(self, request, model_admin):
        scores = (
            GuestRFScore.objects
            .values_list('r_score', flat=True)
            .distinct()
            .order_by('r_score')
        )
        return [(s, f'R{s}') for s in scores]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(r_score=self.value())
        return queryset


class FScoreFilter(admin.SimpleListFilter):
    title = 'F-балл'
    parameter_name = 'f_score'

    def lookups(self, request, model_admin):
        scores = (
            GuestRFScore.objects
            .values_list('f_score', flat=True)
            .distinct()
            .order_by('f_score')
        )
        return [(s, f'F{s}') for s in scores]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(f_score=self.value())
        return queryset


# ── GuestRFScore admin ────────────────────────────────────────────────────────

@admin.register(GuestRFScore, site=tenant_admin)
class GuestRFScoreAdmin(admin.ModelAdmin):
    list_display = (
        'client_col', 'branch_col',
        'recency_days', 'frequency', 'score_col',
        'segment_badge_col', 'calculated_at',
    )
    list_display_links = ('client_col',)
    list_filter = ('segment', RScoreFilter, FScoreFilter, 'client__branch')
    search_fields = ('client__client__first_name', 'client__client__last_name')
    list_select_related = ('client__client', 'client__branch', 'segment')
    readonly_fields = ('calculated_at',)

    fieldsets = (
        (None, {
            'fields': ('client', 'segment'),
        }),
        ('RF-метрики', {
            'fields': (('recency_days', 'r_score'), ('frequency', 'f_score')),
        }),
        ('Служебное', {
            'fields': ('calculated_at',),
            'classes': ('collapse',),
        }),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'client__client', 'client__branch', 'segment',
        )

    @admin.display(description='Гость', ordering='client__client__first_name')
    def client_col(self, obj):
        c = obj.client.client
        return c.first_name or f'vk{c.vk_id}'

    @admin.display(description='Точка', ordering='client__branch__name')
    def branch_col(self, obj):
        return obj.client.branch.name

    @admin.display(description='R / F', ordering='r_score')
    def score_col(self, obj):
        return format_html(
            '<span style="font-family:monospace;font-weight:700;font-size:13px;">'
            'R{}&thinsp;F{}</span>',
            obj.r_score, obj.f_score,
        )

    @admin.display(description='Сегмент', ordering='segment__name')
    def segment_badge_col(self, obj):
        return _segment_badge(obj.segment)


# ── RFMigrationLog admin ──────────────────────────────────────────────────────

@admin.register(RFMigrationLog, site=tenant_admin)
class RFMigrationLogAdmin(admin.ModelAdmin):
    list_display = (
        'client_col', 'branch_col',
        'from_seg_badge', 'arrow_col', 'to_seg_badge',
        'created_at',
    )
    list_display_links = ('client_col',)
    list_filter = ('from_segment', 'to_segment', 'client__branch')
    search_fields = ('client__client__first_name', 'client__client__last_name')
    list_select_related = ('client__client', 'client__branch', 'from_segment', 'to_segment')
    date_hierarchy = 'created_at'
    readonly_fields = ('created_at',)

    def has_change_permission(self, request, obj=None):
        return request.user.is_superuser

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'client__client', 'client__branch', 'from_segment', 'to_segment',
        )

    @admin.display(description='Гость', ordering='client__client__first_name')
    def client_col(self, obj):
        c = obj.client.client
        return c.first_name or f'vk{c.vk_id}'

    @admin.display(description='Точка', ordering='client__branch__name')
    def branch_col(self, obj):
        return obj.client.branch.name

    @admin.display(description='Из сегмента', ordering='from_segment__name')
    def from_seg_badge(self, obj):
        return _segment_badge(obj.from_segment)

    @admin.display(description='')
    def arrow_col(self, obj):
        return mark_safe('<span style="color:var(--body-quiet-color,#aaa);font-size:16px;">→</span>')

    @admin.display(description='В сегмент', ordering='to_segment__name')
    def to_seg_badge(self, obj):
        return _segment_badge(obj.to_segment)


# ── RFSettings admin ──────────────────────────────────────────────────────────

@admin.register(RFSettings, site=tenant_admin)
class RFSettingsAdmin(admin.ModelAdmin):
    list_display = ('branch', 'analysis_period', 'stats_reset_date', 'updated_at')
    readonly_fields = ('created_at', 'updated_at')

    fieldsets = (
        (None, {
            'fields': ('branch', 'analysis_period'),
            'description': 'Период определяет, сколько дней назад учитываются визиты при расчёте частоты.',
        }),
        ('Обнуление статистики', {
            'fields': ('stats_reset_date',),
            'description': (
                'Если задана дата — визиты ДО неё игнорируются при RF-расчёте. '
                'Полезно после смены концепции или ребрендинга.'
            ),
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )


# ── BranchSegmentSnapshot admin ───────────────────────────────────────────────

@admin.register(BranchSegmentSnapshot, site=tenant_admin)
class BranchSegmentSnapshotAdmin(admin.ModelAdmin):
    list_display = ('date', 'branch', 'segment_badge_col', 'guests_count', 'updated_at')
    list_filter = ('branch', 'segment')
    date_hierarchy = 'date'
    readonly_fields = ('created_at', 'updated_at')

    def has_change_permission(self, request, obj=None):
        return request.user.is_superuser

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('branch', 'segment')

    @admin.display(description='Сегмент', ordering='segment__name')
    def segment_badge_col(self, obj):
        return _segment_badge(obj.segment)


# ── KnowledgeBaseDocument admin ───────────────────────────────────────────────

@admin.register(KnowledgeBaseDocument, site=tenant_admin)
class KnowledgeBaseDocumentAdmin(admin.ModelAdmin):
    list_display = ('title', 'is_active', 'created_at', 'updated_at', 'has_text_col')
    list_filter = ('is_active',)
    search_fields = ('title',)
    readonly_fields = ('extracted_text', 'created_at', 'updated_at')

    fieldsets = (
        (None, {
            'fields': ('title', 'file', 'is_active'),
            'description': (
                'Загрузите Word (.docx) или текстовый (.txt) файл с инструкциями для ИИ-анализа отзывов. '
                'Текст извлекается автоматически при сохранении.'
            ),
        }),
        ('Извлечённый текст', {
            'fields': ('extracted_text',),
            'classes': ('collapse',),
            'description': 'Заполняется автоматически. Используется как дополнительный контекст для ИИ.',
        }),
        ('Служебное', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )

    @admin.display(description='Текст извлечён', boolean=True)
    def has_text_col(self, obj):
        return bool(obj.extracted_text)