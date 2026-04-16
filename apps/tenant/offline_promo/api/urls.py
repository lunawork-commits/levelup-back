from django.urls import path

from .views import (
    OfflinePromoCheckView,
    OfflinePromoStartView,
    OfflinePromoClaimView,
    OfflineGiftSelectView,
    OfflineGiftActivateView,
    OfflineGiftStatusView,
    NewClientsStatsView,
    NewClientsDetailView,
    NewClientsSourcesView,
)

urlpatterns = [
    # ── Guest-facing endpoints ────────────────────────────────────────────────
    path('offline-promo/check/',          OfflinePromoCheckView.as_view(),    name='offline-promo-check'),
    path('offline-promo/start/',          OfflinePromoStartView.as_view(),    name='offline-promo-start'),
    path('offline-promo/claim/',          OfflinePromoClaimView.as_view(),    name='offline-promo-claim'),
    path('offline-promo/gift/select/',    OfflineGiftSelectView.as_view(),    name='offline-promo-gift-select'),
    path('offline-promo/gift/activate/',  OfflineGiftActivateView.as_view(),  name='offline-promo-gift-activate'),
    path('offline-promo/gift/status/',    OfflineGiftStatusView.as_view(),    name='offline-promo-gift-status'),

    # ── Admin analytics endpoints ─────────────────────────────────────────────
    path('analytics/new-clients/',         NewClientsStatsView.as_view(),     name='new-clients-stats'),
    path('analytics/new-clients/detail/',  NewClientsDetailView.as_view(),    name='new-clients-detail'),
    path('analytics/new-clients/sources/', NewClientsSourcesView.as_view(),   name='new-clients-sources'),
]
