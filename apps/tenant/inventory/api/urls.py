from django.urls import path

from .views import (
    BirthdayPrizeView,
    BirthdayStatusView,
    InventoryActivateView,
    InventoryCooldownView,
    InventoryView,
    SuperPrizeView,
)

urlpatterns = [
    path('inventory/',          InventoryView.as_view(),         name='inventory'),
    path('super-prize/',        SuperPrizeView.as_view(),         name='super-prize'),
    path('inventory/cooldown/', InventoryCooldownView.as_view(),  name='inventory-cooldown'),
    path('inventory/activate/', InventoryActivateView.as_view(),  name='inventory-activate'),
    path('birthday/status/',    BirthdayStatusView.as_view(),     name='birthday-status'),
    path('birthday/prize/',     BirthdayPrizeView.as_view(),      name='birthday-prize'),
]
