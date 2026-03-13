from django.urls import path

from .views import BuyView, CatalogView, CooldownView

urlpatterns = [
    path('catalog/', CatalogView.as_view(), name='catalog'),
    path('catalog/cooldown/', CooldownView.as_view(), name='cooldown'),
    path('catalog/buy/', BuyView.as_view(), name='buy'),
]
