from django.urls import path

from .views import telegram_webhook

urlpatterns = [
    path('webhook/<str:bot_token>/', telegram_webhook, name='telegram_webhook'),
]
