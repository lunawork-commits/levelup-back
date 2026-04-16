from django.urls import path

from .views import GameClaimView, GameCooldownView, GameStartView

urlpatterns = [
    path('game/start/',    GameStartView.as_view(),    name='game-start'),
    path('game/claim/',    GameClaimView.as_view(),    name='game-claim'),
    path('game/cooldown/', GameCooldownView.as_view(), name='game-cooldown'),
]
