from django.urls import path

from .views import (
    QuestActiveView,
    QuestActivateView,
    QuestCooldownView,
    QuestListView,
    QuestSubmitView,
)

urlpatterns = [
    path('quest/',          QuestListView.as_view(),     name='quest-list'),
    path('quest/active/',   QuestActiveView.as_view(),   name='quest-active'),
    path('quest/activate/', QuestActivateView.as_view(), name='quest-activate'),
    path('quest/submit/',   QuestSubmitView.as_view(),   name='quest-submit'),
    path('quest/cooldown/', QuestCooldownView.as_view(), name='quest-cooldown'),
]
