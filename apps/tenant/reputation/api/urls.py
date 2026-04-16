from django.urls import path

from .views import (
    ReputationSyncStatesView,
    ReputationSyncView,
    ReviewGenerateReplyView,
    ReviewIgnoreView,
    ReviewListView,
    ReviewMarkSeenView,
    ReviewSaveReplyView,
)

urlpatterns = [
    path('reputation/reviews/',                     ReviewListView.as_view(),           name='reputation-reviews'),
    path('reputation/reviews/<int:pk>/mark-seen/',  ReviewMarkSeenView.as_view(),       name='reputation-mark-seen'),
    path('reputation/reviews/<int:pk>/save-reply/', ReviewSaveReplyView.as_view(),      name='reputation-save-reply'),
    path('reputation/reviews/<int:pk>/ignore/',     ReviewIgnoreView.as_view(),         name='reputation-ignore'),
    path('reputation/reviews/<int:pk>/generate-reply/', ReviewGenerateReplyView.as_view(), name='reputation-generate-reply'),
    path('reputation/sync/',                        ReputationSyncView.as_view(),       name='reputation-sync'),
    path('reputation/sync-states/',                 ReputationSyncStatesView.as_view(), name='reputation-sync-states'),
]
