from django.urls import path
from .views import GeneralStatsAPIView, RFStatsAPIView, BranchListAPIView, RecalculateRFView

urlpatterns = [
    path('analytics/stats/',            GeneralStatsAPIView.as_view(), name='analytics-stats'),
    path('analytics/rf/',               RFStatsAPIView.as_view(),      name='analytics-rf'),
    path('analytics/rf/recalculate/',   RecalculateRFView.as_view(),   name='analytics-rf-recalculate'),
    path('analytics/branches/',         BranchListAPIView.as_view(),   name='analytics-branches'),
]
