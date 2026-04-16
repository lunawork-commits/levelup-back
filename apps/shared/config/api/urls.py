from django.urls import path

from .views import PublicLandingSettingsView

urlpatterns = [
    path('public/landing-settings/', PublicLandingSettingsView.as_view(), name='public-landing-settings'),
]
