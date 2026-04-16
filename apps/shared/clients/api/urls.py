from django.urls import path

from .views import TenantDomainView

urlpatterns = [
    path('company/<int:client_id>/', TenantDomainView.as_view(), name='tenant-domain'),
]
