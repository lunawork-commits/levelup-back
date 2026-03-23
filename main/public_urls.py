from django.conf import settings
from django.conf.urls.static import static
from django.urls import include, path

from apps.shared.config.admin_sites import public_admin
from apps.tenant.delivery.api.public_views import PublicDeliveryWebhook

urlpatterns = [
    path('admin/', public_admin.urls),
    path('api/v1/', include('apps.shared.clients.api.urls')),
    path('api/v1/delivery/webhook/', PublicDeliveryWebhook.as_view(), name='public-delivery-webhook'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
