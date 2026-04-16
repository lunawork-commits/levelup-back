from django.conf import settings
from django.conf.urls.static import static
from django.urls import path, include

from drf_spectacular.views import SpectacularAPIView
from drf_spectacular.views import SpectacularSwaggerView as _SwaggerView
from drf_spectacular.views import SpectacularRedocView as _RedocView


class SpectacularSwaggerView(_SwaggerView):
    schema = None


class SpectacularRedocView(_RedocView):
    schema = None

from apps.shared.config.admin_sites import tenant_admin

urlpatterns = [
    path('admin/', tenant_admin.urls),
    path('api/v1/', include('apps.tenant.branch.api.urls')),
    path('api/v1/', include('apps.tenant.catalog.api.urls')),
    path('api/v1/', include('apps.tenant.delivery.api.urls')),
    path('api/v1/', include('apps.tenant.game.api.urls')),
    path('api/v1/', include('apps.tenant.inventory.api.urls')),
    path('api/v1/', include('apps.tenant.quest.api.urls')),
    path('telegram/', include('apps.tenant.telegram.api.urls')),
    path('api/v1/', include('apps.tenant.analytics.api.urls')),
    path('api/v1/', include('apps.tenant.reputation.api.urls')),
    path('api/v1/', include('apps.tenant.offline_promo.api.urls')),
    path('analytics/', include('apps.tenant.analytics.urls')),

    # API Docs
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)