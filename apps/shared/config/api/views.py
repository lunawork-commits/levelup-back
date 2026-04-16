from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.shared.config.models import LandingSettings

from .serializers import LandingSettingsSerializer


class PublicLandingSettingsView(APIView):
    """
    GET /api/v1/public/landing-settings/

    Публичный read-only эндпоинт — фронт (VK Mini App и веб-SPA) читает
    настройки видео-модалки профиля. Без авторизации, кэшируется на 5 минут.
    """
    permission_classes = [AllowAny]
    authentication_classes = []

    def get(self, request):
        obj = LandingSettings.load()
        data = LandingSettingsSerializer(obj, context={'request': request}).data
        resp = Response(data)
        resp['Cache-Control'] = 'public, max-age=300'
        return resp
