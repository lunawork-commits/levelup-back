import json

from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from .serializers import TelegramUpdateSerializer
from .services import process_update


@csrf_exempt
def telegram_webhook(request, bot_token: str):
    """
    GET  → health-check, returns 200 OK (for browser / uptime checks).
    POST → validates the Telegram update and delegates to services.
    """
    if request.method != 'POST':
        return HttpResponse('OK', content_type='text/plain')

    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return HttpResponse(status=400)

    serializer = TelegramUpdateSerializer(data=data)
    if not serializer.is_valid():
        # Unknown or malformed update type — return 200 so Telegram won't retry.
        return HttpResponse(status=200)

    process_update(bot_token, serializer.validated_data)
    return HttpResponse(status=200)
