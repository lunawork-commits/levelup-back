import json
import urllib.request
from uuid import UUID

from ..models import BotAdmin


# ── Telegram API ──────────────────────────────────────────────────────────────

def call_telegram(token: str, method: str, payload: dict | None = None) -> dict:
    """
    Calls a Telegram Bot API method and returns the parsed JSON response.

    GET  when payload is None, POST otherwise.
    Raises urllib.error.URLError / json.JSONDecodeError on failure.
    """
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(
        f'https://api.telegram.org/bot{token}/{method}',
        data=data,
        headers={'Content-Type': 'application/json'} if data else {},
        method='POST' if data else 'GET',
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def send_message(bot_token: str, chat_id: int, text: str) -> None:
    """Fire-and-forget sendMessage. Never raises — best-effort only."""
    try:
        call_telegram(bot_token, 'sendMessage', {'chat_id': chat_id, 'text': text})
    except Exception:
        pass


# ── Bot admin verification ────────────────────────────────────────────────────

def verify_bot_admin(bot_token: str, chat_id: int, token_str: str) -> BotAdmin | None:
    """
    Finds the BotAdmin matching the verification token and bot token,
    sets chat_id, saves, and returns the instance.
    Returns None if the token is invalid or already used.
    """
    try:
        token = UUID(token_str)
    except ValueError:
        return None

    try:
        bot_admin = BotAdmin.objects.select_related('bot').get(
            verification_token=token,
            bot__api=bot_token,
            chat_id__isnull=True,
        )
    except BotAdmin.DoesNotExist:
        return None

    bot_admin.chat_id = chat_id
    bot_admin.save(update_fields=['chat_id'])
    return bot_admin


# ── Update processing ─────────────────────────────────────────────────────────

def process_update(bot_token: str, update: dict) -> None:
    """
    Entry point for a single Telegram update dict (already validated).
    Currently handles only /start <verification_token> in private messages.
    """
    message = update.get('message') or update.get('edited_message')
    if not message:
        return

    text = (message.get('text') or '').strip()
    chat_id = message.get('chat', {}).get('id')

    if not chat_id or not text.startswith('/start '):
        return

    token_str = text[len('/start '):].strip()
    bot_admin = verify_bot_admin(bot_token, chat_id, token_str)

    if bot_admin:
        send_message(
            bot_token,
            chat_id,
            f'✅ {bot_admin.name}, вы успешно подключены к боту '
            f'@{bot_admin.bot.bot_username}. '
            f'Теперь вы будете получать уведомления.',
        )
