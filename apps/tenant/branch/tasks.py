"""
VK polling task for group messages.

Usage:
  — With Celery (recommended):
      from apps.tenant.branch.tasks import poll_vk_messages_task
      poll_vk_messages_task.delay(schema_name='levone', branch_id=1)

  — Without Celery (management command):
      python manage.py poll_vk_messages

  — Register in Celery Beat (celery.py):
      app.conf.beat_schedule = {
          'poll-vk-messages': {
              'task': 'apps.tenant.branch.tasks.poll_all_vk_messages_task',
              'schedule': 30.0,   # every 30 seconds
          },
      }

VK Polling uses messages.getConversations + messages.getHistory API:
  https://dev.vk.com/ru/method/messages.getConversations
  https://dev.vk.com/ru/method/messages.getHistory

Alternatively, configure Callback API in VK group settings
and point it at POST /api/v1/vk/callback/ — then no polling is needed.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

# ── VK API helpers ────────────────────────────────────────────────────────────

VK_API_VERSION = '5.131'
VK_API_BASE    = 'https://api.vk.com/method/'


def _vk_call(method: str, token: str, **params) -> dict:
    """Make a synchronous VK API call. Raises RuntimeError on API errors."""
    params['access_token'] = token
    params['v']            = VK_API_VERSION
    url = VK_API_BASE + method + '?' + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as e:
        raise RuntimeError(f'Network error calling VK {method}: {e}') from e

    if 'error' in data:
        err = data['error']
        raise RuntimeError(f'VK API {method} error {err.get("error_code")}: {err.get("error_msg")}')

    return data.get('response', {})


# ── Per-branch polling ────────────────────────────────────────────────────────

def poll_branch_messages(branch_id: int) -> dict:
    """
    Fetch recent unread conversations from VK group for the given branch
    and save new messages to TestimonialConversation/TestimonialMessage.

    Returns: {'new_messages': int, 'errors': list[str]}
    """
    from apps.tenant.branch.api.services import handle_vk_incoming_message
    from apps.tenant.senler.models import SenlerConfig

    try:
        config = SenlerConfig.objects.select_related('branch').get(branch_id=branch_id)
    except SenlerConfig.DoesNotExist:
        return {'new_messages': 0, 'errors': [f'SenlerConfig not found for branch {branch_id}']}

    if not config.vk_community_token:
        return {'new_messages': 0, 'errors': ['vk_community_token not set']}

    token    = config.vk_community_token
    group_id = config.vk_group_id
    errors: list[str] = []
    new_count = 0

    try:
        # Get recent conversations (up to 20)
        convs_resp = _vk_call(
            'messages.getConversations',
            token,
            group_id=group_id,
            filter='unread',
            count=20,
        )
    except RuntimeError as e:
        return {'new_messages': 0, 'errors': [str(e)]}

    items = convs_resp.get('items', [])

    for item in items:
        conv_data = item.get('conversation', {})
        peer      = conv_data.get('peer', {})
        peer_id   = peer.get('id')

        if not peer_id or peer_id < 0:
            # Skip group chats and service chats, only 1-on-1 DMs
            continue

        try:
            # Fetch last 5 messages from this conversation.
            # mark_as_read=0 keeps messages unread in VK so the guest
            # doesn't see them disappear from unread before we actually reply.
            hist = _vk_call(
                'messages.getHistory',
                token,
                peer_id=peer_id,
                group_id=group_id,
                count=5,
                rev=0,          # newest first
                mark_as_read=0,
            )
        except RuntimeError as e:
            errors.append(f'peer {peer_id}: {e}')
            continue

        for msg in hist.get('items', []):
            # Skip outgoing messages (from_id == -group_id)
            if msg.get('from_id', 0) < 0:
                continue
            text = (msg.get('text') or '').strip()
            if not text:
                continue

            saved = handle_vk_incoming_message(
                group_id=group_id,
                from_id=msg['from_id'],
                message_id=msg['id'],
                text=text,
            )
            if saved is not None:
                new_count += 1

    return {'new_messages': new_count, 'errors': errors}


# ── Celery tasks ──────────────────────────────────────────────────────────────

from celery import shared_task


@shared_task(
    name='apps.tenant.branch.tasks.poll_vk_messages_task',
    bind=True,
    max_retries=3,
    default_retry_delay=60,
)
def poll_vk_messages_task(self, schema_name: str, branch_id: int) -> dict:
    """
    Celery task: poll VK messages for one branch in a specific tenant schema.
    Must be called with the correct schema_name for django-tenants.
    """
    from django_tenants.utils import schema_context
    try:
        with schema_context(schema_name):
            result = poll_branch_messages(branch_id)
            if result['errors']:
                logger.warning(
                    'VK poll branch=%s schema=%s errors=%s',
                    branch_id, schema_name, result['errors'],
                )
            return result
    except Exception as exc:
        logger.exception('VK poll failed schema=%s branch=%s', schema_name, branch_id)
        raise self.retry(exc=exc)


@shared_task(name='apps.tenant.branch.tasks.poll_all_vk_messages_task')
def poll_all_vk_messages_task() -> dict:
    """
    Celery Beat task: iterate ALL active tenants and poll VK messages for each
    branch that has SenlerConfig configured.
    Runs every 30 seconds (configured in main/celery.py beat_schedule).
    """
    from django_tenants.utils import get_tenant_model, schema_context

    TenantModel = get_tenant_model()
    total_new   = 0
    total_err: list[str] = []

    for tenant in TenantModel.objects.exclude(schema_name='public'):
        with schema_context(tenant.schema_name):
            from apps.tenant.senler.models import SenlerConfig
            seen_groups: set[int] = set()
            for cfg in SenlerConfig.objects.filter(is_active=True).select_related('branch'):
                if cfg.vk_group_id in seen_groups:
                    continue
                seen_groups.add(cfg.vk_group_id)
                result = poll_branch_messages(cfg.branch_id)
                total_new += result['new_messages']
                total_err.extend(
                    f'[{tenant.schema_name}/branch={cfg.branch_id}] {e}'
                    for e in result['errors']
                )

    if total_err:
        logger.warning('VK poll all errors: %s', total_err)

    return {'new_messages': total_new, 'errors': total_err}
