import json
import uuid
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, TestCase

from apps.shared.config.admin_sites import tenant_admin

from .api.serializers import (
    TelegramChatSerializer,
    TelegramMessageSerializer,
    TelegramUpdateSerializer,
)
from .api.services import (
    call_telegram,
    process_update,
    send_message,
    verify_bot_admin,
)
from .api.views import telegram_webhook
from .models import BotAdmin, TelegramBot


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TelegramBotModelTest(TestCase):

    def _make_bot(self, **kwargs):
        defaults = {'name': 'Тест Бот', 'bot_username': 'test_bot', 'api': '123:TOKEN'}
        defaults.update(kwargs)
        return TelegramBot(**defaults)

    def test_str(self):
        self.assertEqual(str(self._make_bot()), '@test_bot (Тест Бот)')

    def test_verbose_name(self):
        self.assertEqual(TelegramBot._meta.verbose_name, 'Telegram-бот')
        self.assertEqual(TelegramBot._meta.verbose_name_plural, 'Telegram-боты')

    def test_ordering(self):
        self.assertEqual(TelegramBot._meta.ordering, ['name'])


class BotAdminModelTest(TestCase):

    def _make_bot(self):
        return TelegramBot(name='Бот', bot_username='mybot', api='token')

    def _make_admin(self, **kwargs):
        defaults = {'name': 'Иван Иванов'}
        defaults.update(kwargs)
        obj = BotAdmin(**defaults)
        obj.bot = self._make_bot()
        return obj

    def test_str(self):
        self.assertEqual(str(self._make_admin(name='Иван')), 'Иван → @mybot')

    def test_verification_token_auto_generated(self):
        obj = self._make_admin()
        self.assertIsNotNone(obj.verification_token)
        uuid.UUID(str(obj.verification_token))  # must be valid UUID

    def test_verification_token_unique_per_instance(self):
        a = self._make_admin()
        b = self._make_admin()
        self.assertNotEqual(a.verification_token, b.verification_token)

    def test_chat_id_defaults_to_none(self):
        self.assertIsNone(self._make_admin().chat_id)

    def test_is_active_defaults_true(self):
        self.assertTrue(self._make_admin().is_active)

    def test_verbose_name(self):
        self.assertEqual(BotAdmin._meta.verbose_name, 'Администратор бота')
        self.assertEqual(BotAdmin._meta.verbose_name_plural, 'Администраторы бота')


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------

class TelegramChatSerializerTest(TestCase):

    def test_valid(self):
        s = TelegramChatSerializer(data={'id': 123456})
        self.assertTrue(s.is_valid())
        self.assertEqual(s.validated_data['id'], 123456)

    def test_id_required(self):
        s = TelegramChatSerializer(data={})
        self.assertFalse(s.is_valid())
        self.assertIn('id', s.errors)

    def test_extra_fields_ignored(self):
        s = TelegramChatSerializer(data={'id': 1, 'type': 'private', 'username': 'x'})
        self.assertTrue(s.is_valid())
        self.assertNotIn('type', s.validated_data)


class TelegramMessageSerializerTest(TestCase):

    def _valid(self, **kwargs):
        d = {'message_id': 1, 'chat': {'id': 100}}
        d.update(kwargs)
        return d

    def test_valid_with_text(self):
        s = TelegramMessageSerializer(data=self._valid(text='/start abc'))
        self.assertTrue(s.is_valid())

    def test_text_defaults_to_empty_string(self):
        s = TelegramMessageSerializer(data=self._valid())
        self.assertTrue(s.is_valid())
        self.assertEqual(s.validated_data['text'], '')

    def test_chat_required(self):
        s = TelegramMessageSerializer(data={'message_id': 1})
        self.assertFalse(s.is_valid())
        self.assertIn('chat', s.errors)

    def test_message_id_required(self):
        s = TelegramMessageSerializer(data={'chat': {'id': 1}})
        self.assertFalse(s.is_valid())
        self.assertIn('message_id', s.errors)


class TelegramUpdateSerializerTest(TestCase):

    def _valid(self, **kwargs):
        d = {'update_id': 42}
        d.update(kwargs)
        return d

    def test_valid_without_message(self):
        s = TelegramUpdateSerializer(data=self._valid())
        self.assertTrue(s.is_valid())
        self.assertIsNone(s.validated_data['message'])
        self.assertIsNone(s.validated_data['edited_message'])

    def test_valid_with_message(self):
        s = TelegramUpdateSerializer(data=self._valid(
            message={'message_id': 1, 'chat': {'id': 100}, 'text': '/start abc'},
        ))
        self.assertTrue(s.is_valid())
        self.assertIsNotNone(s.validated_data['message'])

    def test_valid_with_edited_message(self):
        s = TelegramUpdateSerializer(data=self._valid(
            edited_message={'message_id': 1, 'chat': {'id': 100}},
        ))
        self.assertTrue(s.is_valid())
        self.assertIsNotNone(s.validated_data['edited_message'])

    def test_update_id_required(self):
        s = TelegramUpdateSerializer(data={})
        self.assertFalse(s.is_valid())
        self.assertIn('update_id', s.errors)

    def test_extra_fields_ignored(self):
        # callback_query and other update types should not cause failure
        s = TelegramUpdateSerializer(data=self._valid(callback_query={'data': 'x'}))
        self.assertTrue(s.is_valid())
        self.assertNotIn('callback_query', s.validated_data)

    def test_message_with_invalid_chat_fails(self):
        s = TelegramUpdateSerializer(data=self._valid(
            message={'message_id': 1, 'chat': {}},  # chat.id missing
        ))
        self.assertFalse(s.is_valid())


# ---------------------------------------------------------------------------
# Services — call_telegram
# ---------------------------------------------------------------------------

class CallTelegramTest(TestCase):

    def _mock_urlopen(self, mock, body: bytes):
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock.return_value = mock_resp

    @patch('apps.tenant.telegram.api.services.urllib.request.urlopen')
    def test_get_request_when_no_payload(self, mock_urlopen):
        self._mock_urlopen(mock_urlopen, b'{"ok":true}')
        result = call_telegram('TOKEN', 'getMe')
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.method, 'GET')
        self.assertIn('getMe', req.full_url)
        self.assertEqual(result, {'ok': True})

    @patch('apps.tenant.telegram.api.services.urllib.request.urlopen')
    def test_post_request_with_payload(self, mock_urlopen):
        self._mock_urlopen(mock_urlopen, b'{"ok":true,"result":true}')
        call_telegram('TOKEN', 'setWebhook', {'url': 'https://example.com'})
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.method, 'POST')
        self.assertIn('setWebhook', req.full_url)

    @patch('apps.tenant.telegram.api.services.urllib.request.urlopen')
    def test_token_included_in_url(self, mock_urlopen):
        self._mock_urlopen(mock_urlopen, b'{"ok":true}')
        call_telegram('SECRET_TOKEN', 'getMe')
        req = mock_urlopen.call_args[0][0]
        self.assertIn('SECRET_TOKEN', req.full_url)

    @patch('apps.tenant.telegram.api.services.urllib.request.urlopen')
    def test_raises_on_network_error(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError('refused')
        with self.assertRaises(urllib.error.URLError):
            call_telegram('TOKEN', 'getMe')


# ---------------------------------------------------------------------------
# Services — send_message
# ---------------------------------------------------------------------------

class SendMessageTest(TestCase):

    @patch('apps.tenant.telegram.api.services.call_telegram')
    def test_calls_sendMessage_with_correct_args(self, mock_call):
        mock_call.return_value = {'ok': True}
        send_message('TOKEN', 123, 'Привет!')
        mock_call.assert_called_once_with(
            'TOKEN', 'sendMessage', {'chat_id': 123, 'text': 'Привет!'},
        )

    @patch('apps.tenant.telegram.api.services.call_telegram')
    def test_never_raises_on_error(self, mock_call):
        mock_call.side_effect = Exception('network error')
        send_message('TOKEN', 123, 'test')  # must not raise


# ---------------------------------------------------------------------------
# Services — verify_bot_admin
# ---------------------------------------------------------------------------

class VerifyBotAdminTest(TestCase):

    def test_invalid_uuid_returns_none(self):
        self.assertIsNone(verify_bot_admin('TOKEN', 123, 'not-a-uuid'))

    def test_empty_string_returns_none(self):
        self.assertIsNone(verify_bot_admin('TOKEN', 123, ''))

    @patch('apps.tenant.telegram.api.services.BotAdmin.objects')
    def test_bot_admin_not_found_returns_none(self, mock_objects):
        mock_objects.select_related.return_value.get.side_effect = BotAdmin.DoesNotExist
        self.assertIsNone(verify_bot_admin('TOKEN', 123, str(uuid.uuid4())))

    @patch('apps.tenant.telegram.api.services.BotAdmin.objects')
    def test_saves_chat_id_and_returns_instance(self, mock_objects):
        token = uuid.uuid4()
        mock_admin = MagicMock(spec=BotAdmin)
        mock_admin.bot.bot_username = 'bot'
        mock_admin.name = 'Иван'
        mock_objects.select_related.return_value.get.return_value = mock_admin

        result = verify_bot_admin('TOKEN', 999, str(token))

        self.assertEqual(mock_admin.chat_id, 999)
        mock_admin.save.assert_called_once_with(update_fields=['chat_id'])
        self.assertIs(result, mock_admin)

    @patch('apps.tenant.telegram.api.services.BotAdmin.objects')
    def test_query_filters_by_chat_id_isnull(self, mock_objects):
        """Already-connected admins (chat_id set) must not be re-verified."""
        mock_objects.select_related.return_value.get.side_effect = BotAdmin.DoesNotExist
        verify_bot_admin('TOKEN', 123, str(uuid.uuid4()))
        get_kwargs = mock_objects.select_related.return_value.get.call_args[1]
        self.assertTrue(get_kwargs.get('chat_id__isnull'))

    @patch('apps.tenant.telegram.api.services.BotAdmin.objects')
    def test_query_filters_by_bot_api_token(self, mock_objects):
        mock_objects.select_related.return_value.get.side_effect = BotAdmin.DoesNotExist
        verify_bot_admin('MY_BOT_TOKEN', 123, str(uuid.uuid4()))
        get_kwargs = mock_objects.select_related.return_value.get.call_args[1]
        self.assertEqual(get_kwargs.get('bot__api'), 'MY_BOT_TOKEN')


# ---------------------------------------------------------------------------
# Services — process_update
# ---------------------------------------------------------------------------

class ProcessUpdateTest(TestCase):

    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    @patch('apps.tenant.telegram.api.services.send_message')
    def test_no_message_key_does_nothing(self, mock_send, mock_verify):
        process_update('TOKEN', {'update_id': 1})
        mock_verify.assert_not_called()
        mock_send.assert_not_called()

    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_message_without_start_does_nothing(self, mock_verify):
        process_update('TOKEN', {
            'update_id': 1,
            'message': {'chat': {'id': 100}, 'text': 'Привет'},
        })
        mock_verify.assert_not_called()

    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_message_without_chat_id_does_nothing(self, mock_verify):
        process_update('TOKEN', {
            'update_id': 1,
            'message': {'chat': {}, 'text': '/start abc'},
        })
        mock_verify.assert_not_called()

    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_valid_start_calls_verify_with_correct_args(self, mock_verify):
        token_str = str(uuid.uuid4())
        mock_verify.return_value = None
        process_update('BOT_TOKEN', {
            'update_id': 1,
            'message': {'chat': {'id': 123}, 'text': f'/start {token_str}'},
        })
        mock_verify.assert_called_once_with('BOT_TOKEN', 123, token_str)

    @patch('apps.tenant.telegram.api.services.send_message')
    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_successful_verify_sends_confirmation(self, mock_verify, mock_send):
        token_str = str(uuid.uuid4())
        mock_admin = MagicMock()
        mock_admin.name = 'Иван'
        mock_admin.bot.bot_username = 'mybot'
        mock_verify.return_value = mock_admin

        process_update('TOKEN', {
            'update_id': 1,
            'message': {'chat': {'id': 55}, 'text': f'/start {token_str}'},
        })

        mock_send.assert_called_once()
        bot_token, chat_id, text = mock_send.call_args[0]
        self.assertEqual(bot_token, 'TOKEN')
        self.assertEqual(chat_id, 55)
        self.assertIn('Иван', text)
        self.assertIn('mybot', text)

    @patch('apps.tenant.telegram.api.services.send_message')
    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_failed_verify_does_not_send(self, mock_verify, mock_send):
        mock_verify.return_value = None
        process_update('TOKEN', {
            'update_id': 1,
            'message': {'chat': {'id': 1}, 'text': '/start bad-uuid'},
        })
        mock_send.assert_not_called()

    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_edited_message_is_also_processed(self, mock_verify):
        token_str = str(uuid.uuid4())
        mock_verify.return_value = None
        process_update('TOKEN', {
            'update_id': 1,
            'edited_message': {'chat': {'id': 456}, 'text': f'/start {token_str}'},
        })
        mock_verify.assert_called_once_with('TOKEN', 456, token_str)

    @patch('apps.tenant.telegram.api.services.verify_bot_admin')
    def test_start_without_token_does_nothing(self, mock_verify):
        # '/start' alone (no space + token) — stripped token_str is empty
        mock_verify.return_value = None
        process_update('TOKEN', {
            'update_id': 1,
            'message': {'chat': {'id': 1}, 'text': '/start'},
        })
        mock_verify.assert_not_called()


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class TelegramWebhookViewTest(TestCase):

    def setUp(self):
        self.factory = RequestFactory()

    def _post(self, data, bot_token='TOKEN'):
        req = self.factory.post(
            f'/telegram/webhook/{bot_token}/',
            data=json.dumps(data),
            content_type='application/json',
        )
        return telegram_webhook(req, bot_token=bot_token)

    def test_get_returns_200_with_ok(self):
        req = self.factory.get('/telegram/webhook/TOKEN/')
        resp = telegram_webhook(req, bot_token='TOKEN')
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b'OK', resp.content)

    def test_put_returns_200_ok(self):
        req = self.factory.put('/telegram/webhook/TOKEN/')
        resp = telegram_webhook(req, bot_token='TOKEN')
        self.assertEqual(resp.status_code, 200)

    def test_invalid_json_returns_400(self):
        req = self.factory.post(
            '/telegram/webhook/TOKEN/',
            data=b'not-json',
            content_type='application/json',
        )
        resp = telegram_webhook(req, bot_token='TOKEN')
        self.assertEqual(resp.status_code, 400)

    def test_invalid_serializer_returns_200_not_retry(self):
        resp = self._post({'missing_update_id': True})
        self.assertEqual(resp.status_code, 200)

    @patch('apps.tenant.telegram.api.views.process_update')
    def test_valid_update_returns_200(self, mock_process):
        resp = self._post({'update_id': 1})
        self.assertEqual(resp.status_code, 200)

    @patch('apps.tenant.telegram.api.views.process_update')
    def test_valid_update_calls_process_update(self, mock_process):
        resp = self._post({
            'update_id': 1,
            'message': {'message_id': 1, 'chat': {'id': 100}, 'text': '/start abc'},
        }, bot_token='MY_TOKEN')
        mock_process.assert_called_once()
        bot_token_arg = mock_process.call_args[0][0]
        self.assertEqual(bot_token_arg, 'MY_TOKEN')

    @patch('apps.tenant.telegram.api.views.process_update')
    def test_validated_data_passed_to_process_update(self, mock_process):
        token_str = str(uuid.uuid4())
        self._post({
            'update_id': 7,
            'message': {'message_id': 1, 'chat': {'id': 555}, 'text': f'/start {token_str}'},
        })
        update_arg = mock_process.call_args[0][1]
        self.assertEqual(update_arg['message']['chat']['id'], 555)
        self.assertEqual(update_arg['update_id'], 7)

    @patch('apps.tenant.telegram.api.views.process_update')
    def test_update_without_message_still_processed(self, mock_process):
        # Non-message update types (callback_query etc.) → update_id only
        self._post({'update_id': 99})
        mock_process.assert_called_once()


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

class TelegramAdminRegistrationTest(TestCase):

    def test_telegrambot_registered_in_tenant_admin(self):
        self.assertIn(TelegramBot, tenant_admin._registry)

    def test_botadmin_registered_in_tenant_admin(self):
        self.assertIn(BotAdmin, tenant_admin._registry)


class TelegramBotAdminConfigTest(TestCase):

    def setUp(self):
        self.ma = tenant_admin._registry[TelegramBot]

    def test_list_display_fields(self):
        for field in ('name', 'bot_username_link', 'branch', 'admins_count', 'updated_at'):
            self.assertIn(field, self.ma.list_display)

    def test_has_bot_admin_inline(self):
        from .admin import BotAdminInline
        self.assertIn(BotAdminInline, self.ma.inlines)

    def test_register_webhook_action_registered(self):
        self.assertIn('register_webhook', self.ma.actions)

    def test_search_fields(self):
        self.assertIn('name', self.ma.search_fields)
        self.assertIn('bot_username', self.ma.search_fields)


class BotAdminInlineDisplayTest(TestCase):

    def setUp(self):
        from .admin import BotAdminInline
        self.inline = BotAdminInline(TelegramBot, tenant_admin)

    def test_connect_button_no_pk_returns_dash(self):
        obj = MagicMock(spec=BotAdmin)
        obj.pk = None
        self.assertEqual(self.inline.connect_button(obj), '—')

    def test_connect_button_with_chat_id_shows_connected(self):
        obj = MagicMock(spec=BotAdmin)
        obj.pk = 1
        obj.chat_id = 123456
        result = str(self.inline.connect_button(obj))
        self.assertIn('подключён', result)

    @patch('apps.tenant.telegram.admin.reverse')
    def test_connect_button_no_chat_id_shows_link(self, mock_reverse):
        mock_reverse.return_value = '/fake/connect/url/'
        obj = MagicMock(spec=BotAdmin)
        obj.pk = 1
        obj.chat_id = None
        obj.bot_id = 1
        result = str(self.inline.connect_button(obj))
        self.assertIn('Подключить', result)
        self.assertIn('/fake/connect/url/', result)
