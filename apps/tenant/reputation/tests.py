from django.test import SimpleTestCase

from apps.tenant.reputation.sources.gis import GisSource, _parse_review as gis_parse
from apps.tenant.reputation.sources.yandex import YandexSource, _parse_review as ya_parse


class YandexExtractOidTests(SimpleTestCase):

    def test_oid_from_query_string(self):
        url = 'https://yandex.ru/maps/213/moscow/?ll=37.6,55.7&oid=1234567890&sll=37.6,55.7'
        self.assertEqual(YandexSource.extract_external_id(url), '1234567890')

    def test_oid_from_path(self):
        url = 'https://yandex.ru/maps/org/kafe-levelup/9876543210987/'
        self.assertEqual(YandexSource.extract_external_id(url), '9876543210987')

    def test_oid_from_path_without_slug(self):
        url = 'https://yandex.ru/maps/org/555666777'
        self.assertEqual(YandexSource.extract_external_id(url), '555666777')

    def test_none_on_empty(self):
        self.assertIsNone(YandexSource.extract_external_id(''))
        self.assertIsNone(YandexSource.extract_external_id(None))  # type: ignore[arg-type]

    def test_none_on_short_link(self):
        self.assertIsNone(YandexSource.extract_external_id('https://yandex.ru/maps/-/CDABCDEF'))


class GisExtractFirmIdTests(SimpleTestCase):

    def test_firm_id_simple(self):
        url = 'https://2gis.ru/moscow/firm/70000001234567890'
        self.assertEqual(GisSource.extract_external_id(url), '70000001234567890')

    def test_firm_id_with_tab(self):
        url = 'https://2gis.ru/moscow/firm/70000001234567890/tab/reviews'
        self.assertEqual(GisSource.extract_external_id(url), '70000001234567890')

    def test_none_on_short_link(self):
        self.assertIsNone(GisSource.extract_external_id('https://go.2gis.com/abcde'))

    def test_none_on_empty(self):
        self.assertIsNone(GisSource.extract_external_id(''))


class YandexParseReviewTests(SimpleTestCase):

    def test_full_payload(self):
        item = {
            'reviewId': 'rev-42',
            'author': {'name': 'Иван П.'},
            'rating': 5,
            'text': 'Отличное место!',
            'updatedTime': 1700000000000,
        }
        fetched = ya_parse(item)
        assert fetched is not None
        self.assertEqual(fetched.external_id, 'rev-42')
        self.assertEqual(fetched.author_name, 'Иван П.')
        self.assertEqual(fetched.rating, 5)
        self.assertEqual(fetched.text, 'Отличное место!')
        self.assertIsNotNone(fetched.published_at)

    def test_clamps_invalid_rating(self):
        item = {'reviewId': 'rev-1', 'rating': 9}
        fetched = ya_parse(item)
        assert fetched is not None
        self.assertIsNone(fetched.rating)

    def test_returns_none_without_id(self):
        self.assertIsNone(ya_parse({'text': 'hi'}))


class GisParseReviewTests(SimpleTestCase):

    def test_full_payload(self):
        item = {
            'id': 'g-1',
            'user': {'name': 'Анна'},
            'rating': 4,
            'text': 'Неплохо',
            'date_created': '2024-03-01T12:00:00Z',
        }
        fetched = gis_parse(item)
        assert fetched is not None
        self.assertEqual(fetched.external_id, 'g-1')
        self.assertEqual(fetched.author_name, 'Анна')
        self.assertEqual(fetched.rating, 4)
        self.assertIsNotNone(fetched.published_at)

    def test_skips_non_dict(self):
        self.assertIsNone(gis_parse('not-a-dict'))  # type: ignore[arg-type]
