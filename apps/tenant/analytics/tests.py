"""
Tests for analytics features:
  - get_pos_guests_count reads from POSGuestCache
  - fetch_pos_data_all_tenants_task skips tenants without POS config
"""
from datetime import date
from unittest.mock import MagicMock, patch

from django.test import TestCase


class GetPosGuestsCountTest(TestCase):
    """get_pos_guests_count sums POSGuestCache rows for the date range."""

    @patch('apps.tenant.analytics.api.services.POSGuestCache')
    def test_sums_all_branches_when_no_filter(self, MockCache):
        """Returns total across all branches when branch_ids is None."""
        from apps.tenant.analytics.api.services import get_pos_guests_count

        MockCache.objects.filter.return_value.aggregate.return_value = {'total': 150}

        result = get_pos_guests_count(None, date(2025, 1, 1), date(2025, 1, 31))
        self.assertEqual(result, 150)

    @patch('apps.tenant.analytics.api.services.POSGuestCache')
    def test_returns_zero_when_no_cache_data(self, MockCache):
        """Returns 0 when aggregate returns None (no cache rows)."""
        from apps.tenant.analytics.api.services import get_pos_guests_count

        MockCache.objects.filter.return_value.aggregate.return_value = {'total': None}

        result = get_pos_guests_count(None, date(2025, 1, 1), date(2025, 1, 31))
        self.assertEqual(result, 0)

    @patch('apps.tenant.analytics.api.services.POSGuestCache')
    def test_filters_by_branch_ids(self, MockCache):
        """Filters queryset by branch_ids when provided."""
        from apps.tenant.analytics.api.services import get_pos_guests_count

        mock_qs = MagicMock()
        mock_qs.filter.return_value.aggregate.return_value = {'total': 42}
        MockCache.objects.filter.return_value = mock_qs

        result = get_pos_guests_count([1, 2], date(2025, 1, 1), date(2025, 1, 7))
        mock_qs.filter.assert_called_once_with(branch__in=[1, 2])
        self.assertEqual(result, 42)


class FetchPosDataTaskTest(TestCase):
    """fetch_pos_data_all_tenants_task skips tenants without POS config."""

    @patch('apps.tenant.analytics.tasks.get_tenant_model')
    def test_skips_tenant_without_config(self, mock_get_tenant_model):
        """Tenant whose config access raises → skipped, no crash."""
        from apps.tenant.analytics.tasks import fetch_pos_data_all_tenants_task

        tenant = MagicMock()
        tenant.schema_name = 'test_schema'
        # Accessing .config raises
        type(tenant).config = property(
            lambda self: (_ for _ in ()).throw(Exception('no config'))
        )
        mock_get_tenant_model.return_value.objects.exclude.return_value \
            .select_related.return_value = [tenant]

        result = fetch_pos_data_all_tenants_task()
        self.assertEqual(result['tenants'], 0)

    @patch('apps.tenant.analytics.tasks.get_tenant_model')
    def test_skips_tenant_with_pos_none(self, mock_get_tenant_model):
        """Tenants with pos_type=none are skipped without API calls."""
        from apps.tenant.analytics.tasks import fetch_pos_data_all_tenants_task
        from apps.shared.config.models import POSType

        tenant = MagicMock()
        tenant.schema_name = 'test_schema'
        tenant.config.pos_type = POSType.NONE

        mock_get_tenant_model.return_value.objects.exclude.return_value \
            .select_related.return_value = [tenant]

        with patch('apps.tenant.analytics.tasks.sync_get_guests_for_period') as mock_svc:
            fetch_pos_data_all_tenants_task()
            mock_svc.assert_not_called()
