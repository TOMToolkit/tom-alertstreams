from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from django.apps import apps
from django.test import TestCase, tag, override_settings
from django.urls import reverse
from pydantic import ValidationError

from tom_alertstreams.alertstreams.alertstream import NormalizedAlert
from tom_alertstreams.alertstreams.alertstream import save_alert_to_database
from tom_alertstreams.models import Alert


class TestDummy(TestCase):
    """
    This is just a dummy test to make sure the testing infrastructure is working.
    """

    def test_dummy(self):
        assert True


@tag('canary')
class TestDummyCanary(TestCase):
    """
    This is just a dummy test to make sure the testing infrastructure is working.
    """

    def test_dummy_canary(self):
        assert True


class FIFOQueueMixinTest(TestCase):
    """Tests for the FIFOQueueMixin via the Alert model."""

    def setUp(self) -> None:
        # Override FIFO_MAX to a small value so tests run quickly without needing 100 rows.
        self._original_fifo_max = Alert.FIFO_MAX
        Alert.FIFO_MAX = 3

    def tearDown(self) -> None:
        Alert.FIFO_MAX = self._original_fifo_max

    def _make_alert(self, stream_name: str, alert_id: str, day: int) -> Alert:
        """Create and return an Alert with a specific timestamp day."""
        return Alert.objects.create(
            stream_name=stream_name,
            topic='test.topic',
            timestamp=datetime(2024, 1, day, tzinfo=timezone.utc),
            alert_id=alert_id,
            raw_payload={},
        )

    def test_oldest_rows_deleted_when_limit_exceeded(self) -> None:
        """Inserting beyond FIFO_MAX removes the oldest rows for that partition."""
        stream = 'test_fifo'
        for i in range(5):
            self._make_alert(stream, f'alert-{i}', day=i + 1)

        remaining = Alert.objects.filter(stream_name=stream)
        self.assertEqual(remaining.count(), 3)
        # The two oldest (day 1 and day 2) must be gone; the newest must remain.
        self.assertFalse(Alert.objects.filter(alert_id='alert-0').exists())
        self.assertFalse(Alert.objects.filter(alert_id='alert-1').exists())
        self.assertTrue(Alert.objects.filter(alert_id='alert-4').exists())

    def test_fifo_limit_is_per_partition(self) -> None:
        """The row limit applies per stream_name, not across the whole table."""
        for i in range(5):
            self._make_alert('stream_a', f'a-{i}', day=i + 1)
            self._make_alert('stream_b', f'b-{i}', day=i + 1)

        # Each partition should be trimmed independently.
        self.assertEqual(Alert.objects.filter(stream_name='stream_a').count(), 3)
        self.assertEqual(Alert.objects.filter(stream_name='stream_b').count(), 3)

    def test_rows_not_deleted_within_limit(self) -> None:
        """Rows are not deleted when the row count is at or below FIFO_MAX."""
        stream = 'stream_under_limit'
        for i in range(3):
            self._make_alert(stream, f'x-{i}', day=i + 1)

        self.assertEqual(Alert.objects.filter(stream_name=stream).count(), 3)
        self.assertTrue(Alert.objects.filter(alert_id='x-0').exists())


class NormalizedAlertTest(TestCase):
    """Tests for the NormalizedAlert Pydantic model."""

    def test_required_fields_raise_validation_error_when_missing(self) -> None:
        """NormalizedAlert raises ValidationError when required fields are absent."""
        with self.assertRaises(ValidationError):
            NormalizedAlert(stream_name='test')  # missing alert_id and timestamp

    def test_optional_fields_default_to_none(self) -> None:
        """All optional NormalizedAlert fields default correctly when not supplied."""
        alert = NormalizedAlert(
            stream_name='test',
            alert_id='001',
            timestamp=datetime.now(timezone.utc),
        )
        self.assertIsNone(alert.ra)
        self.assertIsNone(alert.dec)
        self.assertIsNone(alert.magnitude)
        self.assertIsNone(alert.object_id)
        self.assertEqual(alert.topic, '')
        self.assertEqual(alert.raw_payload, {})

    def test_full_alert_round_trips_to_dict(self) -> None:
        """model_dump() on a fully-populated NormalizedAlert produces the correct keys."""
        alert = NormalizedAlert(
            stream_name='test',
            alert_id='abc-123',
            timestamp=datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            topic='test.topic',
            object_id='ZTF24aaaaaaaa',
            ra=180.5,
            dec=-30.2,
            magnitude=18.7,
            raw_payload={'foo': 'bar'},
        )
        d = alert.model_dump()
        self.assertEqual(d['stream_name'], 'test')
        self.assertEqual(d['alert_id'], 'abc-123')
        self.assertEqual(d['ra'], 180.5)
        self.assertEqual(d['raw_payload'], {'foo': 'bar'})


class SaveAlertToDatabaseTest(TestCase):
    """Tests for the save_alert_to_database handler."""

    def _make_mock_stream(self, stream_name: str = 'test', alert_id: str = 'test-001') -> MagicMock:
        """Return a mock AlertStream whose normalize_alert produces a fixed NormalizedAlert."""
        mock_stream = MagicMock()

        def normalize(raw_alert: object, topic: str = '') -> NormalizedAlert:
            return NormalizedAlert(
                stream_name=stream_name,
                alert_id=alert_id,
                timestamp=datetime.now(timezone.utc),
                topic=topic,
                raw_payload={},
            )

        mock_stream.get_normalization_function.return_value = normalize
        return mock_stream

    def test_save_creates_alert_in_database(self) -> None:
        """save_alert_to_database persists a NormalizedAlert as an Alert row."""
        mock_stream = self._make_mock_stream(stream_name='antares', alert_id='ant-007')
        result = save_alert_to_database({'data': 'mock'}, alert_stream=mock_stream, topic='test.topic')

        self.assertIsInstance(result, Alert)
        self.assertEqual(result.stream_name, 'antares')
        self.assertEqual(result.alert_id, 'ant-007')
        self.assertEqual(result.topic, 'test.topic')
        self.assertTrue(Alert.objects.filter(alert_id='ant-007').exists())

    def test_save_returns_none_on_normalization_error(self) -> None:
        """save_alert_to_database returns None and does not crash if normalization fails."""
        mock_stream = MagicMock()
        mock_stream.get_normalization_function.return_value = MagicMock(side_effect=RuntimeError('fail'))

        result = save_alert_to_database({'data': 'bad'}, alert_stream=mock_stream)
        self.assertIsNone(result)
        self.assertEqual(Alert.objects.count(), 0)


class RecentAlertsViewTest(TestCase):
    """Tests for the RecentAlertsView."""

    @override_settings(ALERT_STREAMS=[])
    def test_view_returns_200(self) -> None:
        """GET /alertstreams/recent/ returns 200 with no streams configured."""
        url = reverse('alertstreams:recent-alerts')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)

    @override_settings(ALERT_STREAMS=[])
    def test_view_contains_recent_alerts_heading(self) -> None:
        """The Recent Alerts page includes the page heading."""
        url = reverse('alertstreams:recent-alerts')
        response = self.client.get(url)
        self.assertContains(response, 'Recent Alerts')


class AppConfigIntegrationTest(TestCase):
    """Tests for the TomAlertstreamsConfig AppConfig integration points."""

    def test_include_url_paths_returns_alertstreams_pattern(self) -> None:
        """include_url_paths() registers a URL pattern under 'alertstreams/'."""
        app_config = apps.get_app_config('tom_alertstreams')
        url_patterns = app_config.include_url_paths()
        self.assertGreater(len(url_patterns), 0)
        # The first pattern should cover the 'alertstreams/' prefix.
        self.assertIn('alertstreams', str(url_patterns[0].pattern))

    def test_nav_items_returns_navbar_link_partial(self) -> None:
        """nav_items() returns the correct navbar partial path for Recent Alerts."""
        app_config = apps.get_app_config('tom_alertstreams')
        items = app_config.nav_items()
        self.assertEqual(len(items), 1)
        self.assertIn('partial', items[0])
        self.assertIn('navbar_link', items[0]['partial'])
