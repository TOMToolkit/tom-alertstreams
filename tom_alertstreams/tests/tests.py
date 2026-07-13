from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from django.apps import apps
from django.test import TestCase, tag, override_settings
from django.urls import reverse
from pydantic import ValidationError

import warnings

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
            observation_time=datetime(2024, 1, day, tzinfo=timezone.utc),
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
            NormalizedAlert(stream_name='test')  # missing required alert_id

    def test_optional_fields_default_to_none(self) -> None:
        """All optional NormalizedAlert fields default correctly when not supplied."""
        alert = NormalizedAlert(
            stream_name='test',
            alert_id='001',
            observation_time=datetime.now(timezone.utc),
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
            observation_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
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


class AntaresNormalizeAlertTest(TestCase):
    """Tests for the unified AntaresAlertStream.normalize_alert() method.

    Uses mock Locus objects to verify that normalize_alert() correctly handles
    ZTF-only loci, LSST-only loci, cross-matched loci (both surveys), and bare
    loci (neither survey ID present).
    """

    def setUp(self) -> None:
        """Create an AntaresAlertStream instance with a minimal config."""
        from tom_alertstreams.alertstreams.antares import AntaresAlertStream

        self.stream = AntaresAlertStream(
            API_KEY='fake-key',
            API_SECRET='fake-secret',
            TOPIC_HANDLERS={'test_topic': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def _make_mock_locus(
        self,
        locus_id: str = 'ANT2026test',
        ra: float = 180.0,
        dec: float = -30.0,
        properties: dict | None = None,
    ) -> MagicMock:
        """Return a mock antares_client Locus object."""
        locus = MagicMock()
        locus.locus_id = locus_id
        locus.ra = ra
        locus.dec = dec
        locus.properties = properties or {}
        return locus

    def test_ztf_locus_extracts_ztf_object_id(self) -> None:
        """ZTF locus: object_id comes from properties['ztf_object_id']."""
        locus = self._make_mock_locus(properties={
            'newest_alert_observation_time': 60400.5,
            'newest_alert_magnitude': 18.5,
            'ztf_object_id': 'ZTF24aatest',
            'survey': {'ztf': {'id': ['ZTF24aatest']}, 'lsst': {'dia_object_id': [], 'ss_object_id': []}},
        })
        result = self.stream.normalize_alert(locus, topic='extragalactic_staging')

        self.assertEqual(result.object_id, 'ZTF24aatest')
        self.assertEqual(result.magnitude, 18.5)
        self.assertEqual(result.stream_name, 'antares')
        self.assertEqual(result.topic, 'extragalactic_staging')

    def test_lsst_locus_extracts_dia_object_id(self) -> None:
        """LSST locus: object_id comes from survey.lsst.dia_object_id[0]."""
        locus = self._make_mock_locus(properties={
            'newest_alert_observation_time': 60400.5,
            'newest_alert_magnitude': None,
            'survey': {'ztf': {'id': []}, 'lsst': {'dia_object_id': ['170028527925067818'], 'ss_object_id': []}},
        })
        result = self.stream.normalize_alert(locus, topic='in_shadow_virgo')

        self.assertEqual(result.object_id, '170028527925067818')

    def test_cross_matched_locus_prefers_lsst_object_id(self) -> None:
        """Cross-matched locus: LSST dia_object_id takes priority over ZTF ztf_object_id."""
        locus = self._make_mock_locus(properties={
            'newest_alert_observation_time': 60400.5,
            'newest_alert_magnitude': 19.0,
            'ztf_object_id': 'ZTF24aatest',
            'survey': {
                'ztf': {'id': ['ZTF24aatest']},
                'lsst': {'dia_object_id': ['170028527925067818'], 'ss_object_id': []},
            },
        })
        result = self.stream.normalize_alert(locus, topic='extragalactic_staging')

        self.assertEqual(result.object_id, '170028527925067818')
        # Magnitude should still be extracted even though LSST object_id was preferred
        self.assertEqual(result.magnitude, 19.0)

    def test_bare_locus_falls_back_to_locus_id(self) -> None:
        """Bare locus (no ZTF or LSST object ID): falls back to locus_id."""
        locus = self._make_mock_locus(locus_id='ANT2026bare', properties={
            'newest_alert_observation_time': 60400.5,
            'survey': {'ztf': {'id': []}, 'lsst': {'dia_object_id': [], 'ss_object_id': []}},
        })
        result = self.stream.normalize_alert(locus, topic='test_topic')

        self.assertEqual(result.object_id, 'ANT2026bare')
        self.assertEqual(result.alert_id, 'ANT2026bare')

    def test_missing_observation_time_is_none(self) -> None:
        """A locus with no newest_alert_observation_time yields observation_time None."""
        locus = self._make_mock_locus(properties={})
        result = self.stream.normalize_alert(locus)

        self.assertIsNone(result.observation_time)


class SaveAlertToDatabaseTest(TestCase):
    """Tests for the save_alert_to_database handler."""

    def _make_mock_stream(self, stream_name: str = 'test', alert_id: str = 'test-001') -> MagicMock:
        """Return a mock AlertStream whose normalize_alert produces a fixed NormalizedAlert."""
        mock_stream = MagicMock()

        def normalize(raw_alert: object, topic: str = '') -> NormalizedAlert:
            return NormalizedAlert(
                stream_name=stream_name,
                alert_id=alert_id,
                observation_time=datetime.now(timezone.utc),
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


class FinkNormalizeAlertZtfTest(TestCase):
    """Tests for FinkAlertStream.normalize_alert() with ZTF alert data."""

    def setUp(self) -> None:
        """Create a FinkAlertStream instance configured for ZTF."""
        from tom_alertstreams.alertstreams.fink import FinkAlertStream

        self.stream = FinkAlertStream(
            FINK_USERNAME='test-user',
            FINK_GROUP_ID='test-group',
            FINK_SERVER='kafka-ztf.fink-broker.org:24499',
            FINK_SURVEY='ztf',
            TOPIC_HANDLERS={'test_topic': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def test_ztf_alert_extracts_candidate_fields(self) -> None:
        """ZTF alert: object_id, magnitude, ra, dec extracted from candidate dict."""
        alert = {
            'objectId': 'ZTF24aatest',
            'candid': 3346128430915015003,
            'candidate': {
                'jd': 2460400.5,
                'ra': 180.5,
                'dec': -30.2,
                'magpsf': 18.7,
                'fid': 1,
            },
        }
        result = self.stream.normalize_alert(alert, topic='fink_sn_candidates_ztf')

        self.assertEqual(result.object_id, 'ZTF24aatest')
        self.assertEqual(result.alert_id, '3346128430915015003')
        self.assertEqual(result.magnitude, 18.7)
        self.assertEqual(result.ra, 180.5)
        self.assertEqual(result.dec, -30.2)
        self.assertIsNone(result.flux)
        self.assertEqual(result.stream_name, 'fink')
        self.assertEqual(result.topic, 'fink_sn_candidates_ztf')
        self.assertEqual(result.raw_payload, alert)

    def test_cutout_stamps_stripped_from_raw_payload(self) -> None:
        """Cutout stamp data (binary FITS) is excluded from raw_payload."""
        alert = {
            'objectId': 'ZTF24aatest',
            'candid': 12345,
            'candidate': {'jd': 2460400.5, 'ra': 0, 'dec': 0, 'magpsf': 18.0},
            'cutoutScience': {'fileName': 'sci.fits.gz', 'stampData': b'\x1f\x8b'},
            'cutoutTemplate': {'fileName': 'ref.fits.gz', 'stampData': b'\x1f\x8b'},
            'cutoutDifference': {'fileName': 'diff.fits.gz', 'stampData': b'\x1f\x8b'},
        }
        result = self.stream.normalize_alert(alert)

        self.assertNotIn('cutoutScience', result.raw_payload)
        self.assertNotIn('cutoutTemplate', result.raw_payload)
        self.assertNotIn('cutoutDifference', result.raw_payload)
        self.assertIn('objectId', result.raw_payload)
        self.assertIn('candidate', result.raw_payload)

    def test_ztf_alert_timestamp_from_jd(self) -> None:
        """ZTF alert: timestamp is converted from JD to UTC datetime."""
        alert = {
            'objectId': 'ZTF24aatest',
            'candid': 12345,
            'candidate': {'jd': 2460400.5, 'ra': 0, 'dec': 0, 'magpsf': 18.0},
        }
        result = self.stream.normalize_alert(alert)

        # JD 2460400.5 = 2024-03-31 00:00:00 UTC
        self.assertEqual(result.observation_time.year, 2024)
        self.assertEqual(result.observation_time.month, 3)
        self.assertEqual(result.observation_time.day, 31)

    def test_ztf_missing_candidate_defaults_gracefully(self) -> None:
        """ZTF alert: missing candidate dict doesn't crash."""
        alert = {'objectId': 'ZTF24aatest', 'candid': 99999}
        result = self.stream.normalize_alert(alert)

        self.assertEqual(result.object_id, 'ZTF24aatest')
        self.assertIsNone(result.ra)
        self.assertIsNone(result.dec)
        self.assertIsNone(result.magnitude)
        # Missing JD -> no observation time
        self.assertIsNone(result.observation_time)


class FinkNormalizeAlertLsstTest(TestCase):
    """Tests for FinkAlertStream.normalize_alert() with LSST alert data."""

    def setUp(self) -> None:
        """Create a FinkAlertStream instance configured for LSST."""
        from tom_alertstreams.alertstreams.fink import FinkAlertStream

        self.stream = FinkAlertStream(
            FINK_USERNAME='test-user',
            FINK_GROUP_ID='test-group',
            FINK_SERVER='kafka-lsst.fink-broker.org:24499',
            FINK_SURVEY='lsst',
            TOPIC_HANDLERS={'test_topic': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def test_lsst_alert_extracts_dia_source_fields(self) -> None:
        """LSST alert: flux, ra, dec extracted from diaSource dict."""
        alert = {
            'diaSource': {
                'diaSourceId': 9876543210,
                'midpointMjdTai': 60400.5,
                'ra': 150.3,
                'dec': -20.1,
                'psFlux': 1234.56,
                'psFluxErr': 12.3,
            },
            'diaObject': {
                'diaObjectId': 170028527925067818,
            },
        }
        result = self.stream.normalize_alert(alert, topic='fink_sn_candidates_lsst')

        self.assertEqual(result.object_id, '170028527925067818')
        self.assertEqual(result.alert_id, '9876543210')
        self.assertEqual(result.flux, 1234.56)
        self.assertIsNone(result.magnitude)
        self.assertEqual(result.ra, 150.3)
        self.assertEqual(result.dec, -20.1)
        self.assertEqual(result.stream_name, 'fink')
        self.assertEqual(result.topic, 'fink_sn_candidates_lsst')
        self.assertEqual(result.raw_payload, alert)

    def test_lsst_alert_timestamp_from_mjd_tai(self) -> None:
        """LSST alert: timestamp is converted from MJD TAI to UTC datetime."""
        alert = {
            'diaSource': {'diaSourceId': 1, 'midpointMjdTai': 60400.5},
            'diaObject': {'diaObjectId': 12345},
        }
        result = self.stream.normalize_alert(alert)

        # MJD 60400.5 → 2024-03-27 12:00:00 UTC (approx — TAI offset ~37s is small)
        self.assertEqual(result.observation_time.year, 2024)
        self.assertIsNotNone(result.observation_time)

    def test_lsst_sso_alert_uses_designation(self) -> None:
        """LSST SSO alert: falls back to mpc_orbits.designation for object_id."""
        alert = {
            'diaSource': {'diaSourceId': 5555, 'midpointMjdTai': 60400.5, 'ra': 0, 'dec': 0},
            'diaObject': None,
            'mpc_orbits': {'designation': '2024 AB1'},
        }
        result = self.stream.normalize_alert(alert)

        self.assertEqual(result.object_id, '2024 AB1')

    def test_lsst_missing_dia_source_defaults_gracefully(self) -> None:
        """LSST alert: missing diaSource dict doesn't crash."""
        alert = {'diaObject': {'diaObjectId': 12345}}
        result = self.stream.normalize_alert(alert)

        self.assertIsNone(result.ra)
        self.assertIsNone(result.dec)
        self.assertIsNone(result.flux)
        self.assertIsNone(result.observation_time)  # no diaSource -> no observation time


class FinkConfigValidationTest(TestCase):
    """Tests for FinkConfig cross-field validation."""

    def test_matching_server_and_survey_no_warning(self) -> None:
        """No warning when FINK_SERVER contains the survey name."""
        from tom_alertstreams.alertstreams.fink import FinkConfig

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            FinkConfig(
                FINK_USERNAME='test',
                FINK_GROUP_ID='test-group',
                FINK_SERVER='kafka-ztf.fink-broker.org:24499',
                FINK_SURVEY='ztf',
                TOPIC_HANDLERS={'t': 'handler.path'},
            )
        self.assertEqual(len(caught), 0)

    def test_mismatched_server_and_survey_warns(self) -> None:
        """Warning when FINK_SERVER doesn't contain the survey name."""
        from tom_alertstreams.alertstreams.fink import FinkConfig

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            FinkConfig(
                FINK_USERNAME='test',
                FINK_GROUP_ID='test-group',
                FINK_SERVER='kafka-ztf.fink-broker.org:24499',
                FINK_SURVEY='lsst',
                TOPIC_HANDLERS={'t': 'handler.path'},
            )
        self.assertEqual(len(caught), 1)
        self.assertIn('does not contain survey name', str(caught[0].message))

    def test_topics_matching_survey_pass(self) -> None:
        """No error when every topic's suffix matches FINK_SURVEY."""
        from tom_alertstreams.alertstreams.fink import FinkConfig

        # ZTF survey paired with a '_ztf' topic
        FinkConfig(
            FINK_USERNAME='test',
            FINK_GROUP_ID='test-group',
            FINK_SERVER='kafka-ztf.fink-broker.org:24499',
            FINK_SURVEY='ztf',
            TOPIC_HANDLERS={'fink_sn_candidates_ztf': 'handler.path'},
        )
        # LSST survey paired with a '_lsst' topic
        FinkConfig(
            FINK_USERNAME='test',
            FINK_GROUP_ID='test-group',
            FINK_SERVER='kafka-lsst.fink-broker.org:24499',
            FINK_SURVEY='lsst',
            TOPIC_HANDLERS={'fink_sn_candidates_lsst': 'handler.path'},
        )

    def test_opposite_survey_topic_raises(self) -> None:
        """A topic tagged for the opposite survey raises ValidationError.

        This is the silent-data-loss bug the hardened validator guards against:
        FINK_SURVEY='lsst' with a '_ztf' topic would route ZTF alerts to the LSST
        normalize branch and drop them all. Now it fails at config-load time.
        """
        from tom_alertstreams.alertstreams.fink import FinkConfig

        with self.assertRaises(ValidationError) as ctx:
            FinkConfig(
                FINK_USERNAME='test',
                FINK_GROUP_ID='test-group',
                FINK_SERVER='kafka-lsst.fink-broker.org:24499',
                FINK_SURVEY='lsst',
                TOPIC_HANDLERS={'fink_sn_candidates_ztf': 'handler.path'},
            )
        self.assertIn('fink_sn_candidates_ztf', str(ctx.exception))


class FinkPresenterTest(TestCase):
    """Tests for the survey-aware FinkPresenter URL construction."""

    def setUp(self) -> None:
        from tom_alertstreams.tables import FinkPresenter
        self.presenter = FinkPresenter()

    def _alert(self, object_id: str | None, topic: str) -> Alert:
        """Build an unsaved Alert with just the fields the presenter reads."""
        return Alert(stream_name='fink', object_id=object_id, topic=topic)

    def test_ztf_topic_links_to_ztf_portal(self) -> None:
        """A '_ztf' topic routes to the ZTF Fink portal by object_id."""
        url = self.presenter.object_url(self._alert('ZTF21abcdxyz', 'fink_sn_candidates_ztf'))
        self.assertEqual(url, 'https://ztf.fink-portal.org/ZTF21abcdxyz')

    def test_lsst_topic_links_to_lsst_portal(self) -> None:
        """A '_lsst' topic routes to the LSST Fink portal by object_id."""
        url = self.presenter.object_url(self._alert('170028527925067818', 'fink_sn_candidates_lsst'))
        self.assertEqual(url, 'https://lsst.fink-portal.org/170028527925067818')

    def test_unrecognized_topic_suffix_returns_none(self) -> None:
        """A topic with no survey suffix yields no link."""
        url = self.presenter.object_url(self._alert('ZTF21abcdxyz', 'fink.test'))
        self.assertIsNone(url)

    def test_missing_object_id_returns_none(self) -> None:
        """No object_id means no object page to link to."""
        url = self.presenter.object_url(self._alert(None, 'fink_sn_candidates_ztf'))
        self.assertIsNone(url)


# ---------------------------------------------------------------------------
# GCN normalize_alert tests
# ---------------------------------------------------------------------------

class GCNNormalizeAlertTest(TestCase):
    """Tests for GCNKafkaAlertStream.normalize_alert()."""

    def setUp(self) -> None:
        from tom_alertstreams.alertstreams.gcn import GCNKafkaAlertStream

        self.stream = GCNKafkaAlertStream(
            GCN_KAFKA_CLIENT_ID='test-id',
            GCN_KAFKA_CLIENT_SECRET='test-secret',
            TOPIC_HANDLERS={'gcn.circulars': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def _message(self, payload: dict, key: bytes | None = None, topic: str = 'gcn.circulars') -> MagicMock:
        """Build a mock confluent_kafka Message exposing value()/key()/topic()."""
        import json
        msg = MagicMock()
        msg.value.return_value = json.dumps(payload).encode('utf-8')
        msg.key.return_value = key
        msg.topic.return_value = topic
        return msg

    def test_circular_uses_circular_id_and_created_on(self) -> None:
        """gcn.circulars: alert_id from circularId, published_time from createdOn; no observation."""
        msg = self._message({
            'circularId': 44876,
            'createdOn': 1780944383971,
            'subject': 'GRB 260607A: AT2026ong is a type-Ia SN',
        })
        result = self.stream.normalize_alert(msg, topic='gcn.circulars')

        self.assertEqual(result.alert_id, '44876')           # not the Kafka-key hash fallback
        self.assertIsNone(result.observation_time)           # a circular is not an observation
        self.assertEqual(result.published_time.year, 2026)   # createdOn 1780944383971 ms -> 2026
        self.assertEqual(result.stream_name, 'gcn')
        self.assertEqual(result.topic, 'gcn.circulars')

    def test_non_circular_falls_back_to_message_key(self) -> None:
        """Without circularId, alert_id comes from the Kafka message key."""
        msg = self._message({'foo': 'bar'}, key=b'some-key', topic='gcn.classic.text.LVC_INITIAL')
        result = self.stream.normalize_alert(msg, topic='gcn.classic.text.LVC_INITIAL')

        self.assertEqual(result.alert_id, 'some-key')

    def test_heartbeat_falls_back_to_alert_datetime(self) -> None:
        """A gcn.heartbeat (only $schema + alert_datetime, no key) uses alert_datetime as id."""
        msg = self._message(
            {'$schema': 'https://gcn.nasa.gov/docs/schema/v4.1.0/.../Alert.schema.json',
             'alert_datetime': '2026-06-08T21:00:00.557693Z'},
            key=None, topic='gcn.heartbeat',
        )
        result = self.stream.normalize_alert(msg, topic='gcn.heartbeat')

        self.assertEqual(result.alert_id, '2026-06-08T21:00:00.557693Z')  # not a hash
        self.assertIsNone(result.observation_time)
        self.assertEqual(result.published_time.year, 2026)


# ---------------------------------------------------------------------------
# ALeRCE tests
# ---------------------------------------------------------------------------

class AlerceConfigValidationTest(TestCase):
    """Tests for AlerceConfig defaults and required fields."""

    def test_defaults_applied(self) -> None:
        """ALERCE_KAFKA_SERVER and TOPIC_PREFIX have sensible defaults."""
        from tom_alertstreams.alertstreams.alerce import AlerceConfig

        config = AlerceConfig(
            ALERCE_GROUP_ID='g', ALERCE_USERNAME='u', ALERCE_PASSWORD='p',
            TOPIC_HANDLERS={'lc_classifier': 'handler.path'},
        )
        self.assertEqual(config.ALERCE_KAFKA_SERVER, 'kafka.alerce.science:9093')
        self.assertEqual(config.TOPIC_PREFIX, 'lc_classifier')

    def test_missing_credentials_raise(self) -> None:
        """Missing ALERCE_USERNAME/PASSWORD/GROUP_ID raises ValidationError."""
        from tom_alertstreams.alertstreams.alerce import AlerceConfig

        with self.assertRaises(ValidationError):
            AlerceConfig(TOPIC_HANDLERS={'lc_classifier': 'handler.path'})


class AlerceNormalizeAlertTest(TestCase):
    """Tests for AlerceAlertStream.normalize_alert()."""

    def setUp(self) -> None:
        from tom_alertstreams.alertstreams.alerce import AlerceAlertStream

        self.stream = AlerceAlertStream(
            ALERCE_GROUP_ID='test-group',
            ALERCE_USERNAME='test-user',
            ALERCE_PASSWORD='test-pass',
            TOPIC_HANDLERS={'lc_classifier': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def test_extracts_object_and_candidate_fields(self) -> None:
        """object_id/alert_id/ra/dec/magnitude are extracted from an ALeRCE record."""
        record = {
            'oid': 'ZTF26aaxipwr',
            'candid': 3438360811115015009,
            'lastmjd': 60400.5,
            'meanra': 150.3,
            'meandec': -20.1,
            'magpsf': 18.5,
        }
        result = self.stream.normalize_alert(record, topic='lc_classifier_20260605')

        self.assertEqual(result.object_id, 'ZTF26aaxipwr')
        self.assertEqual(result.alert_id, '3438360811115015009')
        self.assertEqual(result.ra, 150.3)
        self.assertEqual(result.dec, -20.1)
        self.assertEqual(result.magnitude, 18.5)
        self.assertEqual(result.stream_name, 'alerce')
        self.assertEqual(result.topic, 'lc_classifier_20260605')

    def test_timestamp_from_lastmjd(self) -> None:
        """timestamp is converted from the record's lastmjd."""
        record = {'oid': 'ZTF1', 'candid': 1, 'lastmjd': 60400.5}
        result = self.stream.normalize_alert(record)

        self.assertEqual(result.observation_time.year, 2024)
        self.assertIsNotNone(result.observation_time)

    def test_aid_fallback_for_object_id(self) -> None:
        """object_id falls back to 'aid' when 'oid' is absent."""
        record = {'aid': 'AL26abc', 'candid': 5}
        result = self.stream.normalize_alert(record)

        self.assertEqual(result.object_id, 'AL26abc')

    def test_missing_fields_default_gracefully(self) -> None:
        """Missing coordinates/magnitude/mjd don't crash; timestamp falls back to now."""
        record = {'oid': 'ZTF1', 'candid': 1}
        result = self.stream.normalize_alert(record)

        self.assertIsNone(result.ra)
        self.assertIsNone(result.dec)
        self.assertIsNone(result.magnitude)
        self.assertIsNone(result.observation_time)  # no lastmjd/firstmjd -> no observation time


# ---------------------------------------------------------------------------
# Lasair normalize_alert tests
# ---------------------------------------------------------------------------

class LasairNormalizeAlertStandardTest(TestCase):
    """Tests for LasairAlertStream.normalize_alert() with standard-tier alerts.

    Standard tier: top-level diaObjectId, ra, decl, UTC — no nested alert dict.
    """

    def setUp(self) -> None:
        from tom_alertstreams.alertstreams.lasair import LasairAlertStream
        # Build a minimal LasairAlertStream with a mock config
        self.stream = LasairAlertStream(
            LASAIR_KAFKA_SERVER='kafka.test:9092',
            LASAIR_GROUP_ID='test-group',
            TOPIC_HANDLERS={'test.topic': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def test_standard_tier_extracts_top_level_fields(self) -> None:
        """Standard tier alert populates diaObjectId, ra, decl, UTC correctly."""
        raw_alert = {
            'diaObjectId': 169760235333878021,
            'ra': 221.87087,
            'decl': -38.16173,
            'UTC': '2026-01-29 11:40:14',
        }
        result = self.stream.normalize_alert(raw_alert, topic='test.topic')
        self.assertEqual(result.stream_name, 'lasair')
        self.assertEqual(result.object_id, '169760235333878021')
        self.assertEqual(result.alert_id, '169760235333878021')
        self.assertAlmostEqual(result.ra, 221.87087)
        self.assertAlmostEqual(result.dec, -38.16173)  # 'decl' mapped to 'dec'
        self.assertIsNone(result.magnitude)
        self.assertIsNone(result.flux)

    def test_standard_tier_timestamp_from_utc_string(self) -> None:
        """Standard tier parses the UTC string into a timezone-aware datetime."""
        raw_alert = {
            'diaObjectId': 12345,
            'ra': 0.0,
            'decl': 0.0,
            'UTC': '2026-01-29 11:40:14',
        }
        result = self.stream.normalize_alert(raw_alert)
        self.assertEqual(result.observation_time.year, 2026)
        self.assertEqual(result.observation_time.month, 1)
        self.assertEqual(result.observation_time.day, 29)
        self.assertIsNotNone(result.observation_time.tzinfo)

    def test_standard_tier_missing_fields_defaults_gracefully(self) -> None:
        """Standard tier with missing optional fields doesn't crash."""
        raw_alert = {}
        result = self.stream.normalize_alert(raw_alert, topic='test.topic')
        self.assertEqual(result.alert_id, '')
        self.assertIsNone(result.object_id)
        self.assertIsNone(result.ra)
        self.assertIsNone(result.dec)
        self.assertIsNone(result.observation_time)  # no UTC/MJD -> no observation time


class LasairNormalizeAlertLiteTest(TestCase):
    """Tests for LasairAlertStream.normalize_alert() with lite-tier alerts.

    Lite tier: standard fields plus alert.diaSourcesList with lightcurve data.
    """

    def setUp(self) -> None:
        from tom_alertstreams.alertstreams.lasair import LasairAlertStream
        self.stream = LasairAlertStream(
            LASAIR_KAFKA_SERVER='kafka.test:9092',
            LASAIR_GROUP_ID='test-group',
            TOPIC_HANDLERS={'test.topic': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'},
        )

    def test_lite_tier_extracts_flux_from_dia_sources(self) -> None:
        """Lite tier extracts psfFlux from the first diaSource entry."""
        raw_alert = {
            'diaObjectId': 169760235333878021,
            'ra': 221.87087,
            'decl': -38.16173,
            'UTC': '2026-01-29 11:40:14',
            'alert': {
                'diaSourcesList': [
                    {
                        'midpointMjdTai': 60736.5,
                        'psfFlux': 1234.567,
                        'psfFluxErr': 12.3,
                        'band': 'r',
                        'reliability': 0.95,
                    },
                ],
            },
        }
        result = self.stream.normalize_alert(raw_alert, topic='test.topic')
        self.assertAlmostEqual(result.flux, 1234.567)
        self.assertIsNone(result.magnitude)

    def test_lite_tier_timestamp_from_mjd(self) -> None:
        """Lite tier uses midpointMjdTai from diaSourcesList for timestamp."""
        raw_alert = {
            'diaObjectId': 12345,
            'ra': 0.0,
            'decl': 0.0,
            'UTC': '2026-01-29 11:40:14',
            'alert': {
                'diaSourcesList': [
                    {'midpointMjdTai': 60736.5, 'psfFlux': 100.0},
                ],
            },
        }
        result = self.stream.normalize_alert(raw_alert)
        # MJD 60736.5 should give a timestamp in 2025
        self.assertEqual(result.observation_time.year, 2025)

    def test_raw_payload_preserved(self) -> None:
        """The full raw alert dict is stored in raw_payload."""
        raw_alert = {
            'diaObjectId': 12345,
            'ra': 10.0,
            'decl': 20.0,
            'UTC': '2026-01-01 00:00:00',
            'extra_field': 'should be preserved',
        }
        result = self.stream.normalize_alert(raw_alert)
        self.assertEqual(result.raw_payload['extra_field'], 'should be preserved')
        self.assertEqual(result.raw_payload['diaObjectId'], 12345)


class LasairConfigTest(TestCase):
    """Tests for LasairConfig Pydantic model validation."""

    def test_valid_config(self) -> None:
        """Valid config with all required fields doesn't raise."""
        from tom_alertstreams.alertstreams.lasair import LasairConfig
        config = LasairConfig(
            LASAIR_KAFKA_SERVER='kafka.test:9092',
            LASAIR_GROUP_ID='test-group',
            TOPIC_HANDLERS={'topic': 'handler.path'},
        )
        self.assertEqual(config.LASAIR_KAFKA_SERVER, 'kafka.test:9092')
        self.assertEqual(config.LASAIR_GROUP_ID, 'test-group')
        self.assertIsNone(config.LASAIR_TOKEN)

    def test_missing_required_field_raises(self) -> None:
        """Missing LASAIR_KAFKA_SERVER raises ValidationError."""
        from tom_alertstreams.alertstreams.lasair import LasairConfig
        with self.assertRaises(ValidationError):
            LasairConfig(
                LASAIR_GROUP_ID='test-group',
                TOPIC_HANDLERS={'topic': 'handler.path'},
            )


# ---------------------------------------------------------------------------
# Pitt-Google (Google Cloud Pub/Sub) tests
# ---------------------------------------------------------------------------

# Real, importable dotted-paths so PittGoogleAlertStream instantiation (which imports its
# TOPIC_HANDLERS values) succeeds. pittgoogle imports stay lazy (inside methods) so test
# collection doesn't hard-require pittgoogle-client, matching the Fink/Lasair pattern.
_SAVE_HANDLER = 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'
_PITTGOOGLE_THROTTLE_HANDLER = 'tom_alertstreams.alertstreams.pittgoogle.save_pittgoogle_throttled'
_PITTGOOGLE_STREAM = 'tom_alertstreams.alertstreams.pittgoogle.PittGoogleAlertStream'


class PittGoogleConfigValidationTest(TestCase):
    """Tests for PittGoogleConfig's one-topic-per-entry rule."""

    def test_single_topic_ok(self) -> None:
        """Exactly one topic validates; PITTGOOGLE_PROJECT defaults to Pitt-Google's project."""
        from tom_alertstreams.alertstreams.pittgoogle import PittGoogleConfig

        config = PittGoogleConfig(TOPIC_HANDLERS={'ztf-loop': _SAVE_HANDLER})
        self.assertEqual(list(config.TOPIC_HANDLERS), ['ztf-loop'])
        self.assertTrue(config.PITTGOOGLE_PROJECT)  # non-empty default
        self.assertIsNone(config.PITTGOOGLE_SUBSCRIPTION)

    def test_multiple_topics_raise(self) -> None:
        """More than one topic per entry raises (one Pub/Sub subscription binds to one topic)."""
        from tom_alertstreams.alertstreams.pittgoogle import PittGoogleConfig

        with self.assertRaises(ValidationError) as ctx:
            PittGoogleConfig(TOPIC_HANDLERS={'ztf-loop': _SAVE_HANDLER, 'ztf-alerts': _SAVE_HANDLER})
        self.assertIn('one topic per entry', str(ctx.exception))


class PittGoogleNormalizeAlertTest(TestCase):
    """Tests for PittGoogleAlertStream.normalize_alert() with ZTF alert data."""

    def setUp(self) -> None:
        from tom_alertstreams.alertstreams.pittgoogle import PittGoogleAlertStream

        self.stream = PittGoogleAlertStream(TOPIC_HANDLERS={'ztf-loop': _PITTGOOGLE_THROTTLE_HANDLER})

    def _make_alert(self, payload: dict, sourceid: object, objectid: object,
                    attributes: dict) -> MagicMock:
        """Minimal stand-in for a pittgoogle.Alert — only the attrs normalize_alert reads."""
        alert = MagicMock()
        alert.dict = payload
        alert.sourceid = sourceid
        alert.objectid = objectid
        alert.attributes = attributes
        return alert

    def test_ids_from_sourceid_and_objectid(self) -> None:
        """alert_id comes from sourceid (candid, the detection); object_id from objectid (objectId)."""
        alert = self._make_alert(
            payload={
                'objectId': 'ZTF18admgfuc',
                'candid': 3438360335315010000,
                'candidate': {'jd': 2460400.5, 'ra': 12.3, 'dec': -4.5, 'magpsf': 18.2},
            },
            sourceid=3438360335315010000,
            objectid='ZTF18admgfuc',
            attributes={'kafka.timestamp': '1780304740275'},
        )
        result = self.stream.normalize_alert(alert, topic='ztf-loop')

        self.assertEqual(result.alert_id, '3438360335315010000')  # candid (the detection)
        self.assertEqual(result.object_id, 'ZTF18admgfuc')         # objectId (the persistent object)
        self.assertEqual(result.ra, 12.3)
        self.assertEqual(result.dec, -4.5)
        self.assertEqual(result.magnitude, 18.2)
        self.assertIsNone(result.flux)
        self.assertEqual(result.stream_name, 'pittgoogle')
        self.assertEqual(result.topic, 'ztf-loop')

    def test_observation_time_from_jd(self) -> None:
        """observation_time is converted from the candidate's JD."""
        alert = self._make_alert(
            payload={'candidate': {'jd': 2460400.5}}, sourceid=1, objectid='X', attributes={})
        result = self.stream.normalize_alert(alert)
        # JD 2460400.5 = 2024-03-31 00:00:00 UTC
        self.assertEqual(result.observation_time.year, 2024)
        self.assertEqual(result.observation_time.month, 3)
        self.assertEqual(result.observation_time.day, 31)

    def test_published_time_from_kafka_timestamp(self) -> None:
        """published_time comes from the survey's upstream kafka.timestamp (ms epoch)."""
        alert = self._make_alert(
            payload={'candidate': {}}, sourceid=1, objectid='X',
            attributes={'kafka.timestamp': '1780304740275'})
        result = self.stream.normalize_alert(alert)

        self.assertIsNotNone(result.published_time)
        self.assertEqual(result.published_time.tzinfo, timezone.utc)
        self.assertAlmostEqual(result.published_time.timestamp(), 1780304740.275, places=3)

    def test_cutouts_stripped_from_raw_payload(self) -> None:
        """Cutout stamp data (binary FITS) is excluded from raw_payload; the rest is kept."""
        alert = self._make_alert(
            payload={
                'objectId': 'ZTF1', 'candid': 1, 'candidate': {'jd': 2460400.5},
                'cutoutScience': {'stampData': b'\x1f\x8b'},
                'cutoutTemplate': {'stampData': b'\x1f\x8b'},
                'cutoutDifference': {'stampData': b'\x1f\x8b'},
            },
            sourceid=1, objectid='ZTF1', attributes={})
        result = self.stream.normalize_alert(alert)

        self.assertNotIn('cutoutScience', result.raw_payload)
        self.assertNotIn('cutoutTemplate', result.raw_payload)
        self.assertNotIn('cutoutDifference', result.raw_payload)
        self.assertIn('candidate', result.raw_payload)

    def test_missing_candidate_defaults_gracefully(self) -> None:
        """A ZTF alert with no candidate dict doesn't crash; coords/times are None."""
        alert = self._make_alert(
            payload={'objectId': 'ZTF1', 'candid': 7}, sourceid=7, objectid='ZTF1', attributes={})
        result = self.stream.normalize_alert(alert)

        self.assertEqual(result.alert_id, '7')
        self.assertEqual(result.object_id, 'ZTF1')
        self.assertIsNone(result.ra)
        self.assertIsNone(result.dec)
        self.assertIsNone(result.magnitude)
        self.assertIsNone(result.observation_time)
        self.assertIsNone(result.published_time)


class SavePittGoogleThrottledTest(TestCase):
    """Tests for the save_pittgoogle_throttled throttle handler (mirrors GCN's heartbeat throttle)."""

    def _make_mock_stream(self) -> MagicMock:
        """Return a mock AlertStream whose normalize_alert produces a fixed NormalizedAlert."""
        mock_stream = MagicMock()

        def normalize(raw_alert: object, topic: str = '') -> NormalizedAlert:
            return NormalizedAlert(
                stream_name='pittgoogle', alert_id='candid-1', topic=topic, raw_payload={})

        mock_stream.get_normalization_function.return_value = normalize
        return mock_stream

    def _alert_with_publish_time(self, publish_time: datetime) -> MagicMock:
        """A pittgoogle.Alert stand-in carrying a Pub/Sub publishTime."""
        alert = MagicMock()
        alert.msg.publish_time = publish_time
        return alert

    def test_saves_alert_in_minute_first_second(self) -> None:
        """A publishTime in the minute's first second is saved."""
        from tom_alertstreams.alertstreams.pittgoogle import save_pittgoogle_throttled

        in_window = datetime(2026, 6, 10, 5, 0, 0, tzinfo=timezone.utc)  # 0 s into the minute
        result = save_pittgoogle_throttled(
            self._alert_with_publish_time(in_window),
            alert_stream=self._make_mock_stream(), topic='ztf-loop')
        self.assertIsInstance(result, Alert)
        self.assertEqual(Alert.objects.count(), 1)

    def test_drops_alert_outside_first_second(self) -> None:
        """A publishTime 30 seconds into the minute is dropped (not saved)."""
        from tom_alertstreams.alertstreams.pittgoogle import save_pittgoogle_throttled

        out_window = datetime(2026, 6, 10, 5, 0, 30, tzinfo=timezone.utc)
        result = save_pittgoogle_throttled(
            self._alert_with_publish_time(out_window),
            alert_stream=self._make_mock_stream(), topic='ztf-loop')
        self.assertIsNone(result)
        self.assertEqual(Alert.objects.count(), 0)


class StreamStatusDedupTest(TestCase):
    """The dashboard shows one badge per STREAM_NAME even with several same-name entries."""

    @override_settings(ALERT_STREAMS=[
        {'ACTIVE': True, 'NAME': _PITTGOOGLE_STREAM,
         'OPTIONS': {'TOPIC_HANDLERS': {'ztf-loop': _PITTGOOGLE_THROTTLE_HANDLER}}},
        {'ACTIVE': True, 'NAME': _PITTGOOGLE_STREAM,
         'OPTIONS': {'TOPIC_HANDLERS': {'ztf-alerts': _SAVE_HANDLER}}},
    ])
    def test_duplicate_stream_name_yields_one_badge(self) -> None:
        """Two 'pittgoogle' entries (one per topic) collapse to a single dashboard badge."""
        from tom_alertstreams.views import _build_stream_status

        status = _build_stream_status()
        pittgoogle_badges = [s for s in status if s['stream_name'] == 'pittgoogle']
        self.assertEqual(len(pittgoogle_badges), 1)
