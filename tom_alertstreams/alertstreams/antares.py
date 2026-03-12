from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

from antares_client.stream import StreamingClient

from tom_alertstreams.alertstreams.alertstream import AlertStream, AlertStreamConfig, NormalizedAlert

logger = logging.getLogger(__name__)

# Mock data constants — chosen to be unmistakably non-astronomical:
# (0, 0) is not a real survey pointing; 99.0 is the astronomical sentinel for "no data".
_MOCK_RA = 0.0
_MOCK_DEC = 0.0
_MOCK_MAGNITUDE = 99.0
INTER_ALERT_SLEEP_MIN = 360  # six minutes
INTER_ALERT_SLEEP_MAX = 420  # seven minutes


# ---------------------------------------------------------------------------
# MJD helper
# ---------------------------------------------------------------------------

def _mjd_to_datetime(mjd: float) -> datetime:
    """Convert Modified Julian Date to a timezone-aware UTC datetime.

    MJD = JD - 2400000.5. JD 2440587.5 = Unix epoch (1970-01-01 00:00:00 UTC).
    So: unix_seconds = (MJD + 2400000.5 - 2440587.5) * 86400
                     = (MJD - 40587.0) * 86400
    """
    unix_seconds = (mjd - 40587.0) * 86400.0
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Pydantic configuration models
# ---------------------------------------------------------------------------

class AntaresConfig(AlertStreamConfig):
    """Pydantic configuration model for real ANTARES alert streams (ZTF and LSST).

    Inherits from AlertStreamConfig (a Pydantic BaseModel), so Pydantic validates
    that API_KEY and API_SECRET are present and raises descriptive errors if not.
    Both AntaresZtfAlertStream and AntaresLsstAlertStream share this config because
    they use the same StreamingClient, same Kafka auth, and same Locus model — the
    only differences are topic names and normalize_alert() field extraction.

    Fields:
        API_KEY: ANTARES API key (required). Obtain at https://antares.noirlab.edu.
        API_SECRET: ANTARES API secret (required). Obtain at https://antares.noirlab.edu.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Maps topic names to handler
            dotted-paths. Known ZTF topics: 'extragalactic_staging',
            'nuclear_transient_staging'.
        GROUP: Kafka consumer group ID. Distinct group IDs let multiple TOM instances
            consume the same stream independently.
        SSL_CA_LOCATION: Path to a TLS Certificate Authority (CA) certificate file.
            When StreamingClient connects to the ANTARES Kafka broker, it uses TLS
            (encrypted connection). TLS requires a CA cert — a file that tells the
            client "trust connections signed by this authority." The antares_client
            package bundles a default CA cert (certificates/kafka-ca.pem) that works
            for the current ZTF broker. If ANTARES uses a different Kafka cluster or
            TLS chain for LSST alerts, the default cert might not be trusted, and
            you'd need to point SSL_CA_LOCATION at the correct CA cert file.
            When None (the default), the bundled cert is used.
            TODO: When researching LSST topic access, find out whether LSST topics
            require a different CA cert and document how to obtain it.
        ENABLE_AUTO_COMMIT: Whether Kafka should auto-commit offsets. Set to False
            for at-least-once processing with manual offset management.
    """
    API_KEY: str
    API_SECRET: str
    GROUP: str = 'tom-alertstreams'
    SSL_CA_LOCATION: str | None = None
    ENABLE_AUTO_COMMIT: bool = True


class AntaresMockConfig(AlertStreamConfig):
    """Pydantic configuration model for AntaresMockAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    No additional fields needed for mock data generation.
    """
    pass


# ---------------------------------------------------------------------------
# Mock ANTARES stream (for demo use without API credentials)
# ---------------------------------------------------------------------------

class AntaresMockAlertStream(AlertStream):
    """Mock ANTARES AlertStream that generates obviously-fake alerts.

    Provides a demo fallback when ANTARES API credentials are not available.
    Uses the same mock pattern as the other stub streams (sentinel coordinates,
    sentinel magnitude, 6–7 minute sleep between alerts).

    The mock stream uses STREAM_NAME='antares' so it occupies the same dashboard
    slot as the real ANTARES streams would if they were the only ones configured.
    """
    configuration_class = AntaresMockConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'antares'

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock ANTARES alert dict to a NormalizedAlert.

        Args:
            raw_alert: Dict produced by listen(); contains mock field values.
            topic: Topic the alert was generated for.

        Returns:
            NormalizedAlert populated from the mock dict fields.
        """
        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic or raw_alert.get('topic', ''),
            timestamp=datetime.fromisoformat(raw_alert['timestamp']),
            alert_id=raw_alert['alert_id'],
            object_id=raw_alert.get('object_id'),
            ra=raw_alert.get('ra'),
            dec=raw_alert.get('dec'),
            magnitude=raw_alert.get('magnitude'),
            raw_payload=raw_alert,
        )

    def listen(self) -> None:
        """Generate mock ANTARES alerts and dispatch to configured topic handlers.

        Loops indefinitely, emitting one mock alert per iteration with a random
        6–7 minute delay. Topics are round-robined if multiple are configured.
        """
        counter = 0
        topics = list(self.config.TOPIC_HANDLERS.keys())

        while True:
            counter += 1
            topic = topics[counter % len(topics)]
            timestamp = datetime.now(timezone.utc)
            object_id = f'MOCK-{self.STREAM_NAME.upper()}-{counter:04d}'
            alert_id = f'MOCK-{timestamp.strftime("%Y%m%d%H%M%S")}'
            mock_alert: dict[str, Any] = {
                'alert_id': alert_id,
                'object_id': object_id,
                'topic': topic,
                'timestamp': timestamp.isoformat(),
                'ra': _MOCK_RA,
                'dec': _MOCK_DEC,
                'magnitude': _MOCK_MAGNITUDE,
                'mock': True,
            }
            logger.debug(f'AntaresMockAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(INTER_ALERT_SLEEP_MIN, INTER_ALERT_SLEEP_MAX))


# ---------------------------------------------------------------------------
# Real ANTARES streams — abstract base with ZTF and LSST subclasses
# ---------------------------------------------------------------------------

class AntaresAlertStream(AlertStream):
    """Abstract base class for real ANTARES alert streams.

    Handles StreamingClient setup and the listen() loop. Not configured directly —
    users configure AntaresZtfAlertStream or AntaresLsstAlertStream, which override
    normalize_alert() for survey-specific field extraction.

    ANTARES is unique among the LSST brokers: it presents a unified Kafka interface
    for both ZTF and LSST alerts via the same StreamingClient and Locus model. The
    only differences between surveys are topic names, properties dict keys, and
    photometry units. This base class captures the shared listen() logic while
    subclasses handle the divergent normalization.

    The StreamingClient is created inside listen() using a context manager (not in
    __init__) so the Kafka consumer is properly closed on errors or shutdown.
    """
    configuration_class = AntaresConfig  # type: ignore[assignment]

    def listen(self) -> None:
        """Consume ANTARES loci and dispatch to configured topic handlers.

        Creates a StreamingClient as a context manager so the underlying Kafka
        consumer is properly closed on errors, KeyboardInterrupt, or normal exit.

        ANTARES delivers (topic, locus) pairs via StreamingClient.iter(). The topic
        includes the stream prefix (e.g. 'client.extragalactic_staging'); we strip
        the prefix to match the base topic names used in TOPIC_HANDLERS.
        """
        # Build optional kwargs from config — only pass non-default values so the
        # StreamingClient uses its own defaults for unset fields.
        optional_kwargs: dict[str, Any] = {}
        if self.config.SSL_CA_LOCATION is not None:
            optional_kwargs['ssl_ca_location'] = self.config.SSL_CA_LOCATION
        if not self.config.ENABLE_AUTO_COMMIT:
            optional_kwargs['enable_auto_commit'] = self.config.ENABLE_AUTO_COMMIT

        with StreamingClient(
            topics=list(self.config.TOPIC_HANDLERS.keys()),
            api_key=self.config.API_KEY,
            api_secret=self.config.API_SECRET,
            group=self.config.GROUP,
            **optional_kwargs,
        ) as antares_streaming_client:
            for topic, locus in antares_streaming_client.iter():
                base_topic = topic.removeprefix(antares_streaming_client._TOPIC_PREFIX)
                logger.info(f'{self.STREAM_NAME} received {locus.locus_id} on {base_topic}')
                self.alert_handler[base_topic](locus, alert_stream=self, topic=base_topic)


class AntaresZtfAlertStream(AntaresAlertStream):
    """ANTARES alert stream for ZTF transient alerts.

    Connects to ANTARES Kafka topics that carry ZTF-originated alerts. Each alert
    is an antares_client Locus object enriched with ZTF-specific properties.

    Known ZTF topics: 'extragalactic_staging', 'nuclear_transient_staging'.
    Contact the ANTARES team for the full topic list.

    Configuration example (settings.py ALERT_STREAMS entry):
        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.antares.AntaresZtfAlertStream',
            'OPTIONS': {
                'API_KEY': os.environ.get('ANTARES_API_KEY', ''),
                'API_SECRET': os.environ.get('ANTARES_API_SECRET', ''),
                'TOPIC_HANDLERS': {
                    'extragalactic_staging': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                    'nuclear_transient_staging': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                },
            },
        }
    """
    STREAM_NAME: ClassVar[str] = 'antares-ztf'

    def normalize_alert(self, raw_alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from an ANTARES Locus object carrying ZTF data.

        Field mappings (discovered via ex_antares.py introspection of a live ZTF locus):
        - timestamp: locus.properties['newest_alert_observation_time'] (MJD float)
        - magnitude: locus.properties['newest_alert_magnitude']
        - object_id: locus.properties['ztf_object_id'] (distinct from locus_id)
        - alert_id: locus.locus_id (used by AntaresPresenter for locus page URL)

        The full Locus object is not JSON-serializable, so raw_payload is left empty.
        Handlers that need Locus-specific data should access raw_alert directly.

        Args:
            raw_alert: An antares_client.models.Locus object.
            topic: The ANTARES topic (e.g. 'extragalactic_staging').

        Returns:
            NormalizedAlert with ZTF-specific fields populated.
        """
        props = raw_alert.properties or {}

        # Timestamp from ANTARES-enriched locus property (MJD float).
        # This avoids lazy-loading locus.alerts, which triggers an HTTP API call.
        mjd = props.get('newest_alert_observation_time')
        timestamp = _mjd_to_datetime(mjd) if mjd is not None else datetime.now(timezone.utc)

        # ZTF magnitude from ANTARES-enriched properties.
        magnitude = props.get('newest_alert_magnitude')

        # ZTF object ID if available; fallback to locus_id.
        object_id = props.get('ztf_object_id', raw_alert.locus_id)

        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            timestamp=timestamp,
            alert_id=str(raw_alert.locus_id),
            object_id=str(object_id),
            ra=float(raw_alert.ra) if raw_alert.ra is not None else None,
            dec=float(raw_alert.dec) if raw_alert.dec is not None else None,
            magnitude=float(magnitude) if magnitude is not None else None,
            flux=None,
            raw_payload={},
        )


class AntaresLsstAlertStream(AntaresAlertStream):
    """ANTARES alert stream for LSST transient alerts.

    Connects to ANTARES Kafka topics that carry LSST-originated alerts. ANTARES
    is structurally ready for LSST — the Locus properties dict already contains
    a 'survey.lsst' namespace with dia_object_id and ss_object_id arrays — but
    LSST topic names and photometry property keys are not yet confirmed.

    This class is provided for forward-compatibility. Activate it in settings once
    LSST topics are available and property key names are confirmed via introspection.

    Configuration example (settings.py ALERT_STREAMS entry):
        {
            'ACTIVE': False,  # activate once LSST topics are confirmed
            'NAME': 'tom_alertstreams.alertstreams.antares.AntaresLsstAlertStream',
            'OPTIONS': {
                'API_KEY': os.environ.get('ANTARES_API_KEY', ''),
                'API_SECRET': os.environ.get('ANTARES_API_SECRET', ''),
                'TOPIC_HANDLERS': {
                    'lsst_placeholder': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                },
            },
        }
    """
    STREAM_NAME: ClassVar[str] = 'antares-lsst'

    def normalize_alert(self, raw_alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from an ANTARES Locus object carrying LSST data.

        LSST-specific field mappings are provisional — based on the Locus properties
        structure observed via ex_antares.py and the antares_client.search module:
        - object_id: properties['survey']['lsst']['dia_object_id'][0] (nested dict)
        - flux: property key TBD (need to inspect a real LSST locus)
        - timestamp: properties['newest_alert_observation_time'] (same as ZTF)

        TODO: Verify all LSST property keys by introspecting a real LSST locus once
        LSST topics are available. Update this method accordingly.

        Args:
            raw_alert: An antares_client.models.Locus object.
            topic: The ANTARES topic for LSST alerts.

        Returns:
            NormalizedAlert with LSST-specific fields populated where known.
        """
        props = raw_alert.properties or {}

        # Timestamp — same MJD property as ZTF (ANTARES-enriched).
        mjd = props.get('newest_alert_observation_time')
        timestamp = _mjd_to_datetime(mjd) if mjd is not None else datetime.now(timezone.utc)

        # LSST object ID from the nested survey dict structure.
        # antares_client.search uses 'properties.survey.lsst.dia_object_id' as a
        # flat key path, but the actual properties dict is nested:
        # properties['survey']['lsst']['dia_object_id'] → list of IDs
        object_id = raw_alert.locus_id  # default fallback
        survey = props.get('survey', {})
        lsst_survey = survey.get('lsst', {}) if isinstance(survey, dict) else {}
        dia_object_ids = lsst_survey.get('dia_object_id', [])
        if dia_object_ids:
            object_id = dia_object_ids[0]

        # LSST flux — property key TBD. Need to inspect a real LSST locus.
        flux = None

        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            timestamp=timestamp,
            alert_id=str(raw_alert.locus_id),
            object_id=str(object_id),
            ra=float(raw_alert.ra) if raw_alert.ra is not None else None,
            dec=float(raw_alert.dec) if raw_alert.dec is not None else None,
            magnitude=None,
            flux=flux,
            raw_payload={},
        )
