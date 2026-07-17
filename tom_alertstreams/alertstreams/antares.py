from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

from antares_client.stream import StreamingClient

from tom_alertstreams.alertstreams.alertstream import (
    AlertStream, AlertStreamConfig, NormalizedAlert, _mjd_to_datetime,
)

logger = logging.getLogger(__name__)

# Mock data constants — chosen to be unmistakably non-astronomical:
# (0, 0) is not a real survey pointing; 99.0 is the astronomical sentinel for "no data".
_MOCK_RA = 0.0
_MOCK_DEC = 0.0
_MOCK_MAGNITUDE = 99.0
INTER_ALERT_SLEEP_MIN = 360  # six minutes
INTER_ALERT_SLEEP_MAX = 420  # seven minutes


# ---------------------------------------------------------------------------
# Pydantic configuration models
# ---------------------------------------------------------------------------

class AntaresConfig(AlertStreamConfig):
    """Pydantic configuration model for AntaresAlertStream.

    Inherits from AlertStreamConfig (a Pydantic BaseModel), so Pydantic validates
    that API_KEY and API_SECRET are present and raises descriptive errors if not.

    Fields:
        API_KEY: ANTARES API key (required). Obtain at https://antares.noirlab.edu.
        API_SECRET: ANTARES API secret (required). Obtain at https://antares.noirlab.edu.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Maps ANTARES filter topic
            names to handler dotted-paths. ANTARES topics are filter outputs, not
            survey-specific — a single topic can carry both ZTF and LSST loci.
            Known topics: 'extragalactic_staging', 'nuclear_transient_staging',
            'in_shadow_virgo'. See the ANTARES tags page for the full list.
        GROUP: Kafka consumer group ID. Distinct group IDs let multiple TOM instances
            consume the same stream independently.
        SSL_CA_LOCATION: Path to a TLS Certificate Authority (CA) certificate file.
            The antares_client package bundles a default CA cert that works for the
            current broker. Set this only if a different CA cert is needed.
            When None (the default), the bundled cert is used.
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
    IS_MOCK: ClassVar[bool] = True

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
            observation_time=None,  # a mock alert has no real observation
            published_time=datetime.fromisoformat(raw_alert['timestamp']),
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
# Real ANTARES stream — unified for all surveys (ZTF, LSST, etc.)
# ---------------------------------------------------------------------------

class AntaresAlertStream(AlertStream):
    """ANTARES alert stream for ZTF, LSST, and future survey alerts.

    ANTARES Kafka topics are filter outputs, not survey-specific — a single topic
    like 'extragalactic_staging' can carry loci with ZTF data, LSST data, or both.
    This class handles all surveys through a single normalize_alert() that extracts
    whatever survey data is present on each locus.

    The StreamingClient is created inside listen() using a context manager (not in
    __init__) so the Kafka consumer is properly closed on errors or shutdown.

    Known topics (from the ANTARES tags page at https://antares.noirlab.edu):
        ZTF: 'extragalactic_staging', 'nuclear_transient_staging'
        Mixed/LSST: 'in_shadow_virgo'
    Topic names generally follow the pattern '{tag_name}_staging', though some
    (like 'in_shadow_virgo') omit the suffix.

    Configuration example (settings.py ALERT_STREAMS entry)::

        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.antares.AntaresAlertStream',
            'OPTIONS': {
                'API_KEY': os.environ.get('ANTARES_API_KEY', ''),
                'API_SECRET': os.environ.get('ANTARES_API_SECRET', ''),
                'TOPIC_HANDLERS': {
                    # ZTF topics
                    'extragalactic_staging': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                    'nuclear_transient_staging': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                    # LSST topics
                    'in_shadow_virgo': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                },
            },
        }
    """
    configuration_class = AntaresConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'antares'

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

    def normalize_alert(self, raw_alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from an ANTARES Locus object.

        Handles both ZTF and LSST data by extracting whatever survey-specific
        properties are present on the locus. Field mappings discovered via
        ex_antares_ztf.py and ex_antares_lsst.py introspection:

        - timestamp: properties['newest_alert_observation_time'] (MJD float)
        - magnitude: properties['newest_alert_magnitude'] (ANTARES-enriched)
        - object_id: LSST dia_object_id (nested) → ZTF ztf_object_id (flat) → locus_id
        - alert_id: locus.locus_id (used by AntaresPresenter for locus page URL)

        The full Locus object is not JSON-serializable, so raw_payload is left empty.
        Handlers that need Locus-specific data should access raw_alert directly.

        Args:
            raw_alert: An antares_client.models.Locus object.
            topic: The ANTARES topic (e.g. 'extragalactic_staging').

        Returns:
            NormalizedAlert with survey-appropriate fields populated.
        """
        alert_properties = raw_alert.properties or {}

        # Extract properties from the raw_alert for transfer to NormalizedAlert.

        # Timestamp from ANTARES-enriched locus property (MJD float).
        # This avoids lazy-loading locus.alerts, which triggers an HTTP API call.
        mjd = alert_properties.get('newest_alert_observation_time')
        timestamp = _mjd_to_datetime(mjd) if mjd is not None else None

        # Magnitude — ANTARES-enriched, populated for ZTF loci.
        magnitude = alert_properties.get('newest_alert_magnitude')

        # Object ID — try LSST nested structure first, then ZTF flat property, then locus_id.
        # ANTARES stores LSST IDs in a nested dict: properties['survey']['lsst']['dia_object_id'] → list
        # ZTF IDs are a flat property: properties['ztf_object_id'] → str
        object_id = raw_alert.locus_id  # fallback
        survey = alert_properties.get('survey', {})
        lsst_survey = survey.get('lsst', {}) if isinstance(survey, dict) else {}
        dia_object_ids = lsst_survey.get('dia_object_id', [])
        if dia_object_ids:
            object_id = dia_object_ids[0]
        elif alert_properties.get('ztf_object_id'):
            object_id = alert_properties['ztf_object_id']

        # Flux — LSST uses flux (nanojansky) instead of magnitude.
        # Property key TBD: no LSST-only locus observed from the stream yet.
        # TODO: once the flux property key is known, extract it here.
        flux = None

        # Log LSST locus properties so we can discover the flux key from production data.
        # Remove this logging once we've confirmed the flux property key and updated
        # the extraction above.
        if dia_object_ids:
            logger.info(f'LSST locus detected: {raw_alert.locus_id} — '
                        f'full properties: {alert_properties}')

        normalized_alert = NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            observation_time=timestamp,
            published_time=None,  # ANTARES locus exposes no broker-publish time we extract
            alert_id=str(raw_alert.locus_id),
            object_id=str(object_id),
            ra=float(raw_alert.ra) if raw_alert.ra is not None else None,
            dec=float(raw_alert.dec) if raw_alert.dec is not None else None,
            magnitude=float(magnitude) if magnitude is not None else None,
            flux=flux,
            raw_payload={},
        )
        return normalized_alert
