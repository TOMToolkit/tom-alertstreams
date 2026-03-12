from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar, Literal

# AMPEL imports are commented out because ampel-ztf / ampel-lsst pull in pymongo,
# whose bundled bson package shadows the standalone python-bson required by
# antares_client. Until that conflict is resolved, the real AMPEL classes cannot
# coexist with ANTARES in the same environment.
#
# from ampel.alert.AmpelAlert import AmpelAlert
# from ampel.base.AuxUnitRegister import AuxUnitRegister
# from ampel.lsst.alert.load.KafkaAlertLoader import KafkaAlertLoader
# from ampel.lsst.alert.LSSTAlertSupplier import LSSTAlertSupplier
# from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
# from ampel.ztf.t0.load.UWAlertLoader import UWAlertLoader

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
# Julian Date helpers
# ---------------------------------------------------------------------------

def _jd_to_datetime(jd: float) -> datetime:
    """Convert Julian Date to a timezone-aware UTC datetime.

    Uses the standard epoch offset: JD 2440587.5 = Unix epoch (1970-01-01 00:00:00 UTC).
    """
    unix_seconds = (jd - 2440587.5) * 86400.0
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)


def _mjd_to_datetime(mjd: float) -> datetime:
    """Convert Modified Julian Date to a timezone-aware UTC datetime.

    MJD = JD - 2400000.5, so we convert back to JD and delegate.
    """
    return _jd_to_datetime(mjd + 2400000.5)


# ---------------------------------------------------------------------------
# Mock AMPEL stream (for demo use while real AMPEL imports are unavailable)
# ---------------------------------------------------------------------------

class AmpelMockConfig(AlertStreamConfig):
    """Pydantic configuration model for AmpelMockAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    No additional fields needed for mock data generation.
    """
    pass


class AmpelMockAlertStream(AlertStream):
    """Mock AMPEL AlertStream that generates obviously-fake alerts.

    Stands in for the real AMPEL streams while the ampel-ztf / ampel-lsst
    packages cannot be installed alongside antares_client (pymongo bson
    conflict). Uses the same mock pattern as the other stub streams.
    """
    configuration_class = AmpelMockConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'ampel'

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock AMPEL alert dict to a NormalizedAlert.

        Args:
            raw_alert: Dict produced by listen(); contains mock field values.
            topic: Kafka topic the alert was consumed from.

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
        """Generate mock AMPEL alerts and dispatch to configured topic handlers.

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
            logger.debug(f'AmpelMockAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(INTER_ALERT_SLEEP_MIN, INTER_ALERT_SLEEP_MAX))


# ---------------------------------------------------------------------------
# ZTF via AMPEL (requires: pip install ampel-ztf)
# ---------------------------------------------------------------------------

class AmpelZtfConfig(AlertStreamConfig):
    """Pydantic configuration for AmpelZtfAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    Fields map to UWAlertLoader parameters.

    Config values:
        BOOTSTRAP: Kafka broker address. The UW public broker is the default.
        STREAM: 'ztf_uw_public' (all programid=1 alerts) or 'ztf_uw_private'
            (adds programid=2). Must match one of the UWAlertLoader literals.
        GROUP_NAME: Kafka consumer group prefix. UWAlertLoader appends '-{stream}'.
        TIMEOUT: Seconds to wait for messages before the supplier stops iterating.
    """
    BOOTSTRAP: str = 'partnership.alerts.ztf.uw.edu:9092'
    STREAM: Literal['ztf_uw_private', 'ztf_uw_public'] = 'ztf_uw_public'
    GROUP_NAME: str = 'tom-alertstreams'
    TIMEOUT: int = 3600


class AmpelZtfAlertStream(AlertStream):
    """Consume ZTF alerts from the UW Kafka broker via AMPEL's ZiAlertSupplier.

    Uses ampel-ztf's supplier/loader stack to connect to the University of
    Washington's ZTF alert archive. The supplier handles Avro deserialization
    and shapes each alert into an AmpelAlert with structured datapoints.

    Alerts are dispatched to the handler registered for the single configured
    topic in TOPIC_HANDLERS. ZTF topic metadata is not propagated through the
    AMPEL iteration chain, so all alerts use the first (and expected only)
    TOPIC_HANDLERS key as the topic.

    Requires: pip install tom-alertstreams[ampel-ztf]
    """
    configuration_class = AmpelZtfConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'ampel-ztf'

    def normalize_alert(self, alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from an AMPEL-shaped ZTF alert.

        The AmpelAlert.datapoints[0] is the current candidate (a ReadOnlyDict)
        containing jd, ra, dec, magpsf, etc. The original ZTF objectId string
        is preserved in AmpelAlert.extra['name'] (alert.stock is an encoded
        AMPEL-internal integer, not the human-readable ZTF name).

        Args:
            alert: AmpelAlert yielded by ZiAlertSupplier.
            topic: Kafka topic name (passed from listen()).

        Returns:
            NormalizedAlert with magnitude populated; flux is None.
        """
        candidate = alert.datapoints[0]
        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            timestamp=_jd_to_datetime(candidate['jd']),
            alert_id=str(alert.id),
            object_id=alert.extra.get('name') if alert.extra else None,
            ra=candidate.get('ra'),
            dec=candidate.get('dec'),
            magnitude=candidate.get('magpsf'),
            flux=None,
            raw_payload=alert.dict(),
        )

    def listen(self) -> None:
        """Connect to the UW Kafka broker and iterate ZTF alerts indefinitely.

        Registers UWAlertLoader with AMPEL's AuxUnitRegister (required for the
        supplier to resolve the loader by name), then creates a ZiAlertSupplier
        that wraps the loader. UWAlertLoader subscribes to topics in __init__
        (via AllConsumingConsumer), so no context manager is needed.

        Each alert is dispatched to the handler for the single configured topic.
        """
        # Deferred import: ampel-ztf's bson (via pymongo) conflicts with
        # antares_client's standalone bson. Only import when actually used.
        from ampel.alert.AmpelAlert import AmpelAlert  # noqa: F811
        from ampel.base.AuxUnitRegister import AuxUnitRegister
        from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
        from ampel.ztf.t0.load.UWAlertLoader import UWAlertLoader

        # Register the loader class so AuxUnitRegister can resolve it by name
        # when the supplier's UnitModel reference is instantiated.
        AuxUnitRegister._dyn['UWAlertLoader'] = UWAlertLoader

        supplier = ZiAlertSupplier(
            deserialize='avro',
            loader={
                'unit': 'UWAlertLoader',
                'config': {
                    'bootstrap': self.config.BOOTSTRAP,
                    'stream': self.config.STREAM,
                    'group_name': self.config.GROUP_NAME,
                    'timeout': self.config.TIMEOUT,
                },
            },
        )

        # ZTF topics are not propagated through the supplier iteration chain
        # (UWAlertLoader.alerts() reads message.topic() for stats but yields
        # only the raw bytes). Use the single configured topic key for dispatch.
        topic = next(iter(self.config.TOPIC_HANDLERS))
        logger.info(
            '%s: listening on %s (stream=%s, group=%s)',
            self.STREAM_NAME, self.config.BOOTSTRAP, self.config.STREAM, self.config.GROUP_NAME,
        )

        for alert in supplier:
            self.alert_handler[topic](alert, alert_stream=self, topic=topic)


# ---------------------------------------------------------------------------
# LSST via AMPEL (requires: pip install ampel-lsst)
# ---------------------------------------------------------------------------

class AmpelLsstConfig(AlertStreamConfig):
    """Pydantic configuration for AmpelLsstAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    Fields map to KafkaAlertLoader / KafkaConsumerBase parameters.

    Config values:
        BOOTSTRAP: Kafka broker address (e.g. 'alert-stream-int.lsst.cloud:9094').
        TOPICS: Explicit list of Kafka topics to subscribe to.
        GROUP_NAME: Kafka consumer group. LSST brokers require this to be prefixed
            with the assigned username (e.g. 'ampel-idfint-tom-alertstreams').
        TIMEOUT: Seconds to wait for messages before the supplier stops iterating.
        AVRO_SCHEMA: Schema registry URL for Avro deserialization
            (e.g. 'https://alert-schemas-int.lsst.cloud'). None to skip.
        SASL_USERNAME / SASL_PASSWORD: SASL/SCRAM credentials assigned by the
            Rubin alert stream operators. See DMTN-210 for details.
        SASL_MECHANISM: SCRAM variant. Rubin uses SCRAM-SHA-512.
        SECURITY_PROTOCOL: Kafka security protocol. Rubin uses SASL_SSL.
        KAFKA_CONSUMER_PROPERTIES: Extra confluent_kafka consumer config passed
            directly to the underlying DeserializingConsumer.
    """
    BOOTSTRAP: str
    TOPICS: list[str]
    GROUP_NAME: str = 'tom-alertstreams'
    TIMEOUT: int = 300
    AVRO_SCHEMA: str | None = None
    SASL_USERNAME: str | None = None
    SASL_PASSWORD: str | None = None
    SASL_MECHANISM: str = 'SCRAM-SHA-512'
    SECURITY_PROTOCOL: str = 'SASL_SSL'
    KAFKA_CONSUMER_PROPERTIES: dict[str, Any] = {}


class AmpelLsstAlertStream(AlertStream):
    """Consume LSST alerts from a Kafka broker via AMPEL's LSSTAlertSupplier.

    Uses ampel-lsst's supplier/loader stack to connect to a Rubin-compatible
    Kafka broker. The loader handles Avro deserialization (optionally via a
    schema registry) and attaches Kafka metadata (__kafka dict) to each alert.
    The supplier shapes alerts into AmpelAlert objects with field-name upgrades
    (e.g. psFlux → psfFlux, midPointTai → midpointMjdTai, decl → dec).

    Authentication uses SASL_SSL + SCRAM-SHA-512 by default, matching the
    Rubin alert distribution system (DMTN-210).

    Requires: pip install tom-alertstreams[ampel-lsst]
    """
    configuration_class = AmpelLsstConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'ampel-lsst'

    def normalize_alert(self, alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from an AMPEL-shaped LSST alert.

        AmpelAlert.datapoints[0] is the triggering diaSource (a ReadOnlyDict)
        with field-upgraded names: midpointMjdTai, psfFlux, ra, dec, band, etc.
        AmpelAlert.stock is the diaObjectId. Kafka topic metadata is available
        in alert.extra['kafka']['topic'] (preserved by KafkaAlertLoader).

        Args:
            alert: AmpelAlert yielded by LSSTAlertSupplier.
            topic: Kafka topic name (passed from listen()).

        Returns:
            NormalizedAlert with flux populated (nanojansky); magnitude is None.
        """
        dia_source = alert.datapoints[0]
        # Topic from Kafka metadata, falling back to the caller-provided value.
        kafka_topic = ''
        if alert.extra and 'kafka' in alert.extra:
            kafka_topic = alert.extra['kafka'].get('topic', '')
        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic or kafka_topic,
            timestamp=_mjd_to_datetime(dia_source['midpointMjdTai']),
            alert_id=str(alert.id),
            object_id=str(alert.stock),
            ra=dia_source.get('ra'),
            dec=dia_source.get('dec'),
            magnitude=None,
            flux=dia_source.get('psfFlux'),
            raw_payload=alert.dict(),
        )

    def listen(self) -> None:
        """Connect to a Kafka broker and iterate LSST alerts indefinitely.

        Registers KafkaAlertLoader with AMPEL's AuxUnitRegister, then creates
        an LSSTAlertSupplier. Unlike ZTF, the LSST loader subscribes to topics
        in __enter__() (a context manager), so we wrap the iteration in a `with`
        block on the loader.

        SASL credentials are passed to the loader via kafka_consumer_properties
        rather than AMPEL's NamedSecret-based SASLAuthentication, which requires
        AMPEL's secret store infrastructure. Passing them as plain consumer
        properties is simpler and sufficient for our use case.
        """
        # Deferred import: ampel-lsst's bson (via pymongo) conflicts with
        # antares_client's standalone bson. Only import when actually used.
        from ampel.base.AuxUnitRegister import AuxUnitRegister
        from ampel.lsst.alert.load.KafkaAlertLoader import KafkaAlertLoader
        from ampel.lsst.alert.LSSTAlertSupplier import LSSTAlertSupplier

        AuxUnitRegister._dyn['KafkaAlertLoader'] = KafkaAlertLoader

        # Build loader config, merging SASL auth into kafka_consumer_properties.
        kafka_props = dict(self.config.KAFKA_CONSUMER_PROPERTIES)
        if self.config.SASL_USERNAME:
            kafka_props.update({
                'security.protocol': self.config.SECURITY_PROTOCOL,
                'sasl.mechanism': self.config.SASL_MECHANISM,
                'sasl.username': self.config.SASL_USERNAME,
                'sasl.password': self.config.SASL_PASSWORD,
            })

        loader_config: dict[str, Any] = {
            'bootstrap': self.config.BOOTSTRAP,
            'topics': self.config.TOPICS,
            'group_name': self.config.GROUP_NAME,
            'timeout': self.config.TIMEOUT,
            'kafka_consumer_properties': kafka_props,
        }
        if self.config.AVRO_SCHEMA:
            loader_config['avro_schema'] = self.config.AVRO_SCHEMA

        supplier = LSSTAlertSupplier(
            deserialize=None,  # KafkaAlertLoader handles deserialization
            loader={'unit': 'KafkaAlertLoader', 'config': loader_config},
        )

        logger.info(
            '%s: listening on %s (topics=%s, group=%s)',
            self.STREAM_NAME, self.config.BOOTSTRAP, self.config.TOPICS, self.config.GROUP_NAME,
        )

        # KafkaAlertLoader subscribes in __enter__() — must use context manager.
        with supplier.alert_loader:
            fallback_topic = next(iter(self.config.TOPIC_HANDLERS))
            for alert in supplier:
                # Extract real topic from Kafka metadata preserved by KafkaAlertLoader.
                topic = ''
                if alert.extra and 'kafka' in alert.extra:
                    topic = alert.extra['kafka'].get('topic', '')
                if not topic:
                    topic = fallback_topic
                self.alert_handler[topic](alert, alert_stream=self, topic=topic)
