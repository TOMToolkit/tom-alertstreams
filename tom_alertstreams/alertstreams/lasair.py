from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

from lasair import lasair_consumer

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

class LasairMockConfig(AlertStreamConfig):
    """Pydantic configuration model for LasairMockAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    No additional fields needed for mock data generation.
    """
    pass


class LasairConfig(AlertStreamConfig):
    """Pydantic configuration for LasairAlertStream.

    Fields:
        LASAIR_TOKEN: REST API token (optional — not used for Kafka auth, but
            available for future REST API features like topic discovery).
            Obtained from lasair.lsst.ac.uk/profile.
        LASAIR_KAFKA_SERVER: Kafka broker address (required).
            LSST: 'lasair-lsst-kafka.lsst.ac.uk:9092'
        LASAIR_GROUP_ID: Kafka consumer group ID (required). Same group ID
            resumes from the last offset; a new group ID fetches ~7 days of
            cached alerts.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Maps a single Lasair
            streaming filter topic to a handler dotted-path. Topics are
            user-created filters from https://lasair.lsst.ac.uk/filters/.
    """
    LASAIR_TOKEN: str | None = None
    LASAIR_KAFKA_SERVER: str
    LASAIR_GROUP_ID: str


# ---------------------------------------------------------------------------
# Mock Lasair stream (for demo use without Kafka connectivity)
# ---------------------------------------------------------------------------

class LasairMockAlertStream(AlertStream):
    """Mock Lasair AlertStream that generates obviously-fake alerts.

    Provides a demo fallback when Kafka connectivity is not available.
    Uses the same mock pattern as the other stub streams (sentinel coordinates,
    sentinel magnitude, 6–7 minute sleep between alerts).

    The mock stream uses STREAM_NAME='lasair' so it occupies the same dashboard
    slot as the real Lasair stream would if it were the only one configured.
    """
    configuration_class = LasairMockConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'lasair'
    IS_MOCK: ClassVar[bool] = True

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock Lasair alert dict to a NormalizedAlert.

        Args:
            raw_alert: Dict produced by listen(); contains mock field values.
            topic: Topic the alert was generated for.

        Returns:
            NormalizedAlert populated from the mock dict fields.
        """
        normalized_alert = NormalizedAlert(
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
        return normalized_alert

    def listen(self) -> None:
        """Generate mock Lasair alerts and dispatch to configured topic handlers.

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
            logger.debug(f'LasairMockAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(INTER_ALERT_SLEEP_MIN, INTER_ALERT_SLEEP_MAX))


# ---------------------------------------------------------------------------
# Real Lasair LSST stream
# ---------------------------------------------------------------------------

class LasairAlertStream(AlertStream):
    """Lasair LSST alert stream via Kafka.

    Uses the ``lasair`` library's ``lasair_consumer`` class to receive JSON-encoded
    alerts from the Lasair LSST Kafka broker. Kafka consuming is unauthenticated
    (public), unlike the REST API which requires a token.

    ``lasair_consumer`` accepts a single topic per instance, following the documented
    pattern at lasair.readthedocs.io/en/main/core_functions/alert-streams.html.
    Configure one TOPIC_HANDLERS entry corresponding to a streaming filter you've
    created at lasair.lsst.ac.uk.

    Lasair alert JSON comes in three tiers (configured per-filter in the Lasair web UI):
    - Standard: ``{diaObjectId, ra, decl, UTC}``
    - Lite: adds ``alert.diaSourcesList[]`` with psfFlux, midpointMjdTai, band, etc.
    - Full: adds ``diaObject`` (80+ attrs) and comprehensive ``diaSource`` data.

    ``normalize_alert()`` handles all three tiers defensively, extracting what's available.

    Configuration example (settings.py ALERT_STREAMS entry)::

        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.lasair.LasairAlertStream',
            'OPTIONS': {
                'LASAIR_TOKEN': os.environ.get('LASAIR_LSST_TOKEN', ''),
                'LASAIR_KAFKA_SERVER': os.environ.get('LASAIR_KAFKA_SERVER', ''),
                'LASAIR_GROUP_ID': os.environ.get('LASAIR_GROUP_ID', ''),
                'TOPIC_HANDLERS': {
                    'lasair_114_TOMToolkit.tom-demo': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                },
            },
        }
    """
    configuration_class = LasairConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'lasair'

    def listen(self) -> None:
        """Consume Lasair alerts from Kafka and dispatch to the configured handler.

        Creates a ``lasair_consumer`` for the single configured topic. The consumer
        wraps ``confluent_kafka.Consumer`` with Lasair's standard settings
        (``auto.offset.reset: smallest``). Polls indefinitely; the consumer is closed
        on errors, KeyboardInterrupt, or normal exit via the finally block.
        """
        # lasair_consumer accepts a single topic — take the one configured topic
        topic = list(self.config.TOPIC_HANDLERS.keys())[0]
        logger.info(f'{self.STREAM_NAME}: connecting to {self.config.LASAIR_KAFKA_SERVER}, '
                    f'group_id={self.config.LASAIR_GROUP_ID}, topic={topic}')

        lasair_kafka_consumer = lasair_consumer(
            self.config.LASAIR_KAFKA_SERVER,
            self.config.LASAIR_GROUP_ID,
            topic,
        )

        try:
            while True:
                msg = lasair_kafka_consumer.poll(timeout=20)
                if msg is None:
                    continue  # timeout with no message, retry
                if msg.error():
                    logger.warning(f'{self.STREAM_NAME} Kafka error on {topic}: {msg.error()}')
                    continue
                alert = json.loads(msg.value())
                logger.info(f'{self.STREAM_NAME} received alert on {topic}')
                self.alert_handler[topic](alert, alert_stream=self, topic=topic)
        finally:
            lasair_kafka_consumer.close()

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Extract common fields from a Lasair LSST alert dict.

        Handles all three Lasair message tiers defensively: checks for lite/full
        tier fields first (nested ``alert.diaSourcesList``), then falls back to
        standard tier fields (top-level ``UTC`` timestamp).

        Lasair field naming differs from other LSST streams:
        - ``decl`` instead of ``dec``
        - ``diaObjectId`` at top level (not nested in diaSource)
        - ``UTC`` as ISO string (standard tier) vs ``midpointMjdTai`` (lite/full)

        Args:
            raw_alert: Alert dict decoded from Lasair's JSON Kafka message.
            topic: The Lasair topic the alert was consumed from.

        Returns:
            NormalizedAlert with tier-appropriate fields populated.
        """
        # diaObjectId is at top level in all Lasair tiers
        dia_object_id = raw_alert.get('diaObjectId')

        # Timestamp and flux: prefer lite/full tier diaSourcesList if available,
        # fall back to standard tier's UTC string
        timestamp = None
        flux = None
        dia_sources = raw_alert.get('alert', {}).get('diaSourcesList', [])
        if dia_sources:
            # Lite/full tier: use the most recent diaSource entry
            latest_source = dia_sources[0]
            mjd = latest_source.get('midpointMjdTai')
            if mjd is not None:
                timestamp = _mjd_to_datetime(mjd)
            flux = latest_source.get('psfFlux')

        # Fall back to standard tier UTC timestamp if lite/full didn't provide one
        if timestamp is None:
            utc_str = raw_alert.get('UTC')
            if utc_str:
                timestamp = datetime.fromisoformat(utc_str).replace(tzinfo=timezone.utc)
            else:
                timestamp = None  # no observation time available for this alert

        normalized_alert = NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            observation_time=timestamp,
            published_time=None,
            alert_id=str(dia_object_id or ''),  # Lasair has no separate alert ID
            object_id=str(dia_object_id) if dia_object_id else None,
            ra=raw_alert.get('ra'),
            dec=raw_alert.get('decl'),          # NB: Lasair uses 'decl' not 'dec'
            magnitude=None,                      # LSST uses flux, not magnitude
            flux=flux,
            raw_payload=raw_alert,
        )
        return normalized_alert
