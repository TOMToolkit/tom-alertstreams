from __future__ import annotations

import logging
from typing import ClassVar, Literal

from babamul import AlertConsumer, LsstAlert, ZtfAlert
from pydantic import Field

from tom_alertstreams.alertstreams.alertstream import AlertStream, AlertStreamConfig, NormalizedAlert

logger = logging.getLogger(__name__)


class BabamulConfig(AlertStreamConfig):
    """Pydantic configuration model for BabamulAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).

    Fields:
        BABAMUL_KAFKA_USERNAME: Kafka username for the Babamul broker (required).
        BABAMUL_KAFKA_PASSWORD: Kafka password for the Babamul broker (required).
        BABAMUL_GROUP_ID: Kafka consumer group ID. Alerts are partitioned across
            consumers sharing the same group_id, so each TOM instance should use
            a unique group_id to receive all alerts.
        BABAMUL_AUTO_COMMIT: Whether to auto-commit Kafka offsets after consuming.
            False (default) means offsets are not committed, so restarting the
            consumer replays from the configured BABAMUL_OFFSET position.
        BABAMUL_OFFSET: Where to start reading when no committed offset exists.
            'EARLIEST' replays all available alerts; 'LATEST' starts from new ones.
    """
    BABAMUL_KAFKA_USERNAME: str = Field(min_length=1)  # don't accept an empty string
    BABAMUL_KAFKA_PASSWORD: str = Field(min_length=1)
    BABAMUL_GROUP_ID: str = Field(min_length=1)
    BABAMUL_AUTO_COMMIT: bool = False
    BABAMUL_OFFSET: Literal['earliest', 'latest'] = 'latest'


class BabamulAlertStream(AlertStream):
    """AlertStream implementation for Babamul (https://github.com/boom-astro/babamul).
    """
    configuration_class = BabamulConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'babamul'

    def normalize_alert(self, raw_alert: ZtfAlert | LsstAlert, topic: str = '') -> NormalizedAlert:
        """Convert a babamul alert to a NormalizedAlert.

        ZtfAlert and LsstAlert are Pydantic BaseModel subclasses provided by the
        ``babamul`` package. Because they are Pydantic models we can:
        - access fields via typed attributes (raw_alert.objectId, candidate.ra, etc.)
        - serialize to JSON-safe dicts with model_dump(mode='json'), which handles
          datetimes, enums (e.g. Band), and nested models automatically
        - skip defensive getattr() / try-except — field access is guaranteed by the schema

        Args:
            raw_alert: A babamul ZtfAlert or LsstAlert from AlertConsumer.
            topic: The Kafka topic the alert arrived on. Falls back to
                raw_alert.topic if not provided.

        Returns:
            NormalizedAlert with fields extracted from the babamul alert's candidate.
        """
        candidate = raw_alert.candidate

        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic or raw_alert.topic or '',
            observation_time=candidate.datetime,
            published_time=None,
            alert_id=str(raw_alert.candid),
            object_id=raw_alert.objectId,
            ra=candidate.ra,
            dec=candidate.dec,
            magnitude=candidate.magpsf,
            raw_payload=raw_alert.model_dump(mode='json'),
        )

    def listen(self) -> None:
        """Consume Babamul alerts and dispatch to configured topic handlers.

        Opens a babamul AlertConsumer as a context manager and iterates over
        incoming alerts. Each alert is dispatched to the handler for its topic.

        babamul raises on a dropped/unreachable broker (BabamulConnectionError),
        a SASL failure (AuthenticationError), or a bad message (DeserializationError)
        — all from inside the `for alert in consumer:` iteration. We let those
        propagate: AlertStream.run() supervises listen() and reconnects after a
        backoff, so connection resilience lives once in the base class rather than
        being re-implemented per broker.
        """
        topics = list(self.config.TOPIC_HANDLERS.keys())

        with AlertConsumer(
            topics=topics,
            username=self.config.BABAMUL_KAFKA_USERNAME,
            password=self.config.BABAMUL_KAFKA_PASSWORD,
            group_id=self.config.BABAMUL_GROUP_ID,
            offset=self.config.BABAMUL_OFFSET,
            auto_commit=self.config.BABAMUL_AUTO_COMMIT,
        ) as consumer:
            alert: ZtfAlert | LsstAlert
            for alert in consumer:  # yields ZtfAlert | LsstAlert (Pydantic models)
                topic = alert.topic or '<topic not found in alert>'
                if topic not in self.alert_handler:
                    logger.warning(
                        f'BabamulAlertStream: alert from topic "{topic}" has no handler. '
                        f'Configured topics: {list(self.alert_handler.keys())}'
                    )
                    continue

                logger.debug(f'BabamulAlertStream: alert {alert.objectId} (candid={alert.candid}) on {topic}')
                self.alert_handler[topic](alert, alert_stream=self, topic=topic)
