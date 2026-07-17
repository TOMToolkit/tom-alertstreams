from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, ClassVar

from gcn_kafka import Consumer

from pydantic import Field

from tom_alertstreams.alertstreams.alertstream import (
    AlertStream, AlertStreamConfig, NormalizedAlert, is_in_hourly_window, save_alert_to_database,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GCNConfig(AlertStreamConfig):
    """Pydantic configuration model for GCNClassicAlertStream.

    Inherits from AlertStreamConfig (a Pydantic BaseModel), so Pydantic validates
    that GCN_KAFKA_CLIENT_ID and GCN_KAFKA_CLIENT_SECRET are present and non-empty.

    Fields:
        GCN_KAFKA_CLIENT_ID: GCN client ID (required). Register at https://gcn.nasa.gov/quickstart.
        GCN_KAFKA_CLIENT_SECRET: GCN client secret (required). Register at https://gcn.nasa.gov/quickstart.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Maps GCN topic names to
            handler dotted-paths. Example topic: 'gcn.circulars'.
        DOMAIN: Kafka broker domain. Defaults to the public GCN broker.
        KAFKA_CONFIG: Optional dict passed directly to the underlying Confluent Kafka
            Consumer for advanced configuration (e.g. group.id, auto.offset.reset).
    """
    GCN_KAFKA_CLIENT_ID: str = Field(min_length=1)  # don't accept an empty string
    GCN_KAFKA_CLIENT_SECRET: str = Field(min_length=1)
    DOMAIN: str = 'gcn.nasa.gov'
    KAFKA_CONFIG: dict = {}  # formerly OPTIONS['CONFIG']; passed to Consumer


class GCNKafkaAlertStream(AlertStream):
    """AlertStream implementation for GCN Kafka.

    GCN (General Coordinates Network, https://gcn.nasa.gov) distributes transient
    alerts from gamma-ray and gravitational-wave observatories. This implementation
    uses the gcn-kafka Python client.

    Configuration example (settings.py ALERT_STREAMS entry):
        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.gcn.GCNKafkaAlertStream',
            'OPTIONS': {
                'GCN_KAFKA_CLIENT_ID': os.environ.get('GCN_KAFKA_CLIENT_ID', ''),
                'GCN_KAFKA_CLIENT_SECRET': os.environ.get('GCN_KAFKA_CLIENT_SECRET', ''),
                'DOMAIN': 'gcn.nasa.gov',          # optional
                'KAFKA_CONFIG': {},                # optional Confluent Kafka settings
                'TOPIC_HANDLERS': {
                    'gcn.circulars': 'tom_alertstreams.alertstreams.gcn.alert_logger',
                    'gcn.classic.text.LVC_INITIAL': 'tom_alertstreams.alertstreams.gcn.alert_logger',
                },
            },
        }

    See https://gcn.nasa.gov/docs/client for gcn_kafka client details.
    """
    configuration_class = GCNConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'gcn'

    def normalize_alert(self, raw_alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from a GCN Kafka message (cimpl.Message).

        GCN alert payloads vary by topic — they can be VOEvent XML, plain text, or JSON.
        This implementation attempts JSON decoding; if the payload is not JSON, the raw
        bytes are stored as a string in raw_payload for downstream handlers to interpret.

        Args:
            raw_alert: A confluent_kafka.cimpl.Message object.
            topic: The GCN Kafka topic (e.g. 'gcn.classic.text.LVC_INITIAL').

        Returns:
            NormalizedAlert with stream_name, topic, and raw_payload populated.
            Astronomical coordinates are not available from GCN Classic text format.
        """
        # Try to decode the alert payload. GCN topics vary in format: some are JSON,
        # others are plain text (VOEvent, GCN Circular text, etc.).

        # TODO: use json-schema to validate alert!!
        raw_payload: dict = {}
        value_bytes = raw_alert.value()
        if value_bytes:
            try:
                raw_payload = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                raw_payload = {'raw_text': value_bytes.decode('utf-8', errors='replace')}

        # alert_id: GCN Circulars carry their circular number in 'circularId' — this is the
        # value GCNPresenter uses to build the gcn.nasa.gov/circulars/{id} link. Other GCN
        # topics fall back to the Kafka message key, then to 'alert_datetime'. The latter is
        # the only useful field a gcn.heartbeat carries (it has just $schema + alert_datetime,
        # no id); using it avoids the alternative of a process-seeded hash of the payload,
        # which changes every run and is meaningless noise in the table.
        circular_id = raw_payload.get('circularId') if isinstance(raw_payload, dict) else None
        key_bytes = raw_alert.key()
        if circular_id is not None:
            alert_id = str(circular_id)
        elif key_bytes:
            alert_id = key_bytes.decode('utf-8')
        else:
            alert_id = raw_payload.get('alert_datetime', '') if isinstance(raw_payload, dict) else ''

        # GCN alerts are reports/notices, not single observations, so observation_time is
        # None. published_time is when GCN issued the alert: Circulars use 'createdOn'
        # (Unix epoch ms), GCN Notices v4+ and heartbeats use 'alert_datetime' (ISO 8601).
        # Fall back to now() for formats with neither (VOEvent XML, plain text).
        published_time = datetime.now(timezone.utc)
        if isinstance(raw_payload, dict):
            if raw_payload.get('createdOn') is not None:
                published_time = datetime.fromtimestamp(raw_payload['createdOn'] / 1000, tz=timezone.utc)
            elif raw_payload.get('alert_datetime'):
                published_time = datetime.fromisoformat(raw_payload['alert_datetime'])

        normalized_alert = NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic or raw_alert.topic(),
            observation_time=None,
            published_time=published_time,
            alert_id=alert_id,
            raw_payload=raw_payload,
        )
        return normalized_alert

    def listen(self) -> None:
        """Consume GCN Kafka alerts and dispatch to configured topic handlers.

        Runs an infinite loop consuming messages from the GCN Kafka broker. Kafka
        errors are logged but do not terminate the loop — transient connectivity
        issues should self-heal on the next consume() call.
        """
        # configure and instanciate the Consumer
        consumer = Consumer(
            client_id=self.config.GCN_KAFKA_CLIENT_ID,
            client_secret=self.config.GCN_KAFKA_CLIENT_SECRET,
            domain=self.config.DOMAIN,
            config=self.config.KAFKA_CONFIG,
        )
        # subscribe to the topics specified in the configuration
        consumer.subscribe(list(self.config.TOPIC_HANDLERS.keys()))

        while True:
            for alert in consumer.consume():
                kafka_error = alert.error()
                if kafka_error is not None:
                    logger.error(
                        f'GCNClassicAlertStream KafkaError: {kafka_error.name()}: {kafka_error.str()}'
                    )
                    continue

                topic = alert.topic()
                if topic not in self.alert_handler:
                    # this shouldn't happen be cause we subscribe to the topics for which
                    # we configured alert handlers for, but just in case:
                    logger.error(
                        f'GCNClassicAlertStream: alert from topic "{topic}" received '
                        f'but no handler defined. Configured topics: {list(self.alert_handler.keys())}'
                    )
                    continue

                # Unified handler convention: alert_stream=self for DI, topic=topic
                # so the handler can pass it to normalize_alert().
                self.alert_handler[topic](alert, alert_stream=self, topic=topic)

        consumer.close()


def alert_logger(raw_alert: Any, alert_stream: AlertStream, topic: str, **kwargs: Any) -> None:
    """Example alert handler for GCNClassicAlertStream.

    Logs the topic and raw value of the Kafka message. Use this as a starting
    point for writing custom handlers; copy it into your TOM's custom_code app
    and modify as needed.

    The **kwargs signature absorbs alert_stream, topic, and any other extras
    passed by the unified handler calling convention.

    Args:
        raw_alert: A confluent_kafka.cimpl.Message object.
        **kwargs: Absorbs alert_stream, topic, and stream-specific extras.
    """
    logger.info(f'gcn.alert_logger topic: {raw_alert.topic()}')
    logger.info(f'gcn.alert_logger value: {raw_alert.value()}')


def save_heartbeat_hourly(raw_alert: Any, alert_stream: AlertStream, **kwargs: Any) -> Any:
    """Alert handler that saves only the gcn.heartbeat on the hour, dropping the rest.

    GCN emits a heartbeat every second — far too frequent for the demo.

    Args:
        raw_alert: The GCN Kafka message (confluent_kafka.cimpl.Message).
        alert_stream: The AlertStream instance (injected; forwarded to save_alert_to_database).
        **kwargs: Stream-specific extras (e.g. topic); forwarded unchanged.

    Returns:
        The saved Alert, or None if this heartbeat was dropped (or the save failed).
    """
    # raw_alert.timestamp() -> (timestamp_type, milliseconds_since_epoch). is_in_hourly_window
    # keeps just the one heartbeat in each UTC hour's first second (see its docstring).
    _, timestamp_ms = raw_alert.timestamp()
    if is_in_hourly_window(timestamp_ms):
        return save_alert_to_database(raw_alert, alert_stream=alert_stream, **kwargs)
    return None  # not in the hour's first second — drop this heartbeat
