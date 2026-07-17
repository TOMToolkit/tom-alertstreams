from __future__ import annotations

import io
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

from confluent_kafka import Consumer
from fastavro import reader as avro_reader

from tom_alertstreams.alertstreams.alertstream import (
    AlertStream, AlertStreamConfig, NormalizedAlert, _mjd_to_datetime,
)

logger = logging.getLogger(__name__)

# Mock data constants — chosen to be unmistakably non-astronomical:
# (0, 0) is not a real survey pointing; 99.0 is the astronomical sentinel for "no data".
_MOCK_RA = 0.0
_MOCK_DEC = 0.0
_MOCK_MAGNITUDE = 99.0
INTER_ALERT_SLEEP_MIN = 3600  # sixty minutes
INTER_ALERT_SLEEP_MAX = 3600


# ---------------------------------------------------------------------------
# Pydantic configuration models
# ---------------------------------------------------------------------------

class AlerceMockConfig(AlertStreamConfig):
    """Pydantic configuration model for AlerceMockAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    No additional fields needed for mock data generation.
    """
    pass


class AlerceConfig(AlertStreamConfig):
    """Pydantic configuration for AlerceAlertStream.

    ALeRCE applies ML classifiers to the survey alert stream (ZTF now, LSST in
    preparation) and re-publishes the results over Kafka. Access requires credentials,
    obtained by emailing alerce.broker@gmail.com — see
    https://github.com/alercebroker/Kafka-Connection-Docs.

    Fields:
        ALERCE_KAFKA_SERVER: Kafka bootstrap server. Defaults to the public ALeRCE broker.
        ALERCE_GROUP_ID: Kafka consumer group ID (required).
        ALERCE_USERNAME: SASL/SCRAM username issued by ALeRCE (required).
        ALERCE_PASSWORD: SASL/SCRAM password issued by ALeRCE (required).
        TOPIC_PREFIX: ALeRCE publishes a new topic per UTC day named '{prefix}_YYYYMMDD'
            (e.g. 'lc_classifier_20260605'), each retained ~48 hours. We subscribe by
            regex on this prefix so the consumer follows the rolling daily topics without
            a config change. Use 'lc_classifier' (light-curve classifier) or
            'stamp_classifier' (stamp classifier).
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Because the literal topic name
            changes daily, the single entry here is keyed on TOPIC_PREFIX rather than a
            concrete topic, e.g.
            {'lc_classifier': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database'}.
    """
    ALERCE_KAFKA_SERVER: str = 'kafka.alerce.science:9093'
    ALERCE_GROUP_ID: str
    ALERCE_USERNAME: str
    ALERCE_PASSWORD: str
    TOPIC_PREFIX: str = 'lc_classifier'


# ---------------------------------------------------------------------------
# Mock ALeRCE stream (for demo use without ALeRCE credentials)
# ---------------------------------------------------------------------------

class AlerceMockAlertStream(AlertStream):
    """Mock ALeRCE AlertStream that generates obviously-fake alerts.

    Demo fallback when ALeRCE Kafka credentials are not available. Uses the same mock
    pattern as the other stub streams (sentinel coordinates, sentinel magnitude, 6-7
    minute sleep between alerts). STREAM_NAME='alerce' so it occupies the same dashboard
    slot the real ALeRCE stream would.
    """
    configuration_class = AlerceMockConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'alerce'
    IS_MOCK: ClassVar[bool] = True

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock ALeRCE alert dict to a NormalizedAlert.

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
        """Generate mock ALeRCE alerts and dispatch to configured topic handlers.

        Loops indefinitely, emitting one mock alert per iteration with a random 6-7
        minute delay. Topics are round-robined if multiple are configured.
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
            logger.debug(f'AlerceMockAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(INTER_ALERT_SLEEP_MIN, INTER_ALERT_SLEEP_MAX))


# ---------------------------------------------------------------------------
# Real ALeRCE stream
# ---------------------------------------------------------------------------

class AlerceAlertStream(AlertStream):
    """ALeRCE alert stream via Kafka.

    Consumes ALeRCE's classifier output with a raw confluent_kafka Consumer, following
    the same lightweight pattern as LasairAlertStream. Authentication is SASL_PLAINTEXT +
    SCRAM-SHA-256 with credentials issued by ALeRCE (email alerce.broker@gmail.com).

    Rolling daily topics: ALeRCE creates a new topic per UTC day, '{TOPIC_PREFIX}_YYYYMMDD',
    each retained ~48 hours. We subscribe with a confluent_kafka regex ('^{prefix}_') so the
    consumer follows the rolling topics automatically — no daily config change. Because the
    literal topic name is therefore not known in advance, every message is dispatched to the
    single handler configured under the TOPIC_PREFIX key (the single-handler approach
    HopskotchAlertStream uses for its wildcard subscriptions).

    Messages are Avro; we decode them with fastavro (already installed as a fink-client
    dependency). NOTE: this assumes container-framed Avro (schema embedded per message, as
    ZTF-derived streams use). If ALeRCE sends schemaless/Confluent-wire Avro instead, the
    decode in listen() must switch to fastavro.schemaless_reader with ALeRCE's published
    schema — finalize against a real message once credentials are available.

    Configuration example (settings.py ALERT_STREAMS entry)::

        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.alerce.AlerceAlertStream',
            'OPTIONS': {
                'ALERCE_GROUP_ID': os.environ.get('ALERCE_GROUP_ID', ''),
                'ALERCE_USERNAME': os.environ.get('ALERCE_USERNAME', ''),
                'ALERCE_PASSWORD': os.environ.get('ALERCE_PASSWORD', ''),
                'TOPIC_PREFIX': 'lc_classifier',
                'TOPIC_HANDLERS': {
                    'lc_classifier': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                },
            },
        }
    """
    configuration_class = AlerceConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'alerce'

    def listen(self) -> None:
        """Consume ALeRCE alerts from Kafka and dispatch to the configured handler.

        Subscribes by regex to the rolling daily topics and polls indefinitely. Avro
        message values are decoded with fastavro; each decoded record is dispatched to the
        single handler keyed on TOPIC_PREFIX. Errors propagate to AlertStream.run(), which
        logs and restarts listen() — connection resilience lives there, not here.
        """
        prefix = self.config.TOPIC_PREFIX
        # The literal daily topic isn't in TOPIC_HANDLERS, so resolve the handler by the
        # prefix key (falling back to the sole configured handler for robustness).
        handler = self.alert_handler.get(prefix) or next(iter(self.alert_handler.values()))

        alerce_kafka_consumer = Consumer({
            'bootstrap.servers': self.config.ALERCE_KAFKA_SERVER,
            'group.id': self.config.ALERCE_GROUP_ID,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'SCRAM-SHA-256',
            'sasl.username': self.config.ALERCE_USERNAME,
            'sasl.password': self.config.ALERCE_PASSWORD,
            # Demo semantics: show only current alerts (the table is a latest-only FIFO).
            'auto.offset.reset': 'latest',
        })
        # confluent_kafka treats a leading '^' as a subscription regex, matching every
        # current and future '{prefix}_YYYYMMDD' topic.
        alerce_kafka_consumer.subscribe([f'^{prefix}_'])
        logger.info(f'{self.STREAM_NAME}: subscribed to ^{prefix}_ on {self.config.ALERCE_KAFKA_SERVER}')

        try:
            while True:
                msg = alerce_kafka_consumer.poll(timeout=20)
                if msg is None:
                    continue  # poll timeout with no message, retry
                if msg.error():
                    logger.warning(f'{self.STREAM_NAME} Kafka error: {msg.error()}')
                    continue
                topic = msg.topic()
                logger.info(f'{self.STREAM_NAME} received message on {topic}')
                # ALeRCE messages are Avro; fastavro.reader handles container-framed Avro
                # (schema embedded), yielding one record per alert.
                for record in avro_reader(io.BytesIO(msg.value())):
                    handler(record, alert_stream=self, topic=topic)
        finally:
            alerce_kafka_consumer.close()

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Extract common fields from an ALeRCE classifier record.

        ALeRCE classifier outputs are keyed by object id and candidate id, plus features
        and class probabilities; coordinates and magnitude may or may not be present
        depending on the classifier. We populate whatever is available. Field names follow
        ALeRCE conventions: 'oid' (object id), 'candid' (candidate id), 'meanra'/'meandec',
        'lastmjd' (MJD). These should be confirmed against a real message.

        Args:
            raw_alert: Avro-decoded ALeRCE record (a dict).
            topic: The (daily) topic the alert arrived on.

        Returns:
            NormalizedAlert with the available fields populated.
        """
        record = dict(raw_alert)  # fastavro yields plain dicts; copy for raw_payload safety

        object_id = record.get('oid') or record.get('aid')
        candid = record.get('candid')

        # Timestamp from the object's most recent detection MJD, if present.
        mjd = record.get('lastmjd') or record.get('firstmjd')
        timestamp = _mjd_to_datetime(mjd) if mjd is not None else None

        ra = record.get('meanra', record.get('ra'))
        dec = record.get('meandec', record.get('dec'))
        magnitude = record.get('magpsf')

        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            observation_time=timestamp,
            published_time=None,
            alert_id=str(candid) if candid is not None else str(object_id or ''),
            object_id=str(object_id) if object_id else None,
            ra=float(ra) if ra is not None else None,
            dec=float(dec) if dec is not None else None,
            magnitude=float(magnitude) if magnitude is not None else None,
            raw_payload=record,
        )
