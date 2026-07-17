from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

import pittgoogle
from pydantic import model_validator

from tom_alertstreams.alertstreams.alertstream import (
    AlertStream, AlertStreamConfig, NormalizedAlert, _jd_to_datetime, is_in_minute_window,
    save_alert_to_database,
)

logger = logging.getLogger(__name__)

# Pitt-Google publishes its public topics in this Google Cloud project.
PITTGOOGLE_PROJECT_DEFAULT: str = pittgoogle.ProjectIds().pittgoogle  # 'ardent-cycling-243415'

# Mock data constants — chosen to be unmistakably non-astronomical:
# (0, 0) is not a real survey pointing; 99.0 is the astronomical sentinel for "no data".
_MOCK_RA = 0.0
_MOCK_DEC = 0.0
_MOCK_MAGNITUDE = 99.0
INTER_ALERT_SLEEP_MIN = 3600  # sixty minutes
INTER_ALERT_SLEEP_MAX = 3600


def _schema_for_topic(topic_name: str) -> str:
    """Return the pittgoogle alert schema used to deserialize a topic's messages."""
    if topic_name.startswith('lsst'):
        return 'lsst'
    if topic_name.startswith('lvk'):
        return 'lvk'
    return 'ztf'


# ---------------------------------------------------------------------------
# Pydantic configuration models
# ---------------------------------------------------------------------------

class PittGoogleMockConfig(AlertStreamConfig):
    """Pydantic configuration model for PittGoogleMockAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    No additional fields needed for mock data generation.
    """
    pass


class PittGoogleConfig(AlertStreamConfig):
    """Pydantic configuration for the real PittGoogleAlertStream.

    Pitt-Google distributes alerts over Google Cloud Pub/Sub (not Kafka). The topic
    is the single key in TOPIC_HANDLERS (as with GCN/Hopskotch); a Pub/Sub subscription
    binds to exactly one topic, so each entry handles one topic. GCP authentication is
    read from the environment by pittgoogle — not configured here:
        GOOGLE_CLOUD_PROJECT          the project that owns the subscription
        GOOGLE_APPLICATION_CREDENTIALS path to the service-account key JSON

    Fields:
        PITTGOOGLE_PROJECT: Google Cloud project that publishes the topic. Defaults to
            Pitt-Google's public project ('ardent-cycling-243415').
        PITTGOOGLE_SUBSCRIPTION: Name of the (persistent) subscription to create in our
            project. Optional; defaults to the topic name, so two entries that read
            different topics get distinct subscriptions automatically.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Must contain exactly one topic.
    """
    PITTGOOGLE_PROJECT: str = PITTGOOGLE_PROJECT_DEFAULT
    PITTGOOGLE_SUBSCRIPTION: str | None = None

    @model_validator(mode='after')
    def _check_single_topic(self) -> PittGoogleConfig:
        """Require exactly one topic per entry.

        A Pub/Sub subscription binds to exactly one topic and pittgoogle.Consumer.stream()
        consumes a single subscription, so a PittGoogleAlertStream instance handles one
        topic. To consume several Pitt-Google topics, configure several ALERT_STREAMS
        entries (one per topic) — readstreams runs each in its own thread.
        """
        if len(self.TOPIC_HANDLERS) != 1:
            raise ValueError(
                f'PittGoogleAlertStream requires exactly one topic per entry (one Pub/Sub '
                f'subscription binds to one topic); got {len(self.TOPIC_HANDLERS)}: '
                f'{list(self.TOPIC_HANDLERS)}. Configure a separate ALERT_STREAMS entry per topic.'
            )
        return self


# ---------------------------------------------------------------------------
# Throttling alert handler for high-rate Pitt-Google topics (ztf-loop, ztf-alerts)
# ---------------------------------------------------------------------------

def save_pittgoogle_throttled(raw_alert: Any, alert_stream: AlertStream, **kwargs: Any) -> Any:
    """Alert handler that saves only ~one alert per UTC minute, dropping (but acking) the rest.

    Throttles a high-rate Pitt-Google topic to ~one saved alert per minute. Both ZTF topics
    need this: ztf-loop replays a recent alert ~1/sec, and ztf-alerts is the full ZTF firehose
    (tens/sec when ZTF observes, plus any accumulated subscription backlog). Saving every alert
    would swamp the demo's SQLite DB and — worse — outrun Pub/Sub's lease deadline: the client
    can't ack fast enough, leases expire, and Pub/Sub redelivers ("Dropping N items because they
    were leased too long"). Dropping-but-acking the rest keeps acks fast, so the consumer stays
    current and any backlog drains. The demo only displays the recent few per topic anyway.
    Mirrors gcn.save_heartbeat_hourly; a minute cadence here keeps the live ZTF feed visibly
    fresh. To change the cadence, swap is_in_minute_window for is_in_hourly_window.

    The throttle clock is the Pub/Sub publishTime (raw_alert.msg.publish_time) gated by
    is_in_minute_window. (Deliberately distinct from the alert's published_time field, which
    records the survey's upstream kafka.timestamp; see PittGoogleAlertStream.normalize_alert.)

    Args:
        raw_alert: The pittgoogle.Alert delivered by the Consumer.
        alert_stream: The AlertStream instance (injected; forwarded to save_alert_to_database).
        **kwargs: Stream-specific extras (e.g. topic); forwarded unchanged.

    Returns:
        The saved Alert, or None if this alert was dropped (or the save failed). The
        Consumer acks the Pub/Sub message either way (see PittGoogleAlertStream.listen).
    """
    # Throttle on the Pub/Sub publishTime; fall back to receipt time if it's absent.
    message = getattr(raw_alert, 'msg', None)
    publish_time = getattr(message, 'publish_time', None)
    if publish_time is None:
        publish_time = datetime.now(timezone.utc)
    timestamp_ms = int(publish_time.timestamp() * 1000)
    if is_in_minute_window(timestamp_ms):
        return save_alert_to_database(raw_alert, alert_stream=alert_stream, **kwargs)
    return None  # outside the minute's first second — drop this alert


# ---------------------------------------------------------------------------
# Mock Pitt-Google stream (demo fallback without GCP credentials)
# ---------------------------------------------------------------------------

class PittGoogleMockAlertStream(AlertStream):
    """Mock Pitt-Google AlertStream that generates obviously-fake alerts.

    Provides a demo fallback when Google Cloud credentials are not available. Uses the
    same mock pattern as the other stub streams (sentinel coordinates, sentinel magnitude).
    STREAM_NAME='pittgoogle' so it occupies the same dashboard slot as the real stream.
    """
    configuration_class = PittGoogleMockConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'pittgoogle'
    IS_MOCK: ClassVar[bool] = True  # generates fake alerts, not a real Pitt-Google feed

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock Pitt-Google alert dict to a NormalizedAlert.

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
        """Generate mock Pitt-Google alerts and dispatch to configured topic handlers.

        Loops indefinitely, emitting one mock alert per iteration. Topics are
        round-robined if multiple are configured.
        """
        counter = 0
        topics = list(self.config.TOPIC_HANDLERS.keys())

        # for this mock, generate fake alerts (rather than listen to the stream) endlessly
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
            logger.debug(f'PittGoogleMockAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(INTER_ALERT_SLEEP_MIN, INTER_ALERT_SLEEP_MAX))


# ---------------------------------------------------------------------------
# Real Pitt-Google stream (Google Cloud Pub/Sub)
# ---------------------------------------------------------------------------

class PittGoogleAlertStream(AlertStream):
    """Real Pitt-Google alert stream over Google Cloud Pub/Sub (not Kafka).

    Pitt-Google distributes alerts over Pub/Sub. We consume by creating a subscription in
    our own Google Cloud project (GOOGLE_CLOUD_PROJECT), attached to one of Pitt-Google's
    public topics, then streaming messages through a pittgoogle.Consumer.

    A subscription binds to exactly one topic, so each instance handles a single topic (the
    one key in TOPIC_HANDLERS). Configure one ALERT_STREAMS entry per topic — e.g. the
    throttled 'ztf-loop' (a ~1/sec firehose, handled by save_ztf_loop_hourly) and the live
    'ztf-alerts'. Both use STREAM_NAME='pittgoogle'; readstreams runs each in its own thread.

    Authentication is read from the environment by pittgoogle:
        GOOGLE_CLOUD_PROJECT           our project that owns the subscription
        GOOGLE_APPLICATION_CREDENTIALS path to the service-account key JSON

    Configuration example (settings.py ALERT_STREAMS entry)::

        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.pittgoogle.PittGoogleAlertStream',
            'OPTIONS': {
                'TOPIC_HANDLERS': {
                    'ztf-loop': 'tom_alertstreams.alertstreams.pittgoogle.save_ztf_loop_hourly',
                },
            },
        }
    """
    configuration_class = PittGoogleConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'pittgoogle'

    def listen(self) -> None:
        """Stream alerts from one Pitt-Google Pub/Sub topic and dispatch to its handler.

        Creates (or verifies) a persistent subscription to the topic, then opens a streaming
        pull via pittgoogle.Consumer. Consumer.stream() blocks and raises on a fatal error,
        so the AlertStream.run() supervisor handles reconnect — no retry loop here.
        """
        topic_name = next(iter(self.config.TOPIC_HANDLERS))  # one topic per entry (validated)
        subscription_name = self.config.PITTGOOGLE_SUBSCRIPTION or topic_name
        schema_name = _schema_for_topic(topic_name)

        subscription = pittgoogle.Subscription(
            subscription_name,
            topic=pittgoogle.Topic(topic_name, projectid=self.config.PITTGOOGLE_PROJECT),
            schema_name=schema_name,
        )
        subscription.touch()  # create-or-verify in our project; persistent (NOT deleted)
        logger.info(f'{self.STREAM_NAME}: streaming {topic_name} via subscription {subscription_name}')

        def message_callback(alert: pittgoogle.Alert) -> pittgoogle.pubsub.Response:
            # The Consumer builds Alert(msg) WITHOUT a schema_name; set it from the topic so the
            # schema-aware accessors (sourceid/objectid/dict) resolve in normalize_alert.
            alert.schema_name = schema_name
            # Dispatch to the configured handler (save_alert_to_database, or the ztf-loop
            # throttle). ALWAYS ack afterward: Pub/Sub redelivers un-acked messages, so a
            # throttled-and-dropped alert must be acked too, or it piles up as an un-acked
            # backlog. We swallow handler errors here so a single bad alert is acked + logged
            # rather than redelivered forever (save_alert_to_database already swallows its own).
            try:
                self.alert_handler[topic_name](alert, alert_stream=self, topic=topic_name)
            except Exception:
                logger.exception(f'{self.STREAM_NAME}: handler error on topic {topic_name}')
            return pittgoogle.pubsub.Response(ack=True)

        consumer = pittgoogle.Consumer(subscription, msg_callback=message_callback)
        # Open the streaming pull in the background, then block on its future ourselves.
        # pittgoogle's Consumer.stream(block=True) would instead `while True: sleep(60)` and
        # NEVER observe the streaming-pull future — so a terminal failure (auth loss, broker
        # drop, deleted subscription) would stall this stream silently, invisible to run().
        # Blocking on result() surfaces such a failure as an exception, which propagates to
        # run() for a supervised restart (the resilience the other streams get for free).
        consumer.stream(block=False)
        try:
            consumer.streaming_pull_future.result()  # blocks; raises on terminal failure
        finally:
            # Tear the pull/executor down before run() retries, so background threads don't
            # leak across restarts. Best-effort — the original failure should propagate.
            try:
                consumer.stop()
            except Exception:
                logger.debug(f'{self.STREAM_NAME}: error during consumer.stop()', exc_info=True)

    def normalize_alert(self, raw_alert: pittgoogle.Alert, topic: str = '') -> NormalizedAlert:
        """Map a Pitt-Google ZTF alert to a NormalizedAlert.

        Identity comes from the schema-aware accessors (these work for both ztf and lsst):
            sourceid -> alert_id    the detection that triggered the alert (ZTF candid)
            objectid -> object_id   the persistent astronomical object (ZTF objectId)
        Coordinates, magnitude, and observation time come from the ZTF 'candidate' dict —
        alert.ra/dec are None for ZTF, so we read candidate.ra/dec directly.

        Args:
            raw_alert: A pittgoogle.Alert (schema_name set by listen()'s callback).
            topic: The Pitt-Google topic the alert arrived on.

        Returns:
            NormalizedAlert with ZTF fields populated. (LSST diaSource handling is deferred
            until Pitt-Google's lsst-* topics go live.)
        """
        # Strip cutout stamp data (cutoutScience/Template/Difference) from raw_payload — large,
        # binary, compressed FITS that aren't JSON-serializable. Same idiom as Fink. (pittgoogle
        # 0.3.22 has no drop_cutouts(); a dict comprehension is predictable and keeps the rest.)
        raw_payload = {key: value for key, value in raw_alert.dict.items() if not key.startswith('cutout')}
        candidate = raw_payload.get('candidate', {})  # ZTF: nested per-detection measurements

        # published_time = the survey's upstream publish time. Pitt-Google ingests from ZTF's
        # Kafka and carries the original Kafka message timestamp (ms epoch) in the Pub/Sub
        # attributes as 'kafka.timestamp'. Distinct from the Pub/Sub publishTime that throttles
        # ztf-loop (save_ztf_loop_hourly).
        kafka_timestamp_ms = raw_alert.attributes.get('kafka.timestamp')
        published_time = (
            datetime.fromtimestamp(int(kafka_timestamp_ms) / 1000, tz=timezone.utc)
            if kafka_timestamp_ms else None
        )

        # observation_time from the detection's Julian Date.
        jd = candidate.get('jd')
        observation_time = _jd_to_datetime(jd) if jd is not None else None

        object_id = raw_alert.objectid
        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            observation_time=observation_time,
            published_time=published_time,
            alert_id=str(raw_alert.sourceid),
            object_id=str(object_id) if object_id is not None else None,
            ra=candidate.get('ra'),
            dec=candidate.get('dec'),
            magnitude=candidate.get('magpsf'),
            flux=None,
            raw_payload=raw_payload,
        )
