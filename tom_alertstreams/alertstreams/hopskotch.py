from __future__ import annotations

import logging
import re
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, ClassVar

from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone as tz

from hop import Stream
from hop.auth import Auth
from hop.io import Metadata, StartPosition, list_topics
from hop.models import JSONBlob

from tom_alertstreams.alertstreams.alertstream import AlertStream, AlertStreamConfig, NormalizedAlert

logger = logging.getLogger(__name__)


class HopskotchConfig(AlertStreamConfig):
    """Pydantic configuration model for HopskotchAlertStream.

    Inherits from AlertStreamConfig (a Pydantic BaseModel), so Pydantic validates
    that URL, GROUP_ID, USERNAME, and PASSWORD are present.

    Fields:
        URL: Hopskotch broker URL (required). Typically 'kafka://kafka.scimma.org/'.
        GROUP_ID: Kafka consumer group ID (required). Must be prefixed with your
            SCiMMA username to match SCiMMA Auth permissions. Format:
            '<scimma_username>-<unique-suffix>'.
        USERNAME: SCiMMA Auth username (required). Obtain at https://hop.scimma.org/.
        PASSWORD: SCiMMA Auth password (required). Obtain at https://hop.scimma.org/.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Maps Hopskotch topic names
            to handler dotted-paths. Supports wildcards: '*' matches all public topics;
            'prefix.*' matches topics whose names match the regex.
        START_POSITION: Where to start consuming. 'LATEST' (default) means only new
            messages; 'EARLIEST' replays from the beginning of the topic's retention window.
    """
    URL: str
    GROUP_ID: str
    USERNAME: str
    PASSWORD: str
    START_POSITION: str = 'LATEST'


class HopskotchAlertStream(AlertStream):
    """AlertStream implementation for SCiMMA Hopskotch (hop.scimma.org).

    Hopskotch is a Kafka-based message bus for time-domain astronomy operated by
    SCiMMA (https://scimma.org). It carries alerts from multiple sources including
    HERMES and GW notices. This implementation uses the hop-client Python library.

    Special topic support:
      - '*' in TOPIC_HANDLERS subscribes to ALL public topics via the wildcard handler.
      - 'prefix.*' patterns subscribe to all public topics matching the regex.
      - Direct topic names take priority over wildcard matches.

    Configuration example (settings.py ALERT_STREAMS entry):
        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.hopskotch.HopskotchAlertStream',
            'OPTIONS': {
                'URL': 'kafka://kafka.scimma.org/',
                'GROUP_ID': os.environ.get('SCIMMA_AUTH_USERNAME', '') + '-my-tom',
                'USERNAME': os.environ.get('SCIMMA_AUTH_USERNAME', ''),
                'PASSWORD': os.environ.get('SCIMMA_AUTH_PASSWORD', ''),
                'START_POSITION': 'LATEST',         # optional
                'TOPIC_HANDLERS': {
                    'sys.heartbeat': 'tom_alertstreams.alertstreams.hopskotch.heartbeat_handler',
                    'hermes.*': 'tom_alertstreams.alertstreams.hopskotch.alert_logger',
                },
            },
        }

    See https://hop-client.readthedocs.io/ for hop-client documentation.
    """
    configuration_class = HopskotchConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'hopskotch'

    # Seconds between checks for new public topics when wildcard subscriptions are active.
    PUBLIC_TOPIC_CHECK_INTERVAL: ClassVar[int] = 300

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        logger.debug(f'HopskotchAlertStream.__init__() config: {self.config}')

        # Fetch public topics and build the stream URL up front — if the configuration
        # is broken, we want to fail here (before listen() spawns in its own thread)
        # so the error is visible immediately at startup.
        self.public_topics = self.get_all_public_topics()
        self.stream_url = self.get_stream_url()

        start_position = StartPosition.LATEST
        if self.config.START_POSITION == 'EARLIEST':
            start_position = StartPosition.EARLIEST
        self.stream = self.get_stream(start_position)

    def get_all_public_topics(self) -> list[str]:
        """Return the current list of publicly-readable Hopskotch topic names.

        Queries the Hopskotch broker directly via the hop-client. Filters out
        internal Kafka topics (those starting with '__' or containing no '.').

        Returns:
            List of topic name strings available on the Hopskotch broker.
        """
        hop_auth = Auth(self.config.USERNAME, self.config.PASSWORD)
        logger.info('HopskotchAlertStream: fetching public topics from SCiMMA Auth.')
        all_topics = list_topics(self.config.URL, hop_auth)
        # Exclude internal Kafka topics (__consumer_offsets, etc.) and topics
        # without a namespace separator (no '.') which are internal by convention.
        publicly_readable = [
            topic for topic in all_topics.keys()
            if not (topic.startswith('__') and topic.count('.') == 0)
        ]
        logger.debug(f'HopskotchAlertStream public topics: {publicly_readable}')
        return publicly_readable

    def get_stream_url(self) -> str:
        """Build the Hopskotch stream URL with topics appended.

        Hopskotch requires topics to be specified in the URL rather than via a
        subscribe() call. This method resolves wildcard patterns against the
        current public topic list before building the URL.

        Returns:
            Fully-qualified Hopskotch stream URL with topics.

        Raises:
            ImproperlyConfigured: if TOPIC_HANDLERS is empty (hop requires ≥1 topic).
        """
        if not self.config.TOPIC_HANDLERS:
            raise ImproperlyConfigured(
                'HopskotchAlertStream requires at least one entry in TOPIC_HANDLERS. '
                'Check ALERT_STREAMS in settings.py.'
            )

        base_url = self.config.URL.rstrip('/') + '/'

        # Expand wildcard patterns against the public topic list.
        specified = set(self.config.TOPIC_HANDLERS.keys())
        if '*' in specified:
            # Full wildcard: subscribe to every public topic.
            specified = specified | set(self.public_topics)
        else:
            # Partial wildcards: 'hermes.*' → all public topics matching the regex.
            for pattern in list(specified):
                if '*' in pattern:
                    specified |= {t for t in self.public_topics if re.match(pattern, t)}

        # Remove the wildcard placeholders — real topic names only.
        concrete_topics = [t for t in specified if '*' not in t]
        hopskotch_url = base_url + ','.join(concrete_topics)
        logger.debug(f'HopskotchAlertStream stream URL: {hopskotch_url}')
        return hopskotch_url

    def get_stream(self, start_position: StartPosition = StartPosition.LATEST) -> Stream:
        """Create and return a hop-client Stream object.

        Args:
            start_position: Where to start consuming (LATEST or EARLIEST).

        Returns:
            An authenticated hop.Stream ready for use in listen().
        """
        hop_auth = Auth(self.config.USERNAME, self.config.PASSWORD)
        return Stream(auth=hop_auth, start_at=start_position)

    def normalize_alert(self, raw_alert: Any, topic: str = '') -> NormalizedAlert:
        """Extract common fields from a Hopskotch JSONBlob alert.

        Hopskotch delivers alerts as hop.models.JSONBlob objects (or other hop model
        types). The .content attribute holds the parsed dict. Since Hopskotch carries
        alerts from many sources (HERMES, GW notices, etc.), only a generic extraction
        is possible at this level; science-specific handlers should subclass and override.

        Args:
            raw_alert: A hop.models.JSONBlob (or similar hop model) object.
            topic: The Hopskotch topic the alert arrived on.

        Returns:
            NormalizedAlert with stream_name, topic, and raw_payload populated.
        """
        content = getattr(raw_alert, 'content', None) or {}
        alert_id = str(content.get('message_id', id(raw_alert)))
        return NormalizedAlert(
            stream_name=self.STREAM_NAME,
            topic=topic,
            observation_time=None,  # generic Hopskotch messages carry no parsed obs/publish time
            published_time=None,
            alert_id=alert_id,
            raw_payload=content if isinstance(content, dict) else {},
        )

    def listen(self) -> None:
        """Consume Hopskotch alerts and dispatch to configured topic handlers.

        Runs an infinite loop reading from the Hopskotch stream. Periodically checks
        for new public topics (every PUBLIC_TOPIC_CHECK_INTERVAL seconds) and restarts
        the stream if the topic list has changed, so new topics are picked up without
        a manual restart.

        Topic matching priority:
          1. Exact topic name match
          2. Regex wildcard pattern match (e.g. 'hermes.*')
          3. Catch-all '*' handler

        Handler calling convention:
            handler(alert, alert_stream=self, topic=topic, metadata=metadata)
        Handlers absorb extras they do not need via **kwargs.
        """
        last_check_time = tz.now()
        while True:
            try:
                logger.info(
                    f'HopskotchAlertStream: opening stream {self.stream_url} '
                    f'with group_id: {self.config.GROUP_ID}'
                )
                with self.stream.open(self.stream_url, 'r', group_id=self.config.GROUP_ID) as src:
                    for alert, metadata in src.read(metadata=True):
                        topic = metadata.topic

                        # Determine the handler: exact match, then regex wildcard, then '*'.
                        if topic in self.alert_handler:
                            handler = self.alert_handler[topic]
                        else:
                            handler = None
                            for pattern, candidate in self.alert_handler.items():
                                if pattern != '*' and '*' in pattern and re.match(pattern, topic):
                                    handler = candidate
                                    break
                            if handler is None:
                                handler = self.alert_handler.get('*')

                        if handler is not None:
                            # Unified convention + Hopskotch-specific metadata kwarg.
                            handler(alert, alert_stream=self, topic=topic, metadata=metadata)
                        else:
                            logger.error(
                                f'HopskotchAlertStream: alert from topic "{topic}" received '
                                f'but no handler matched. Configured: {list(self.alert_handler.keys())}'
                            )

                        # Periodically refresh public topics to pick up new ones automatically.
                        if (tz.now() - last_check_time).total_seconds() > self.PUBLIC_TOPIC_CHECK_INTERVAL:
                            last_check_time = tz.now()
                            fresh_topics = self.get_all_public_topics()
                            if set(fresh_topics) != set(self.public_topics):
                                logger.info('HopskotchAlertStream: new public topics found — restarting stream.')
                                self.public_topics = fresh_topics
                                self.stream_url = self.get_stream_url()
                                break  # Exit inner loop; outer while True reopens the stream.

            except Exception as ex:
                logger.error(f'HopskotchAlertStream.listen: {ex}')
                logger.error(traceback.format_exc())


def heartbeat_handler(heartbeat: JSONBlob, **kwargs: Any) -> None:
    """Example handler for the Hopskotch sys.heartbeat topic.

    Logs every 300th heartbeat to avoid flooding the log. Copy into your TOM's
    custom_code app and modify as needed.

    The **kwargs signature absorbs alert_stream, topic, metadata, and any other
    extras passed by the unified handler calling convention.

    Args:
        heartbeat: A hop.models.JSONBlob with a 'timestamp' and 'count' in .content.
        **kwargs: Absorbs alert_stream, topic, metadata, and stream-specific extras.
    """
    content: dict = heartbeat.content
    timestamp = datetime.fromtimestamp(content['timestamp'] / 1e6, tz=timezone.utc)
    if content.get('count', 0) % 300 == 0:
        logger.info(f'Hopskotch heartbeat at {timestamp.isoformat()}: {content}')


def alert_logger(alert: JSONBlob, **kwargs: Any) -> None:
    """Example alert handler for HopskotchAlertStream.

    Logs the topic and alert UUID. Copy into your TOM's custom_code app and
    modify as needed.

    The **kwargs signature absorbs alert_stream, topic, metadata, and any other
    extras passed by the unified handler calling convention. Access metadata via
    kwargs.get('metadata') if needed.

    Args:
        alert: A hop.models.JSONBlob (or other hop model type).
        **kwargs: Absorbs alert_stream, topic, metadata, and stream-specific extras.
    """
    metadata: Metadata | None = kwargs.get('metadata')
    alert_uuid = None
    if metadata is not None:
        uuid_tuple = next((h for h in metadata.headers if h[0] == '_id'), None)
        if uuid_tuple:
            alert_uuid = uuid.UUID(bytes=uuid_tuple[1])
    topic = kwargs.get('topic', getattr(metadata, 'topic', 'unknown') if metadata else 'unknown')
    logger.info(f'Hopskotch alert (uuid={alert_uuid}) on topic "{topic}": {alert}')