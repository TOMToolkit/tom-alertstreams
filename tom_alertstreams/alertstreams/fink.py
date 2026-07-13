from __future__ import annotations

import logging
import random
import time
import warnings
from datetime import datetime, timezone
from typing import Any, ClassVar, Literal

from fink_client.consumer import AlertConsumer, extract_id_from_lsst
from pydantic import model_validator

from tom_alertstreams.alertstreams.alertstream import (
    AlertStream, AlertStreamConfig, NormalizedAlert, _jd_to_datetime, _mjd_to_datetime,
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

class FinkMockConfig(AlertStreamConfig):
    """Pydantic configuration model for FinkMockAlertStream.

    Inherits TOPIC_HANDLERS from AlertStreamConfig (a Pydantic BaseModel).
    No additional fields needed for mock data generation.
    """
    pass


class FinkConfig(AlertStreamConfig):
    """Pydantic configuration for FinkAlertStream.

    Fields:
        FINK_USERNAME: Fink username (required). Obtained via Fink registration.
        FINK_PASSWORD: Fink password (optional — not all accounts require one).
        FINK_GROUP_ID: Kafka consumer group ID (required).
        FINK_SERVER: Kafka broker address for the chosen survey (required).
            ZTF: 'kafka-ztf.fink-broker.org:24499'
            LSST: 'kafka-lsst.fink-broker.org:24499'
        FINK_SURVEY: Survey name: 'ztf' or 'lsst'. Default: 'lsst'.
        TOPIC_HANDLERS: Inherited from AlertStreamConfig. Maps Fink topic names
            to handler dotted-paths. Topics are science-filter outputs specific
            to each survey (e.g. 'fink_sn_candidates_ztf' for ZTF).
    """
    FINK_USERNAME: str
    FINK_PASSWORD: str | None = None
    FINK_GROUP_ID: str
    FINK_SERVER: str
    FINK_SURVEY: Literal['ztf', 'lsst'] = 'lsst'

    @model_validator(mode='after')
    def _check_server_matches_survey(self) -> FinkConfig:
        """Warn if FINK_SERVER doesn't contain the survey name.

        Fink uses separate Kafka brokers per survey. A mismatch between
        FINK_SURVEY and FINK_SERVER likely means the wrong broker address.
        """
        if self.FINK_SURVEY not in self.FINK_SERVER:
            warnings.warn(
                f'FINK_SERVER ({self.FINK_SERVER}) does not contain '
                f'survey name ({self.FINK_SURVEY}). Please verify that the '
                f'broker address matches the chosen survey.',
                stacklevel=2,
            )
        return self

    @model_validator(mode='after')
    def _check_topics_match_survey(self) -> FinkConfig:
        """Fail loudly if a topic is tagged for the wrong survey.

        Fink science topics follow the '<filter>_<survey>' naming convention
        (e.g. 'fink_sn_candidates_ztf', 'fink_sn_candidates_lsst'). A topic
        ending in the opposite survey's suffix is unambiguously a misconfiguration:
        normalize_alert() branches on FINK_SURVEY, so a ZTF topic consumed under
        FINK_SURVEY='lsst' would hit the LSST branch, raise KeyError on the missing
        'diaObject' key, get swallowed by save_alert_to_database()'s broad except,
        and silently drop every alert.

        Unlike the server check (a substring heuristic that only warns), this is a
        hard error: the suffix mismatch is definitive, so we raise at config-load
        time. get_alert_streams() converts the resulting ValidationError into an
        ImproperlyConfigured, so the misconfiguration surfaces at startup rather
        than as silent data loss at runtime.

        Topics with no survey suffix (test topics, the mock's 'fink.test') are
        never flagged — only a topic carrying the *opposite* survey's suffix is.
        """
        opposite_survey = 'ztf' if self.FINK_SURVEY == 'lsst' else 'lsst'
        mismatched = [topic for topic in self.TOPIC_HANDLERS if topic.endswith(f'_{opposite_survey}')]
        if mismatched:
            raise ValueError(
                f"FINK_SURVEY is '{self.FINK_SURVEY}' but these topics are tagged for "
                f"'{opposite_survey}': {mismatched}. Fink topics follow the "
                f"'<filter>_<survey>' convention; a survey/topic mismatch routes alerts "
                f"to the wrong normalize_alert() branch and silently drops them."
            )
        return self


# ---------------------------------------------------------------------------
# Mock Fink stream (for demo use without Fink credentials)
# ---------------------------------------------------------------------------

class FinkMockAlertStream(AlertStream):
    """Mock Fink AlertStream that generates obviously-fake alerts.

    Provides a demo fallback when Fink credentials are not available.
    Uses the same mock pattern as the other stub streams (sentinel coordinates,
    sentinel magnitude, 6–7 minute sleep between alerts).

    The mock stream uses STREAM_NAME='fink' so it occupies the same dashboard
    slot as the real Fink stream would if it were the only one configured.
    """
    configuration_class = FinkMockConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'fink'
    IS_MOCK: ClassVar[bool] = True

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Map a mock Fink alert dict to a NormalizedAlert.

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
        """Generate mock Fink alerts and dispatch to configured topic handlers.

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
            logger.debug(f'FinkMockAlertStream: mock alert {object_id}')
            self.alert_handler[topic](mock_alert, alert_stream=self, topic=topic)
            time.sleep(random.uniform(INTER_ALERT_SLEEP_MIN, INTER_ALERT_SLEEP_MAX))


# ---------------------------------------------------------------------------
# Real Fink stream — supports both ZTF and LSST surveys
# ---------------------------------------------------------------------------

class FinkAlertStream(AlertStream):
    """Fink alert stream for ZTF and LSST alerts.

    Uses fink-client's AlertConsumer to receive Avro-encoded alerts from
    the Fink broker. The FINK_SURVEY config field determines which alert
    schema to expect (ZTF candidate-based vs LSST diaSource-based).

    Each FinkAlertStream instance reads from a single survey. To read both
    ZTF and LSST, configure two entries in ALERT_STREAMS with different
    FINK_SURVEY and FINK_SERVER values.

    Configuration example (settings.py ALERT_STREAMS entry)::

        {
            'ACTIVE': True,
            'NAME': 'tom_alertstreams.alertstreams.fink.FinkAlertStream',
            'OPTIONS': {
                'FINK_USERNAME': os.environ.get('FINK_USERNAME', ''),
                'FINK_GROUP_ID': os.environ.get('FINK_GROUPID', ''),
                'FINK_SERVER': os.environ.get('FINK_LSST_SERVER', ''),
                'FINK_SURVEY': 'lsst',
                'TOPIC_HANDLERS': {
                    'fink_sn_candidates_lsst': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
                },
            },
        }
    """
    configuration_class = FinkConfig  # type: ignore[assignment]
    STREAM_NAME: ClassVar[str] = 'fink'

    def listen(self) -> None:
        """Consume Fink alerts and dispatch to configured topic handlers.

        Creates an AlertConsumer as a context manager so the underlying Kafka
        consumer is properly closed on errors, KeyboardInterrupt, or normal exit.
        """
        # Build the consumer config dict directly from Django settings.
        # fink-client's README describes a CLI workflow (fink_client_register writes
        # credentials to ~/.finkclient/{survey}_credentials.yml, then fink_consumer
        # reads them via load_credentials()). We bypass that entirely — AlertConsumer
        # accepts a plain config dict, so we read from env vars via Django settings
        # instead. This avoids needing CLI setup in deployment (Docker, etc.).
        #
        # 'bootstrap.servers' is Kafka's term for the initial broker address used
        # to discover the cluster — we expose it as FINK_SERVER in our config.
        consumer_config: dict[str, str] = {
            'username': self.config.FINK_USERNAME,
            'group.id': self.config.FINK_GROUP_ID,
            'bootstrap.servers': self.config.FINK_SERVER,
        }
        if self.config.FINK_PASSWORD is not None:
            consumer_config['password'] = self.config.FINK_PASSWORD

        with AlertConsumer(
            topics=list(self.config.TOPIC_HANDLERS.keys()),
            config=consumer_config,
            survey=self.config.FINK_SURVEY,
        ) as fink_consumer:
            while True:
                topic, alert, key = fink_consumer.poll(timeout=30)
                if topic is None:
                    continue  # timeout with no message, retry
                logger.info(f'{self.STREAM_NAME} received alert on {topic}')
                self.alert_handler[topic](alert, alert_stream=self, topic=topic)

    def normalize_alert(self, raw_alert: dict, topic: str = '') -> NormalizedAlert:
        """Extract common fields from a Fink alert dict.

        Handles both ZTF and LSST alert schemas based on the FINK_SURVEY config.
        Fink alerts are plain dicts (decoded from Avro), so raw_payload is populated.

        ZTF alert structure:
            - objectId, candid at top level
            - candidate dict: jd, ra, dec, magpsf, fid, ...
        LSST alert structure:
            - diaSource dict: diaSourceId, midpointMjdTai, ra, dec, psFlux, ...
            - diaObject dict: diaObjectId (or mpc_orbits.designation for SSOs)

        Args:
            raw_alert: Alert dict decoded from Fink's Avro stream.
            topic: The Fink topic the alert was consumed from.

        Returns:
            NormalizedAlert with survey-appropriate fields populated.
        """
        # Strip cutout stamp data from raw_payload — these are compressed FITS images
        # (cutoutScience, cutoutTemplate, cutoutDifference) that are large, binary, and
        # not JSON-serializable. Keep everything else for debugging and data exploration.
        raw_payload = {k: v for k, v in raw_alert.items() if not k.startswith('cutout')}

        # Fink stamps its own processing times. brokerEndProcessTimestamp is when Fink
        # finished processing and published this alert — a naive-UTC ISO string. The gap
        # between it and observation_time is Fink's processing latency.
        published_str = raw_alert.get('brokerEndProcessTimestamp')
        published_time = (
            datetime.fromisoformat(published_str).replace(tzinfo=timezone.utc)
            if published_str else None
        )

        if self.config.FINK_SURVEY == 'ztf':
            # ZTF alert structure: top-level objectId/candid, nested candidate dict
            candidate = raw_alert.get('candidate', {})
            jd = candidate.get('jd')
            timestamp = _jd_to_datetime(jd) if jd is not None else None
            normalized_alert = NormalizedAlert(
                stream_name=self.STREAM_NAME,
                topic=topic,
                observation_time=timestamp,
                published_time=published_time,
                alert_id=str(raw_alert.get('candid', '')),
                object_id=raw_alert.get('objectId'),
                ra=candidate.get('ra'),
                dec=candidate.get('dec'),
                magnitude=candidate.get('magpsf'),
                flux=None,
                raw_payload=raw_payload,
            )
        else:
            # LSST alert structure: nested diaSource/diaObject dicts
            dia_source = raw_alert.get('diaSource', {})
            mjd_tai = dia_source.get('midpointMjdTai')
            timestamp = _mjd_to_datetime(mjd_tai) if mjd_tai is not None else None

            # extract_id_from_lsst (from fink-client) handles static objects
            # (diaObject.diaObjectId) vs moving objects (mpc_orbits.designation).
            object_id, _ = extract_id_from_lsst(raw_alert)

            normalized_alert = NormalizedAlert(
                stream_name=self.STREAM_NAME,
                topic=topic,
                observation_time=timestamp,
                published_time=published_time,
                alert_id=str(dia_source.get('diaSourceId', '')),
                object_id=str(object_id),
                ra=dia_source.get('ra'),
                dec=dia_source.get('dec'),
                magnitude=None,
                flux=dia_source.get('psFlux'),
                raw_payload=raw_payload,
            )
        return normalized_alert
