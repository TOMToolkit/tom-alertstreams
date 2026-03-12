from __future__ import annotations

import abc
import logging
from datetime import datetime
from typing import Any, Callable, ClassVar

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from pydantic import BaseModel, ValidationError

from tom_alertstreams.models import Alert

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---------------------------------------------------------------------------
# Typed alert intermediate
# ---------------------------------------------------------------------------

class NormalizedAlert(BaseModel):
    """Typed intermediate produced by AlertStream.normalize_alert().

    Every AlertStream subclass's normalize_alert() method returns a NormalizedAlert.
    Handlers that need to persist alerts (e.g. save_alert_to_database) rely on this
    type so that one handler function works across all streams.

    All fields except stream_name, alert_id, and timestamp are optional because
    not every stream provides the same metadata. The raw_payload preserves the full
    original alert object (serialized to a dict) for handlers that need
    stream-specific data not captured in the normalised fields.

    Fields:
        stream_name: Short canonical name of the stream (from AlertStream.STREAM_NAME).
        topic: Kafka topic the alert arrived on. Empty string if not available.
        timestamp: UTC datetime of the alert. Defaults to an empty string if unknown.
        alert_id: Stream-specific identifier for this alert.
        object_id: Astronomical object identifier (e.g. ZTF object name), if available.
        ra: Right ascension in decimal degrees, if available.
        dec: Declination in decimal degrees, if available.
        magnitude: Apparent magnitude, if available. ZTF streams populate this field.
        flux: Flux in nanojansky, if available. LSST streams populate this field.
        raw_payload: The full original alert as a plain dict for downstream use.
    """
    stream_name: str
    alert_id: str
    timestamp: datetime
    topic: str = ''
    object_id: str | None = None
    ra: float | None = None
    dec: float | None = None
    magnitude: float | None = None
    flux: float | None = None
    raw_payload: dict = {}


# ---------------------------------------------------------------------------
# Pydantic configuration models
# ---------------------------------------------------------------------------

class AlertStreamConfig(BaseModel):
    """Pydantic base configuration for all AlertStream subclasses.

    Inheriting from pydantic's BaseModel means that Pydantic validates required
    fields, coerces types, and raises descriptive field-level ValidationError
    messages when configuration is missing or incorrect — replacing the previous
    manual required_keys / allowed_keys validation approach.

    Every stream must declare its topic-to-handler mapping. Subclass configs
    inherit this and add stream-specific authentication and connection fields.
    Handler values are dotted-path strings; they are imported at AlertStream
    instantiation time by _process_topic_handlers().

    Example subclass:
        class MyStreamConfig(AlertStreamConfig):
            USERNAME: str
            PASSWORD: str
            START_POSITION: str = 'LATEST'
    """
    # Maps topic names to dotted-path strings of callable alert handler functions.
    # Example: {'my.topic': 'myapp.handlers.save_alert_to_database'}
    TOPIC_HANDLERS: dict[str, str]


# ---------------------------------------------------------------------------
# AlertStream abstract base class
# ---------------------------------------------------------------------------

class AlertStream(abc.ABC):
    """Abstract base class for Kafka alert stream implementations.

    To implement a new AlertStream subclass:

    1. Define a Pydantic config model (subclass of AlertStreamConfig) that declares
       all required and optional configuration fields. Pydantic handles validation
       and produces error messages for misconfigured streams.

    2. Set class variables:
         configuration_class = MyStreamConfig
         STREAM_NAME = 'mystream'           # short canonical name, written to Alert.stream_name

    3. Override normalize_alert(raw_alert, topic='') -> NormalizedAlert to extract
       stream-specific fields (ra, dec, magnitude, object_id, etc.). This returns
       a basic (Pydantic BaseModel subclass) NormaizedAlert that can be consistently
       used reguardless of which stream the alert came from.

    4. Implement listen() -> None. This method is not expected to return. It should:
         a. Connect to the Kafka stream using credentials from self.config
         b. Subscribe to the topics in self.config.TOPIC_HANDLERS
         c. Dispatch each incoming alert to its handler. Alert handlers shoud
            use the following function signiture:
              self.alert_handler[topic](raw_alert, alert_stream=self, topic=topic)
            Include any stream-specific extras as named keyword args (e.g. metadata=metadata).
            Handlers use **kwargs to absorb extras they do not need.

    The alert_stream=self argument provides dependency injection, allowing the
    same generic handler function (e.g. save_alert_to_database) to serve all
    streams because it receives the stream instance and can call the AlertStream's
    get_normalization_function() to obtain stream-specific parsing logic without
    knowing which stream it is working with.
    """
    # alertstream.AlertStreamConfig is a Pydantic BaseModel subclass
    # the settings.ALERT_STREAMS configuration dictionary will validated according
    # to the AlertStreamConfig subclass specified here.
    configuration_class: ClassVar[type[AlertStreamConfig]]

    # Short canonical name written to Alert.stream_name. Must be unique across
    # all configured streams. Used by the presenter registry in tables.py to
    # look up the appropriate AlertStreamPresenter for URL construction.
    STREAM_NAME: ClassVar[str]

    def __init__(self, **kwargs: Any) -> None:
        # read and validate the alertstream configuration
        self.config: AlertStreamConfig = self.configuration_class(**kwargs)

        # Convert TOPIC_HANDLERS dotted-path strings to callable functions.
        self.alert_handler: dict[str, Callable] = self._process_topic_handlers()

    def _get_stream_classname(self) -> str:
        """Return the qualified class name of this AlertStream subclass.

        This is just a way to get the name of the subclass.
        """
        return type(self).__qualname__

    def _process_topic_handlers(self) -> dict[str, Callable]:
        """Import and return handler callables from the TOPIC_HANDLERS configuration.

        This is a step in the AlertStream instanciation:

        In settings.py, the configuration dictionary TOPIC_HANDLER dictionary
        for each stream maps a topic to a dotted-path string specifying the alert
        handler for that topic's alerts. This method converts the dotted-path string
        to a Callable.

        Returns:
            A dict mapping topic name strings to callable handler functions.

        Raises:
            ImproperlyConfigured: if any handler dotted-path cannot be imported.
        """
        alert_handler = {}
        for topic, callable_string in self.config.TOPIC_HANDLERS.items():
            try:
                alert_handler[topic] = import_string(callable_string)
            except ImportError as err:
                msg = (
                    f'Could not import handler "{callable_string}" for topic "{topic}" '
                    f'in {self._get_stream_classname()}. Check your TOPIC_HANDLERS setting. '
                    f'Error: {err}'
                )
                raise ImproperlyConfigured(msg)
        return alert_handler

    @abc.abstractmethod
    def normalize_alert(self, raw_alert: Any, topic: str = '') -> NormalizedAlert:
        """Convert a raw stream-specific alert object to a NormalizedAlert.

        Every AlertStream subclass must implement this method. The returned
        NormalizedAlert is a Pydantic BaseModel subclass. As such, it is typed
        and validated (vs a dictionary of unvalidated key and values).

        While the stream is known by virtue of the AlertSteam subclass implementing
        this method, the topic argument is provided by the listen() loop and should
        be passed through to the NormalizedAlert.topic field.

        Args:
            raw_alert: The stream-specific alert object received from listen().
            topic: The Kafka topic this alert arrived on.

        Returns:
            A NormalizedAlert with as many fields populated as the stream supports.
        """
        pass  # implement me

    def get_normalization_function(self) -> Callable:
        """Return the normalization callable for this stream.

        By default returns self.normalize_alert. Override this method to substitute
        a completely different normalization implementation without subclassing, for
        example to use a function defined outside this class/subclass (e.g. in your
        `custom_code` or other INSTALLED_APP).)

        Returns:
            A callable with signature (raw_alert, topic='') -> NormalizedAlert.
        """
        return self.normalize_alert

    @abc.abstractmethod
    def listen(self) -> None:
        """Consume alerts from the stream indefinitely.

        This method is not expected to return. Implementations should:
          1. Connect to the Kafka stream using credentials from self.config
          2. Subscribe to the topics in self.config.TOPIC_HANDLERS (the topic
             keys are also available via self.alert_handler.keys())
          3. For each incoming alert, dispatch to the handler using the unified
             calling convention:

               self.alert_handler[topic](raw_alert, alert_stream=self, topic=topic)

          The alert_stream=self argument injects this AlertStream instance so that
          handlers can call get_normalization_function() without knowing the specific
          stream type (dependency injection via keyword argument).

          Pass any additional stream-specific context as named keyword arguments:

               self.alert_handler[topic](raw_alert, alert_stream=self, topic=topic,
                                         metadata=metadata)  # Hopskotch example

          Handlers absorb extras they do not need via **kwargs.
        """
        pass # implement me


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------

def get_alert_stream_classes() -> list[type[AlertStream]]:
    """Return the imported class for each configured alert stream.

    Imports each active stream class from settings.ALERT_STREAMS by its dotted
    NAME path. Does NOT instantiate the classes — this is the lightweight
    alternative to get_alert_streams() for when you only need access to class
    attributes (e.g. STREAM_NAME).

    Streams that are inactive (ACTIVE=False) are skipped. Streams that fail to
    import are logged and skipped so a single misconfigured entry doesn't break
    the caller.
    """
    classes: list[type[AlertStream]] = []
    for stream_config in getattr(settings, 'ALERT_STREAMS', []):
        if not stream_config.get('ACTIVE', True):
            continue
        try:
            classes.append(import_string(stream_config['NAME']))
        except (ImportError, AttributeError, KeyError) as exc:
            logger.warning(
                'get_alert_stream_classes: could not import %s: %s',
                stream_config.get('NAME'), exc,
            )
    return classes


def get_default_alert_streams() -> list[AlertStream]:
    """Return the AlertStream instances configured in settings.ALERT_STREAMS.

    `get_alert_streams()` is the general function. Here, we call that function
    and pass in the configuration dictionary from settings.ALERT_STREAMS.

    Raises:
        ImproperlyConfigured: if ALERT_STREAMS is not defined in settings, or if
            any stream's configuration is invalid.
    """
    try:
        return get_alert_streams(settings.ALERT_STREAMS)
    except AttributeError as err:
        raise ImproperlyConfigured(
            f'ALERT_STREAMS is not configured in settings.py: {err}'
        )


def get_alert_streams(alert_stream_configs: list) -> list[AlertStream]:
    """Instantiate and return AlertStream objects from a list of config dicts.

    Use this function if your alert streams are configured somewhere other
    than settings.ALERT_STREAMS.

    Each config dict must have:
        NAME (str): dotted-path to an AlertStream subclass
        OPTIONS (dict): keyword arguments passed to the subclass constructor
        ACTIVE (bool, optional): if False, skip this stream (defaults to True)

    Args:
        alert_stream_configs: List of configuration dictionaries from ALERT_STREAMS.

    Returns:
        A list of instantiated AlertStream subclass objects for active streams.

    Raises:
        ImproperlyConfigured: if a NAME cannot be imported, or if Pydantic
            validation of OPTIONS fails for any stream.
    """
    alert_streams = []
    for alert_stream_config in alert_stream_configs:
        if not alert_stream_config.get('ACTIVE', True):
            logger.debug(
                f'get_alert_streams: skipping inactive stream: {alert_stream_config["NAME"]}'
            )
            continue

        # Dynamically import the AlertStream subclass by dotted-path name.
        try:
            klass = import_string(alert_stream_config['NAME'])
        except ImportError as err:
            raise ImproperlyConfigured(
                f'Could not import AlertStream class "{alert_stream_config["NAME"]}". '
                f'Check the NAME key in your ALERT_STREAMS setting. Error: {err}'
            )

        # Pydantic validates required fields in OPTIONS and raises ValidationError
        # with field-level detail if anything is missing or mistyped.
        try:
            alert_stream: AlertStream = klass(**alert_stream_config.get('OPTIONS', {}))
        except ValidationError as err:
            raise ImproperlyConfigured(
                f'Configuration for {alert_stream_config["NAME"]} is invalid:\n{err}'
            )

        alert_streams.append(alert_stream)

    return alert_streams


# ---------------------------------------------------------------------------
# Here's a handler that puts an alert (after normalization) into the the Alerts table
# ---------------------------------------------------------------------------

def save_alert_to_database(raw_alert: Any, alert_stream: AlertStream, **kwargs: Any) -> Alert | None:
    """Persist a raw alert to the database after normalization.

    This could be used as example code for an alert handler that might
    handle alerts from multiple streams with different formats. The normalization
    step (and injected alert stream) make this possible.

    `alert_stream` is injected by AlertStream.listen() so that this one generic
    handler can serve all streams — each stream's normalize_alert() provides
    stream-specific field extraction without this function needing to know
    which stream it is working with (dependency injection via keyword argument).

    Args:
        raw_alert: The stream-specific alert object received from listen().
        alert_stream: The AlertStream instance; provides the normalization function.
        **kwargs: Stream-specific extras (e.g., topic, metadata from Hopskotch);
            the 'topic' kwarg, if present, is forwarded to normalize_alert() so the
            NormalizedAlert.topic field is populated correctly.

    Returns:
        The created Alert instance, or None if normalization or save fails.
    """
    # get the AlertStream subclass-specific normaization function
    normalize = alert_stream.get_normalization_function()
    topic: str = kwargs.get('topic', '')
    try:
        normalized_alert: NormalizedAlert = normalize(raw_alert, topic=topic)
        # NormalizedAlert is a Pydantic BaseModel subclass with a model_dump method
        alert = Alert.objects.create(**normalized_alert.model_dump())
        return alert
    except Exception as ex:
        logger.error(f'save_alert_to_database: failed to save alert: {ex}'
                     f'raw_alert: {raw_alert}')
        return None
