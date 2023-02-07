# tom-alertstreams

`tom-alertstreams` is a reusable TOM Toolkit app for listening to kafka streams.

`tom-alertstreams` provides a management command, `readstreams`. There are no `urlpatterns`,
no Views, and no templates. The `readstreams` management command reads the `settings.py` `ALERT_STREAMS`
configuration and starts listening to each configured Kafka stream. It is not expected
to return, and is intended to run along side your TOM's server component. The `ALERT_STREAMS`
configuration (see below) tells `readstreams` what streams to access, how to access them,
what topics to listen to, and what to do with messages that arrive on a given topic.

## Installation

1. Install the package into your TOM environment:
    ```bash
    pip install tom-alertstreams
   ```

2. In your project `settings.py`, add `tom_alertstreams` to your `INSTALLED_APPS` setting:

    ```python
    INSTALLED_APPS = [
        ...
        'tom_alertstreams',
    ]
    ```

At this point you can verify the installation by running `./manage.py` to list the available
management commands and see

   ```bash
   [tom_alertstreams]
       readstreams
   ```
in the output.

## Configuration

Each Kafka stream that your TOM listens to (via `readstreams`) will have a configuration dictionary
in your `settings.py` `ALERT_STREAMS`. `ALERT_STREAMS` is a list of configuration dictionaries, one
dictionary for each Kafka stream. Here's an example `ALERT_STREAMS` configuration for two Kafka streams:
[SCiMMA Hopskotch](https://scimma.org/hopskotch.html) and
[GCN Classic over Kafka](https://gcn.nasa.gov/quickstart).

```python
ALERT_STREAMS = [
    {
        'ACTIVE': True,
        'NAME': 'tom_alertstreams.alertstreams.hopskotch.HopskotchAlertStream',
        'OPTIONS': {
            'URL': 'kafka://kafka.scimma.org/',
            # The hop-client requires that the GROUP_ID prefix match the SCIMMA_AUTH_USERNAME
            'GROUP_ID': os.getenv('SCIMMA_AUTH_USERNAME', "") + '-' + 'uniqueidforyourapp12345',
            'USERNAME': os.getenv('SCIMMA_AUTH_USERNAME', None),
            'PASSWORD': os.getenv('SCIMMA_AUTH_PASSWORD', None),
            'TOPIC_HANDLERS': {
                'sys.heartbeat': 'tom_alertstreams.alertstreams.hopskotch.heartbeat_handler',
                'tomtoolkit.test': 'tom_alertstreams.alertstreams.hopskotch.alert_logger',
                'hermes.test': 'tom_alertstreams.alertstreams.hopskotch.alert_logger',
                'hermes.*': 'regex match public topics here, requires * handler to be defined'
                '*': 'default_handler_here'
            },
        },
    },
    {
        'ACTIVE': True,
        'NAME': 'tom_alertstreams.alertstreams.gcn.GCNClassicAlertStream',
        # The keys of the OPTIONS dictionary become (lower-case) properties of the AlertStream instance.
        'OPTIONS': {
            # see https://github.com/nasa-gcn/gcn-kafka-python#to-use for configuration details.
            'GCN_CLASSIC_CLIENT_ID': os.getenv('GCN_CLASSIC_CLIENT_ID', None),
            'GCN_CLASSIC_CLIENT_SECRET': os.getenv('GCN_CLASSIC_CLIENT_SECRET', None),
            'DOMAIN': 'gcn.nasa.gov',  # optional, defaults to 'gcn.nasa.gov'
            'CONFIG': {  # optional
                # 'group.id': 'tom_alertstreams-my-custom-group-id',
                # 'auto.offset.reset': 'earliest',
                # 'enable.auto.commit': False
            },
            'TOPIC_HANDLERS': {
                'gcn.classic.text.LVC_INITIAL': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
                'gcn.classic.text.LVC_PRELIMINARY': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
                'gcn.classic.text.LVC_RETRACTION': 'tom_alertstreams.alertstreams.alertstream.alert_logger',
            },
        },
    }
]
```

The configuration dictionary for each `AlertStream` subclass will contain these key-value pairs:
* `ACTIVE`: Boolean which tells `readstreams` to access this stream. Should be `True`, unless you want to
keep a configuration dictionary, but ignore the stream.
* `NAME`: The name of the `AlertStream` subclass that implements the interface to this Kafka stream. `tom_alertstreams`
will provide `AlertStream` subclasses for major astromical Kafka streams. See below for instructions on Subclassing
the `AlertStream` base class.
* `OPTIONS`: A dictionary of key-value pairs specific to the`AlertStream` subclass given by `NAME`. The doc string for
the `AlertStream` subclass should document what is expected. Typically, a URL, authentication information, and a
dictionary, `TOPIC_HANDLERS`, will be required. See "Subclassing `AlertStream`" below. The `AlertStream` subclass will
convert the key-value pairs of the `OPTIONS` dictionary into properties (and values) of the `AlertStream` subclass
instance.
  * The hopskotch alert stream supports a wildcard of `*` for an alert handler topic name. If specified, ALL public topics will be subscribed and use that handler function. A directly specified topic handler will always be used before the `*` handler for any topic that is covered twice. 

### Getting Kafka Stream Credentials
As part of your `OPTIONS` for each Kafka stream, you need to configure access credentials. Visit these links
to get credentials for [Hopskotch](https://hop.scimma.org/) and [GCN Classic over Kafka](https://gcn.nasa.gov/quickstart).
Set the environment variables with the username and passwords obtained. Do not check them in to your code repository.


## Alert Handling

Assuming that an `AlertStream` subclass exists for the Kafka stream of interest,
the keys of the `TOPIC_HANDLERS` dictionary are the topics that will be subscribed to. The values
of the `TOPIC_HANDLERS` dictionary specify alert handling methods that will be imported and called
for each alert recieved on that topic. An example is provided,
`tom_alerts.alertstreams.alertstream.alert_logger`, which simply logs the alert.

To customize this behaviour according to the needs of your TOM, define an alert handling function for each
topic that you wish to subscribe to. Your `TOPIC_HANDLERS` dictionary will have a an entry for each topic
whose key is the topic name and whose value is a string indicating the dot-path to the alert handling function.
When the `AlertStream` subclass is instanciated, the `OPTIONS` dictionary is read and an `alert_handler`
dictionary is created. It is keyed by topic name and it's values are the imported callable functions specified by the
dot-path strings. `readstreams` will call the alert handler for each alert that comes in on the topic. The signiture
of the alert handling function is specific to the `AlertStream` subclasss.

## Subclassing `AlertStream`

Ideally, As a TOM developer, there is already an `AlertStream`-subclass for the alert stream that you
want your TOM to listen to. If so, you need only to configure your TOM to use it in  `settings.py`
`ALERT_STREAMS`. If you must implement your own `AlertStream` subclass, please get in touch. In the meantime, here's a brief outline:

1. Create subclass of `AlertStream`.

2. Create `required_keys` and `allowed_keys` class variables in your `AlertStream`-subclass.

   These are lists of strings refering to the keys of the `OPTIONS` dictionary. The purpose of these is to
   help TOM developers using your `AlertStream`-subclass with the key-value pairs in their `ALERT_STREAMS`
  `OPTIONS` configuration dictionary.

3. Implement the `listen()` method.

   This method will be called by the `readstreams` management command and is not expected to return. It
   should instanciate your consumer, subscribe to the topics configured in `ALERT_STREAMS`, and start
   consuming. The detail of this will depend on the kafka-client used. See `alertstreams.gcn.listen()`
   and `alertstreams.hopskotch.listen()` for examples to follow.
   
   The loop which consumes messages in your `listen()` method should extract the topic from each message
   and call `self.alert_handler[topic]()` with the message or message-derived arguments specific to your
   kafka client. Users of your `AlertStream`-subclass will write these topic-specific alert handling methods
   and configure them in the `TOPIC_HANLDERS` dictionary of their `ALERT_STREAMS` configuration.
   The `AlertStream` base class will set up the `alert_handler` dictionary according to your users'
   configuration. It helps your users to provide an example `alert_hander()` function in your module as
   an example. (Again, see `alertstreams.gcn.listen()` and `alertstreams.hopskotch.listen()`, their
   configurations in `settings.py`, and the `alertstreams.gcn.alert_logger()` and
   `alertstreams.hopskotch.alert_logger() methods, for example).
