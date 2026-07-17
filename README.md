# tom-alertstreams

`tom-alertstreams` is a reusable [TOM Toolkit](https://tom-toolkit.readthedocs.io/) app
that listens to Kafka-based astronomical alert streams, persists incoming alerts to a
database, and displays them on a "Recent Alerts" page.

## Features

- **`readstreams` management command** — connects to configured Kafka streams, dispatches
  each incoming alert to a topic-specific handler, and runs indefinitely alongside your
  TOM's web server.
- **`Alert` model** — a FIFO-queued Django model that stores normalized alert fields
  (stream name, topic, timestamp, RA/Dec, magnitude, raw payload). Older rows are
  automatically pruned when the per-stream limit is exceeded.
- **Recent Alerts page** — an HTMX-driven filterable table at `/alertstreams/recent/`
  that auto-registers via TOM Toolkit's AppConfig integration points (no manual URL
  include or navbar edit required).
- **`save_alert_to_database` handler** — a ready-made `TOPIC_HANDLERS` value that
  normalizes and persists any incoming alert to the database.

---

## Installation

1. Install the package into your TOM environment, specifying which alert streams you want:

    ```bash
    # Install support for specific streams (recommended)
    pip install tom-alertstreams[gcn]           # GCN Classic over Kafka
    pip install tom-alertstreams[hopskotch]     # SCiMMA Hopskotch
    pip install tom-alertstreams[antares]       # ANTARES
    pip install tom-alertstreams[fink]          # Fink

    # Multiple streams at once
    pip install tom-alertstreams[gcn,hopskotch,antares]

    # All supported streams
    pip install tom-alertstreams[all-streams]
    ```

2. Add `tom_alertstreams` to `INSTALLED_APPS` in your `settings.py`:

    ```python
    INSTALLED_APPS = [
        ...
        'tom_alertstreams',
    ]
    ```

3. Run migrations to create the `Alert` table:

    ```bash
    python manage.py migrate
    ```

That's it. If your project uses the TOM Toolkit base URLs (`include('tom_common.urls')`),
the Recent Alerts page is now available at `/alertstreams/recent/` and the "Recent Alerts"
navbar link appears automatically — no additional URL or template changes required.

Verify the installation by checking that `readstreams` appears in `./manage.py` output:

```bash
[tom_alertstreams]
    readstreams
```

---

## Configuration

Add an `ALERT_STREAMS` list to your `settings.py`. Each entry is a dict with three keys:

| Key | Type | Description |
|-----|------|-------------|
| `ACTIVE` | `bool` | Set `False` to disable a stream without removing its config. |
| `NAME` | `str` | Dotted path to an `AlertStream` subclass. |
| `OPTIONS` | `dict` | Stream-specific connection and topic-handler configuration. |

The `OPTIONS` dict is validated by a Pydantic model on startup — missing required fields
raise descriptive errors before `readstreams` attempts any network connection.

### Optional settings

```python
# Enable the Recent Alerts web page and navbar link (default: False).
# When False (or absent), the /alertstreams/recent/ URL is not registered and
# the navbar link does not appear. The Alert model, migrations, and
# save_alert_to_database handler remain available regardless of this setting —
# only the web display layer is gated. Set to True if your TOM should display
# a live table of incoming alerts.
SHOW_RECENT_ALERTS = True

# Maximum number of alerts to retain per stream (default: 100).
# Only relevant when SHOW_RECENT_ALERTS = True and save_alert_to_database is used.
ALERTSTREAMS_RECENT_COUNT = 100
```

---

## Stream configuration reference

### SCiMMA Hopskotch

```python
{
    'ACTIVE': True,
    'NAME': 'tom_alertstreams.alertstreams.hopskotch.HopskotchAlertStream',
    'OPTIONS': {
        'URL': 'kafka://kafka.scimma.org/',
        # GROUP_ID must be prefixed with your SCiMMA username.
        'GROUP_ID': os.environ.get('SCIMMA_AUTH_USERNAME', '') + '-my-tom',
        'USERNAME': os.environ.get('SCIMMA_AUTH_USERNAME', ''),
        'PASSWORD': os.environ.get('SCIMMA_AUTH_PASSWORD', ''),
        'START_POSITION': 'LATEST',   # optional: 'LATEST' (default) or 'EARLIEST'
        'TOPIC_HANDLERS': {
            'sys.heartbeat': 'tom_alertstreams.alertstreams.hopskotch.heartbeat_handler',
            # Wildcard patterns: 'hermes.*' matches all hermes.* topics
            'hermes.*': 'tom_alertstreams.alertstreams.handlers.save_alert_to_database',
            # '*' matches ALL public topics not covered by a more specific entry
            '*': 'tom_alertstreams.alertstreams.hopskotch.alert_logger',
        },
    },
},
```

Credentials: [hop.scimma.org](https://hop.scimma.org/)

### GCN Classic over Kafka

```python
{
    'ACTIVE': True,
    'NAME': 'tom_alertstreams.alertstreams.gcn.GCNClassicAlertStream',
    'OPTIONS': {
        'GCN_CLASSIC_CLIENT_ID': os.environ.get('GCN_CLASSIC_CLIENT_ID', ''),
        'GCN_CLASSIC_CLIENT_SECRET': os.environ.get('GCN_CLASSIC_CLIENT_SECRET', ''),
        'DOMAIN': 'gcn.nasa.gov',    # optional, default shown
        'KAFKA_CONFIG': {},          # optional dict passed to the Confluent Kafka Consumer
        'TOPIC_HANDLERS': {
            'gcn.classic.text.LVC_INITIAL': 'tom_alertstreams.alertstreams.handlers.save_alert_to_database',
            'gcn.classic.text.LVC_PRELIMINARY': 'tom_alertstreams.alertstreams.gcn.alert_logger',
        },
    },
},
```

Credentials: [gcn.nasa.gov/quickstart](https://gcn.nasa.gov/quickstart)

### ANTARES

```python
{
    'ACTIVE': True,
    'NAME': 'tom_alertstreams.alertstreams.antares.AntaresAlertStream',
    'OPTIONS': {
        'API_KEY': os.environ.get('ANTARES_API_KEY', ''),
        'API_SECRET': os.environ.get('ANTARES_API_SECRET', ''),
        'GROUP': 'my-tom-consumer-group',    # optional, default: 'tom-alertstreams'
        'SSL_CA_LOCATION': None,             # optional path to CA cert, default: None
        'ENABLE_AUTO_COMMIT': True,          # optional, default: True
        'TOPIC_HANDLERS': {
            'nasa-ztf-test': 'tom_alertstreams.alertstreams.handlers.save_alert_to_database',
        },
    },
},
```

Credentials: [antares.noirlab.edu](https://antares.noirlab.edu)

### Fink

Install: `pip install tom-alertstreams[fink]`

Fink runs a separate Kafka broker and web portal per survey, and its topics follow
the `<filter>_<survey>` convention (e.g. `fink_sn_candidates_ztf`,
`fink_sn_candidates_lsst`). `FINK_SERVER`, `FINK_SURVEY`, and the topic suffixes must
all agree — a mismatch (e.g. `FINK_SURVEY='lsst'` with `_ztf` topics) is rejected at
startup, because it would route alerts to the wrong parser and silently drop them. Read
one survey per stream; configure two `ALERT_STREAMS` entries to read both.

```python
{
    'ACTIVE': True,
    'NAME': 'tom_alertstreams.alertstreams.fink.FinkAlertStream',
    'OPTIONS': {
        'FINK_USERNAME': os.environ.get('FINK_USERNAME', ''),
        'FINK_GROUP_ID': os.environ.get('FINK_GROUPID', ''),
        'FINK_SERVER': 'kafka-ztf.fink-broker.org:24499',  # LSST: kafka-lsst.fink-broker.org:24499
        'FINK_SURVEY': 'ztf',                              # 'ztf' or 'lsst'
        'TOPIC_HANDLERS': {
            'fink_sn_candidates_ztf': 'tom_alertstreams.alertstreams.alertstream.save_alert_to_database',
        },
    },
},
```

Credentials: register at [fink-broker.org](https://fink-broker.org).

### ALeRCE, AMPEL, Babamul, Lasair, Pitt-Google

These LSST broker stubs generate obviously-fake mock alerts (ra=0, dec=0,
magnitude=99, object IDs prefixed with `MOCK-`) for demonstration and development.
Replace with real implementations when the broker clients become available.

```python
{
    'ACTIVE': True,
    'NAME': 'tom_alertstreams.alertstreams.alerce.AlerceAlertStream',
    'OPTIONS': {
        'TOPIC_HANDLERS': {
            'alerce-topic': 'tom_alertstreams.alertstreams.handlers.save_alert_to_database',
        },
    },
},
# Similarly for ampel, babamul, lasair, pittgoogle — same OPTIONS structure.
```

---

## Running the alert listener

Start `readstreams` alongside your Django development server:

```bash
python manage.py readstreams
```

Each configured active stream runs in its own thread. `readstreams` is not expected to
return. In production, run it as a separate process managed by systemd, supervisor, or
your preferred process manager.

---

## Alert persistence

`save_alert_to_database` is a ready-made handler that normalizes an alert and saves it
to the `Alert` model. Use it as a `TOPIC_HANDLERS` value:

```python
'TOPIC_HANDLERS': {
    'my.topic': 'tom_alertstreams.alertstreams.handlers.save_alert_to_database',
}
```

Rows older than `ALERTSTREAMS_RECENT_COUNT` (default 100) per stream are automatically
pruned after each save. The Recent Alerts page at `/alertstreams/recent/` displays
persisted alerts with HTMX-powered live filtering by stream name or full-text search.

---

## Local Development

```bash
git clone https://github.com/TOMToolkit/tom_alertstreams.git
cd tom_alertstreams

# Create and activate a virtual environment (Python 3.9+)
python -m venv .venv
source .venv/bin/activate

# Install with stream extras for development
pip install -e ".[all-streams]"

# Run tests (requires a full TOM Toolkit environment)
python manage.py test tom_alertstreams --exclude-tag=canary
```

For extending `tom_alertstreams` — writing custom handlers, subclassing `AlertStream`,
or overriding the Alert model — see the extension guide in the TOM Toolkit documentation.
