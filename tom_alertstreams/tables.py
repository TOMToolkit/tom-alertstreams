from __future__ import annotations

import logging
import urllib.parse
from typing import Any, ClassVar

import django_filters
import django_tables2 as tables
from django import forms
from django.utils.html import format_html

from tom_alertstreams.alertstreams.alertstream import get_alert_stream_classes
from tom_alertstreams.models import Alert
from tom_common.htmx_table import HTMXTable, HTMXTableFilterSet

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AlertTable
# ---------------------------------------------------------------------------

class AlertTable(HTMXTable):
    """HTMX-driven table of recent alerts from all configured alert streams.

    Receives a presenter_map at construction time — a dict mapping stream_name
    to an AlertStreamPresenter instance. render_alert_id() and render_object_id()
    delegate URL construction to the presenter, keeping this table fully generic
    with zero stream-specific logic.
    """
    # Custom partial that includes an OOB swap to update the stream status dashboard
    partial_template_name = 'tom_alertstreams/partials/alert_table_partial.html'

    def __init__(
        self,
        *args: Any,
        presenter_map: dict[str, AlertStreamPresenter] | None = None,
        **kwargs: Any,
    ) -> None:
        self.presenter_map = presenter_map or {}
        super().__init__(*args, **kwargs)

    #
    # Custom field renderers
    #

    stream_name = tables.Column(verbose_name='Stream')  # sets the column header value

    # render_FIELDNAME() methods are called automatically when present
    def render_timestamp(self, value: Any) -> str:
        """Render timestamp in unambiguous UTC 24-hour format.

        The result looks like this: 2026-03-05 18:51:30 UTC
        """
        return value.strftime('%Y-%m-%d %H:%M:%S UTC')

    def render_alert_id(self, record: Alert, value: str) -> str:
        """Render alert_id as a hyperlink if the stream's presenter provides a URL."""
        presenter = self.presenter_map.get(record.stream_name)
        if presenter:
            url = presenter.alert_url(record)
            if url:
                return format_html('<a href="{}" target="_blank">{}</a>', url, value)
        return value

    def render_object_id(self, record: Alert, value: str) -> str:
        """Render object_id as a hyperlink if the stream's presenter provides a URL."""
        if not value:
            return value
        presenter = self.presenter_map.get(record.stream_name)
        if presenter:
            url = presenter.object_url(record)
            if url:
                return format_html('<a href="{}" target="_blank">{}</a>', url, value)
        return value

    def render_ra(self, value: Any) -> str:
        """Render RA to 5 decimal places (~0.04 arcsec, matching LSST precision)."""
        return f'{value:.5f}' if value is not None else ''

    def render_dec(self, value: Any) -> str:
        """Render Dec to 5 decimal places (~0.04 arcsec, matching LSST precision)."""
        return f'{value:.5f}' if value is not None else ''

    def render_magnitude(self, value: Any) -> str:
        """Render magnitude to 3 decimal places (~1 mmag, matching survey photometric precision)."""
        return f'{value:.3f}' if value is not None else ''

    def render_flux(self, value: Any) -> str:
        """Render flux in nanojansky to 3 decimal places."""
        return f'{value:.3f}' if value is not None else ''

    class Meta(HTMXTable.Meta):
        model = Alert
        fields = [
            'selection', 'alert_id', 'stream_name', 'topic', 'timestamp',
            'object_id', 'ra', 'dec', 'magnitude', 'flux',
        ]


def _get_stream_name_choices() -> list[tuple[str, str]]:
    """Build dropdown choices from the active streams in settings.ALERT_STREAMS.

    Called at form-render time (not import time) so changes to settings take
    effect without restarting the process. Returns a list of (value, label)
    tuples using each stream's STREAM_NAME. Streams that fail to import are
    silently skipped so a misconfigured entry doesn't break the filter form.
    """
    return [(klass.STREAM_NAME, klass.STREAM_NAME) for klass in get_alert_stream_classes()]


class AlertFilterSet(HTMXTableFilterSet):
    """FilterSet for the Recent Alerts table.

    Provides a 'query' full-text search (inherited from HTMXTableFilterSet) plus
    the fields defined here, which appear in the Advanced> expansion of the form.
    """
    # these are the fields that appear in the Advanced> expansion
    stream_name = django_filters.ChoiceFilter(
        field_name='stream_name',
        label='Stream',
        empty_label='All streams',
        choices=_get_stream_name_choices,
        widget=forms.Select(attrs={
            'hx-get': '',           # empty string: GET goes to the current page URL
            'hx-trigger': 'change',
            'hx-target': 'div.table-container',
            'hx-swap': 'innerHTML',
            'hx-indicator': '.progress',
            'hx-include': 'closest form',
        }),
    )

    class Meta:
        model = Alert
        fields = ['stream_name']


# ---------------------------------------------------------------------------
# AlertStreamPresenter — display adapter for URL construction
# ---------------------------------------------------------------------------

class AlertStreamPresenter:
    """Presentation adapter: constructs display URLs from an Alert record.

    Each presenter knows how to build URLs for a specific alert stream's web
    portal. The base implementation returns None for all URLs — streams with
    no web portal (AMPEL, Hopskotch, Pitt-Google) use this default.

    For streams with a web portal, create a subclass that:
    1. Sets BASE_URL to the portal's root URL
    2. Overrides alert_url() and/or object_url() to construct the full URL
       by combining BASE_URL with the stream-specific path structure using
       an f-string (e.g., f'{self.BASE_URL}/object/{alert.object_id}')

    Register custom presenters in the STREAM_PRESENTERS dict at the bottom
    of this section. Streams not in the registry use this base class.

    Follows the same structural pattern as Django's ModelAdmin: domain objects
    (AlertStream) are unaware of their presenter. Registration is in the
    presentation layer (this module).
    """
    BASE_URL: ClassVar[str | None] = None

    def alert_url(self, alert: Alert) -> str | None:
        """Return the URL for an alert detail page, or None."""
        return None

    def object_url(self, alert: Alert) -> str | None:
        """Return the URL for an object/source page, or None."""
        return None


class AlercePresenter(AlertStreamPresenter):
    """ALeRCE object pages: https://alerce.online/object/{object_id}"""
    BASE_URL = 'https://alerce.online'

    def object_url(self, alert: Alert) -> str | None:
        if not alert.object_id:
            return None
        return f'{self.BASE_URL}/object/{alert.object_id}'


class AntaresPresenter(AlertStreamPresenter):
    """ANTARES locus pages: https://antares.noirlab.edu/loci/{alert_id}"""
    BASE_URL = 'https://antares.noirlab.edu'

    def alert_url(self, alert: Alert) -> str | None:
        return f'{self.BASE_URL}/loci/{alert.alert_id}'


class BabamulPresenter(AlertStreamPresenter):
    """Babamul object pages: https://babamul.caltech.edu/objects/{survey}/{object_id}

    Survey is inferred from the object ID prefix. ZTF IDs start with 'ZTF',
    LSST IDs start with 'LSST'. Unrecognized prefixes get no link.
    """
    BASE_URL = 'https://babamul.caltech.edu'

    def object_url(self, alert: Alert) -> str | None:
        if not alert.object_id:
            return None
        if alert.object_id.startswith('ZTF'):
            survey = 'ZTF'
        elif alert.object_id.startswith('LSST'):
            survey = 'LSST'
        else:
            logger.warning('BabamulPresenter: unrecognized object_id prefix: %s', alert.object_id)
            return None
        return f'{self.BASE_URL}/objects/{survey}/{alert.object_id}'


class FinkPresenter(AlertStreamPresenter):
    """Fink object pages: https://fink-portal.org/{object_id}"""
    BASE_URL = 'https://fink-portal.org'

    def object_url(self, alert: Alert) -> str | None:
        if not alert.object_id:
            return None
        return f'{self.BASE_URL}/{alert.object_id}'


class GCNPresenter(AlertStreamPresenter):
    """GCN circular pages: https://gcn.nasa.gov/circulars/{alert_id}"""
    BASE_URL = 'https://gcn.nasa.gov'

    def alert_url(self, alert: Alert) -> str | None:
        return f'{self.BASE_URL}/circulars/{alert.alert_id}'


class LasairPresenter(AlertStreamPresenter):
    """Lasair object pages: https://lasair-ztf.lsst.ac.uk/objects/{object_id}/"""
    BASE_URL = 'https://lasair-ztf.lsst.ac.uk'

    def object_url(self, alert: Alert) -> str | None:
        if not alert.object_id:
            return None
        return f'{self.BASE_URL}/objects/{alert.object_id}/'


class LsstPresenter(AlertStreamPresenter):
    """Rubin Science Platform deep links via ADQL query API.

    Constructs a URL that opens the RSP Portal with a pre-filled ADQL query
    for the diaObject. Requires RSP login (CILogon) — unauthenticated users
    are redirected to the login page, then to the query result.

    The schema prefix (e.g. 'dp1') changes per Rubin data release.
    """
    BASE_URL: ClassVar[str] = 'https://data.lsst.cloud'
    TAP_URL: ClassVar[str] = 'https://data.lsst.cloud/api/tap'
    SCHEMA_PREFIX: ClassVar[str] = 'dp1'

    def object_url(self, alert: Alert) -> str | None:
        if not alert.object_id:
            return None
        adql = f'SELECT * FROM {self.SCHEMA_PREFIX}.DiaObject WHERE diaObjectId={alert.object_id}'
        encoded_adql = urllib.parse.quote(adql)
        return f'{self.BASE_URL}/portal/app/?api=tap&service={self.TAP_URL}&adql={encoded_adql}&execute=true'


# Streams not listed here use the default AlertStreamPresenter (no URLs).
STREAM_PRESENTERS: dict[str, type[AlertStreamPresenter]] = {
    'alerce': AlercePresenter,
    'ampel-lsst': LsstPresenter,
    'antares': AntaresPresenter,
    'antares-ztf': AntaresPresenter,
    'antares-lsst': AntaresPresenter,
    'babamul': BabamulPresenter,
    'fink': FinkPresenter,
    'gcn': GCNPresenter,
    'lasair': LasairPresenter,
}
