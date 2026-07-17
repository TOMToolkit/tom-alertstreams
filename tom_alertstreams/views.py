from __future__ import annotations

from typing import Any

from django.db.models import Max, QuerySet
from django.http import HttpResponse, HttpRequest
from django.utils import timezone
from django_filters.views import FilterView

from tom_alertstreams.alertstreams.alertstream import get_alert_stream_classes
from tom_alertstreams.models import Alert
from tom_alertstreams.tables import (
    AlertFilterSet, AlertStreamPresenter, AlertTable, STREAM_PRESENTERS,
)
from tom_common.htmx_table import HTMXTableViewMixin


def _build_presenter_map() -> dict[str, AlertStreamPresenter]:
    """Build a presenter instance for each configured active alert stream.

    Looks up each stream's STREAM_NAME in the STREAM_PRESENTERS registry.
    Streams not in the registry get the default AlertStreamPresenter (no URLs).
    """
    return {
        klass.STREAM_NAME: STREAM_PRESENTERS.get(klass.STREAM_NAME, AlertStreamPresenter)()
        for klass in get_alert_stream_classes()
    }


def _build_stream_status() -> list[dict[str, Any]]:
    """Build per-stream "last received" status for the dashboard.

    Returns a list of dicts with keys: stream_name, is_mock, latest_received, now.
    One entry per active stream (deduped by STREAM_NAME — see below); streams with
    no alerts in the database appear with latest_received=None.

    `latest_received` is Max(created) — when we last INGESTED an alert for the
    stream — not Max(observation_time), the alert's observation time. The two can differ
    by hours: a stream may be actively receiving alerts whose observation times are
    old (e.g. a survey that isn't observing at this hour, or a consumer working
    through a backlog), so observation time would mislabel a live stream as stale.
    The dashboard reads "Time since last alert received", so created is the metric
    that matches it.

    One aggregate DB query (created is indexed).
    """
    # Latest ingest (created) time per stream, in one query
    latest_by_stream: dict[str, Any] = {
        row['stream_name']: row['latest']
        for row in Alert.objects.values('stream_name').annotate(latest=Max('created'))
    }

    # Single now value so timesince is consistent across all badges. Iterate the configured
    # classes (not just names) so we can flag streams running a mock/stub (IS_MOCK), and
    # dedupe by STREAM_NAME: a broker can be configured as several ALERT_STREAMS entries that
    # share a name (Pitt-Google runs one entry per Pub/Sub topic, all 'pittgoogle'), and
    # get_alert_stream_classes() returns one class per entry — so first-seen wins and each
    # stream gets a single badge. latest_received is already aggregated across the stream's
    # topics (Max(created) keyed by stream_name).
    now = timezone.now()
    status: list[dict[str, Any]] = []
    seen_stream_names: set[str] = set()
    for klass in get_alert_stream_classes():
        if klass.STREAM_NAME in seen_stream_names:
            continue
        seen_stream_names.add(klass.STREAM_NAME)
        status.append({
            'stream_name': klass.STREAM_NAME,
            'is_mock': klass.IS_MOCK,
            'latest_received': latest_by_stream.get(klass.STREAM_NAME),
            'now': now,
        })
    return status


class RecentAlertsView(HTMXTableViewMixin, FilterView):
    """Display the most recent alerts from all configured alert streams.

    No login is required — the Recent Alerts page is intentionally public so that
    demo visitors and potential TOM developers can browse it without an account.

    Alert and object links (e.g. to ANTARES loci, ALeRCE objects) are built on the
    fly by AlertStreamPresenter subclasses (registered in tables.STREAM_PRESENTERS),
    so no URLs need to be stored in the database.
    """
    template_name = 'tom_alertstreams/recent_alerts.html'
    model = Alert
    table_class = AlertTable
    filterset_class = AlertFilterSet
    paginate_by = 20

    def get_queryset(self) -> QuerySet[Alert]:
        """Defer raw_payload — the large JSONField that the table never displays.

        Loading and JSON-parsing raw_payload for every fetched row is the dominant
        per-request cost here; the table only renders the normalized columns, so we
        skip it. Ordering comes from Alert.Meta (newest received first). Nothing in
        this view or the presenters reads raw_payload.
        """
        return Alert.objects.defer('raw_payload')

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Add stream status data for the dashboard.

        Runs on every request (both full page and HTMX partial) so the OOB
        swap in the custom partial template can refresh the dashboard badges.
        """
        context = super().get_context_data(**kwargs)
        context['stream_status'] = _build_stream_status()
        return context

    def get_table_kwargs(self) -> dict[str, Any]:
        """Inject the presenter map into the AlertTable constructor.

        Each configured AlertStream is paired with an AlertStreamPresenter
        (looked up by STREAM_NAME in the STREAM_PRESENTERS registry). The
        presenter handles URL construction — the table just calls
        presenter.alert_url() / presenter.object_url() and renders the result.

        This method is implemented in django-tables2.SingleTableMixin,
        which HTMXTableViewMixin inherits from. It's called like this:

        get_context_data()          # SingleTableMixin (django-tables2)
          └── get_table(**self.get_table_kwargs())
                ├── get_table_kwargs()    # returns {} by default; we override
                └── get_table(presenter_map=…)
        """
        kwargs = super().get_table_kwargs()
        kwargs['presenter_map'] = _build_presenter_map()
        return kwargs


# ---------------------------------------------------------------------------
# HTMX cascading select endpoint for the topic filter
# ---------------------------------------------------------------------------

def topic_choices_view(request: HttpRequest) -> HttpResponse:
    """Return <option> elements for the topic filter, scoped to a stream.

    HTMX cascading select endpoint: called when the stream_name dropdown
    changes. Returns raw <option> HTML that replaces the topic <select>
    innerHTML. When no stream is selected, returns topics from all streams.
    """
    stream_name = request.GET.get('stream_name', '')
    qs = Alert.objects.all()
    if stream_name:
        qs = qs.filter(stream_name=stream_name)
    topics = qs.values_list('topic', flat=True).distinct().order_by('topic')

    # Build <option> HTML — "All topics" empty option first, then available topics
    options = ['<option value="" selected>All topics</option>']
    options.extend(f'<option value="{topic}">{topic}</option>' for topic in topics)
    return HttpResponse('\n'.join(options))
