from __future__ import annotations

from django.urls import include, path

# Minimal root URLconf for the test harness.
#
# Mirrors the include that TomAlertstreamsConfig.include_url_paths() registers in
# a real TOM, so reverse('alertstreams:recent-alerts') resolves under the
# 'alertstreams' instance namespace during view tests. Only the alertstreams URLs
# are mounted — the view tests render against a stub tom_common/base.html (see
# tests/templates/), so none of the full TOM's navbar/auth URLs are needed here.
urlpatterns = [
    path('alertstreams/', include('tom_alertstreams.urls', namespace='alertstreams')),
]
