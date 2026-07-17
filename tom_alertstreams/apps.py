from django.apps import AppConfig
from django.conf import settings
from django.urls import include, path


class TomAlertstreamsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'tom_alertstreams'

    def include_url_paths(self) -> list:
        """Register tom_alertstreams URLs with the TOM Common URL configuration.

        Returns URL patterns that mount the Recent Alerts page at /alertstreams/,
        but only when `SHOW_RECENT_ALERTS = True` is set in settings. If the setting
        is absent or False, an empty list is returned and the page is not accessible.
        """
        # only return urlpatterns if they opt-in
        if not getattr(settings, 'SHOW_RECENT_ALERTS', False):
            return []
        return [
            path('alertstreams/', include('tom_alertstreams.urls', namespace='alertstreams')),
        ]

    def nav_items(self) -> list:
        """Add the 'Recent Alerts' link to the TOM navbar.

        Returns a list of navbar item dicts auto-discovered by the navbar_app_addons
        template tag, but only when SHOW_RECENT_ALERTS = True is set in settings.
        """
        # only show the navbar item if they opt-in
        if not getattr(settings, 'SHOW_RECENT_ALERTS', False):
            return []
        return [{'partial': 'tom_alertstreams/partials/navbar_link.html'}]
