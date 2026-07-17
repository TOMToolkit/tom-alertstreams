from django.contrib import admin

from tom_alertstreams.models import Alert


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = ('stream_name', 'alert_id', 'published_time', 'observation_time', 'object_id', 'magnitude', 'flux')
    list_filter = ('stream_name',)
    search_fields = ('alert_id', 'object_id')
