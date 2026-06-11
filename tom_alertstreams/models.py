from __future__ import annotations

from typing import Any, ClassVar

from django.conf import settings
from django.db import models


class FIFOQueueMixin(models.Model):
    """Mixin that enforces a per-partition maximum row count.

    Turns a model's table into a First-In-First-Out (FIFO) queue by extending
    save() to delete the oldest rows beyond FIFO_MAX after every insert.

    Subclasses set FIFO_MAX and FIFO_PARTITION_FIELDS as class variables.

    Partitioning: FIFO_PARTITION_FIELDS names one or more model fields whose
    values together define a partition. Each unique combination of partition
    field values gets its own independent FIFO budget of FIFO_MAX rows.

    With a single partition field (e.g. ``('stream_name',)``), the table holds
    at most ``FIFO_MAX × number_of_distinct_stream_names`` rows. With compound
    fields (e.g. ``('stream_name', 'topic')``), each unique combination gets
    its own budget, so the table can hold up to
    ``FIFO_MAX × number_of_distinct_(stream_name, topic)_pairs`` rows.
    For example: 8 streams × 19 topics/stream × 1000 max = 152,000 rows
    (though in practice most streams have far fewer topics).

    If FIFO_PARTITION_FIELDS is None, the limit applies to the entire table.
    """
    FIFO_MAX: ClassVar[int] = 10
    FIFO_PARTITION_FIELDS: ClassVar[tuple[str, ...] | None] = None

    class Meta:
        abstract = True

    def save(self, *args: Any, **kwargs: Any) -> None:
        super().save(*args, **kwargs)  # save, then trim: Ensures the newly-saved
        # this is the extention to the method:
        self._enforce_fifo_limit()     # row is counted against the FIFO_MAX limit.

    def _enforce_fifo_limit(self) -> None:
        """Delete rows beyond FIFO_MAX, oldest-received first, within this partition.

        The partition is defined by FIFO_PARTITION_FIELDS — all rows sharing the
        same values across those fields form one partition. Each partition is
        independently capped at FIFO_MAX rows.

        Eviction is by `created` (insertion/received time) — true FIFO order — NOT by
        the alert's observation `timestamp`. Ordering a FIFO by observation time would
        instantly evict a freshly-received alert whose observation time is old (e.g. a
        broker streaming last night's alerts now, after the survey went quiet), so it
        would never appear in the table. By `created`, the most-recently-received rows
        are always kept. (The concrete model must provide a `created` insertion-time
        field; Alert does, via auto_now_add.)

        Uses list() to materialise PKs before the DELETE to avoid a SQLite restriction
        that forbids DELETE from a table referenced in the same statement's subquery.
        """
        qs = self.__class__.objects.all()  # query set
        if self.FIFO_PARTITION_FIELDS is not None:
            # Scope to rows sharing this instance's values for all partition fields
            partition_filter = {
                field: getattr(self, field) for field in self.FIFO_PARTITION_FIELDS
            }
            qs = qs.filter(**partition_filter)
        # Keep the FIFO_MAX most-recently-received rows; materialise PKs first to avoid
        # a SQLite subquery-in-DELETE restriction.
        excess_pks = list(
            # the list of the pks beyond FIFO_MAX
            qs.order_by('-created').values_list('pk', flat=True)[self.FIFO_MAX:]
        )
        if excess_pks:
            self.__class__.objects.filter(pk__in=excess_pks).delete()  # delete overflow


class Alert(FIFOQueueMixin):
    """A normalized alert received from an alert stream, stored for recent display.

    This class is designed specifically for a Recent Alerts demonstration page.
    It's the model shown in the table of recent alerts and size is limited by
    the FIFOQueueMixin, FIFO_MAX, and FIFO_PARTITION_FIELDS.
    """
    # Set the mixin class variables — partition by (stream_name, topic) so each
    # stream+topic combination gets its own independent FIFO budget.
    FIFO_MAX: ClassVar[int] = getattr(settings, 'ALERTSTREAMS_RECENT_COUNT', 10)
    FIFO_PARTITION_FIELDS: ClassVar[tuple[str, ...] | None] = ('stream_name', 'topic')

    stream_name = models.CharField(max_length=100, db_index=True)
    topic = models.CharField(max_length=200)
    timestamp = models.DateTimeField(db_index=True)
    alert_id = models.CharField(max_length=200)
    object_id = models.CharField(max_length=200, blank=True, null=True)
    ra = models.FloatField(null=True)
    dec = models.FloatField(null=True)
    magnitude = models.FloatField(null=True)
    flux = models.FloatField(null=True)
    raw_payload = models.JSONField(default=dict)

    class Meta(FIFOQueueMixin.Meta):  # this is the way you subclass the internal Meta class
        abstract = False  # override for the concrete model (abstract is True in the super)
        ordering = ['-timestamp']
        indexes = [models.Index(fields=['stream_name', 'timestamp'])]

    def __str__(self) -> str:
        return f'Alert {self.alert_id} from {self.stream_name} at {self.timestamp}'
