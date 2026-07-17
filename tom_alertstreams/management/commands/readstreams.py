import logging
from threading import Thread

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand

from tom_alertstreams.alertstreams.alertstream import get_default_alert_streams


logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = 'Consume alerts from the alert streams configured in the settings.py ALERT_STREAMS'

    def handle(self, *args, **options):
        logger.debug(f'readstreams.Command.handle() args: {args}')
        logger.debug(f'readstreams.Command.handle() options: {options}')

        try:
            alert_streams = get_default_alert_streams()
        except ImproperlyConfigured as ex:
            logger.error(f'{ex.__class__.__name__}: Configure alert streams in settings.py ALERT_STREAMS: {ex}')
            exit(1)

        # Run each alert_stream in its own Thread (sort of at the same time).
        # Target run() (not listen()) so each stream is supervised: run() catches
        # and logs any error from listen() and restarts it, so a single stream's
        # broker outage can't silently kill that thread while the others continue.
        # daemon=True so a single Ctrl-C (see the join loop below) tears everything down.
        threads = []
        for alert_stream in alert_streams:
            t = Thread(target=alert_stream.run, name=alert_stream._get_stream_classname(), daemon=True)
            t.start()
            threads.append(t)
            logger.info((f'read_streams {alert_stream._get_stream_classname()} TID={t.native_id} ; '
                         f'thread identifier={t.ident}'))

        # Important: Block the main thread on the stream threads instead of returning.
        # (i.e. don't let `AlertStream.handle()` return).

        # Why: If AlertStreams.handle() returns, the interpreter enters its
        # shutdown/finalize state. In that state, Google Cloud Pub/Sub's gRPC streaming
        # pull silently stops delivering messages. So, Pitt-Google, for example,
        # streams ingest nothing. Kafka-based streams are not affected.
        try:
            while any(thread.is_alive() for thread in threads):  # the "join loop" mentioned above
                for thread in threads:
                    thread.join(timeout=1.0)  # 1s timeout keeps Ctrl-C working
        except KeyboardInterrupt:
            logger.info('readstreams: KeyboardInterrupt received; shutting down.')
