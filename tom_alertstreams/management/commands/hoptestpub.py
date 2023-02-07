import datetime
import logging

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand

from tom_alertstreams.alertstreams.alertstream import get_default_alert_streams
from tom_alertstreams.alertstreams.hopskotch import HopskotchAlertStream

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = 'Publish a timestamped test message to Hopskotch tomtoolkit.test topic.'

    def handle(self, *args, **options):
        logger.debug(f'hoptestpub.Command.handle() args: {args}')
        logger.debug(f'hoptestpub.Command.handle() options: {options}')

        try:
            alert_streams = get_default_alert_streams()
            # extract the HopskotchAlertStream
            hopskotch_alert_stream = next(stream for stream in alert_streams if isinstance(stream, HopskotchAlertStream))

        except ImproperlyConfigured as ex:
            logger.error(f'{ex.__class__.__name__}: Configure alert streams in settings.py ALERT_STREAMS: {ex}')
            exit(1)

        stream = hopskotch_alert_stream.get_stream()
        topic = 'tomtoolkit.test'  # SCiMMA Admin topic permissions for Credential are assumed to have been set up

        with stream.open(hopskotch_alert_stream.url+topic, "w") as s:
            s.write({
                'created': datetime.datetime.utcnow().isoformat(),
                'created-by': 'tom-alertstreams hoptestpub.py'
             })

        logger.info('hoptestpub Command.handle() returning...')
