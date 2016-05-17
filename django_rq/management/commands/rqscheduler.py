import time
from optparse import make_option

from django.core.management.base import BaseCommand
from django_rq import get_scheduler


class Command(BaseCommand):
    """
    Runs RQ scheduler
    """
    help = __doc__
    args = '<queue>'

    option_list = BaseCommand.option_list + (
        make_option(
            '--interval',
            '-i',
            type=int,
            dest='interval',
            default=60,
            help="How often the scheduler checks for new jobs to add to the "
                 "queue (in seconds).",
        ),
        make_option(
            '--queue',
            type=str,
            dest='queue',
            default='default',
            help="Name of the queue used for scheduling.",
        ),
        make_option(
            '--retry',
            action='store_true',
            help='Retry connection if scheduler already exists.',
        ),
    )

    def handle(self, *args, **options):
        interval = options.get('interval')
        scheduler = get_scheduler(
            name=options.get('queue'), interval=interval)
        while 1:
            try:
                scheduler.run()
                break
            except ValueError as err:
                if options.get('retry', False) and 'already an active RQ scheduler' in str(err):
                    msg = 'RQ scheduler already exists, retrying in {} seconds'.format(interval)
                    scheduler.log.warning(msg)
                    time.sleep(interval)
                else:
                    raise
