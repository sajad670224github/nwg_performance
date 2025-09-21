import time
import datetime
from django.core.management.base import BaseCommand

from sla.tasks import sla_kpi_task

class Command(BaseCommand):
    help = 'Calculate SLA for last four days'

    def handle(self, *args, **kwargs):
        print('**start command')
        # loop()
        while True:
            try:
                start_time = datetime.datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - datetime.timedelta(days=5)
                sla_kpi_task(start_time, 1, force_calculation=True)
            except Exception as e:
                print(str(e))
            # snoozing till next run
            tomorrow_run = datetime.datetime.now().replace(microsecond=0, second=0, minute=0, hour=7) + datetime.timedelta(days=1)
            current_time = datetime.datetime.now().replace(microsecond=0)
            time_to_sleep = (tomorrow_run - current_time).days * 24 * 3600 + (tomorrow_run - current_time).seconds
            print(f"Snoozing for {time_to_sleep} seconds...")
            time.sleep(time_to_sleep)

