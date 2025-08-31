from django.test import TestCase

# Create your tests here.
from sla.score_calc import *
from sla.kpi_calc import *
from sla.hi_calc import *
from sla.score_calc import *
from sla.tasks import *
layer=['region']; techs=['UMTS']
dt = datetime.datetime(2025,4,15,0,0)

filters_ = {}
if 'all' not in [i.lower() for i in layer]:
    filters_['layer__in'] = layer
if 'all' not in [i.lower() for i in techs]:
    filters_['technology__in'] = techs
if filters_:
    pm_reports = SlaPmModels.objects.filter(**filters_)
else:
    pm_reports = SlaPmModels.objects.all()

report = pm_reports[0]


from sla.tasks import *
layer=['all']; techs=['all']
start_time = datetime.datetime(2025,4,15,0,0)
iterations = 1
sla_kpi_task(start_time, iterations, layer=['all'], techs=["All"], force_calculation=True)