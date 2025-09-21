import datetime
import time

from sla.kpi_calc import calculate_sla_kpi
from sla.hi_calc import daily_hi_calculation
from sla.score_calc import tech_score_calculate
from sla.final_score_calc import calculate_final_score
from sla.models import SlaPmModels, ReportHistory
from ios_input.clickhouse import ClickhouseApi


sla_functions = {
    'kpi': calculate_sla_kpi,
    'hi': daily_hi_calculation,
    'score': tech_score_calculate
}

def need_calculation(dt, report, type_, force_calculation):
    if force_calculation:
        return True
    technology = getattr(report, 'technology', 'unknown')
    layer = getattr(report, 'layer', 'region')
    if not ReportHistory.objects.filter(technology=technology,
                                        level=layer,
                                        day=dt,
                                        type=type_).exists():
        return True
    return False

def update_report_history(dt, report, type_):
    technology = getattr(report, 'technology', 'unknown')
    layer = getattr(report, 'layer', 'region')
    item = ReportHistory(day=dt, technology=technology, level=layer, type=type_)
    item.save()


def calculations(dt, report, type_, force_calculation):
    if need_calculation(dt, report, type_, force_calculation):
        sla_functions[type_](dt, report)
        update_report_history(dt, report, type_)
        ch = ClickhouseApi("")
        print('-'*100)
        print(ch.client.query_dataframe(f"select technology, count(*) from mt_sla where type='kpi' and time in ('{dt.isoformat().replace('T', ' ')}') and layer='region' group by technology"))
        print('-' * 100)
        print(ch.client.query_dataframe(
            f"select technology, count(*) from mt_sla where type='kpi' and time in ('{dt.isoformat().replace('T', ' ')}') and layer='sector' group by technology"))
        ch.close()
        # print("sleep for 30 seconds")
        # time.sleep(30)
    else:
        print(f"read {type_} data for {report.technology} on {dt} from database")

def sla_kpi_task(start_time, iterations, layer=['all'], techs=["GSM", "UMTS", "LMBB", "LFBB", "NMBB"], force_calculation=False):
    for days in range(iterations):
        dt = start_time + datetime.timedelta(days=days)
        filters_ = {}
        if 'all' not in [i.lower() for i in layer]:
            filters_['layer__in'] = layer
        if 'all' not in [i.lower() for i in techs]:
            filters_['technology__in'] = techs
        if filters_:
            pm_reports = SlaPmModels.objects.filter(**filters_)
        else:
            pm_reports = SlaPmModels.objects.all()
        for report in pm_reports:
            for type_ in ['kpi', 'hi', 'score']:
                print(f"{'*'*100}\ncalculating {report.layer}: {report.technology}: {type_}")
                calculations(dt, report, type_, force_calculation)

        if need_calculation(dt, "", 'fscore', False):
            calculate_final_score(dt, ['Irancell'])
            update_report_history(dt, "", 'fscore')
        else:
            print(f"The final score is calculated on {dt}")