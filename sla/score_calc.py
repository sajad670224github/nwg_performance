import datetime
import json
import pandas as pd
from pandas import json_normalize

from ios_input.clickhouse import ClickhouseApi
from sla.models import Baseline, TirInformation, ULImprovementLevel
from sla.hi_calc import atoll_sector_region
from sla.kpi_calc import prepare_for_insert_clickhouse, delete_data_clickhouse, insert_dataframe_batch

def determine_tier_score(x, df_tier):
    df_tier.sort_values('score', ascending=True, inplace=True)
    for tier in list(df_tier["name"]):
        if tier != 'tier4' and x["index"] < x[tier]:
            return df_tier[df_tier["name"]==tier]["score"].values[0]
    return df_tier[df_tier["name"]=="tier4"]["score"].values[0]

def find_bl(dt, report, level, df_hi):
    level_n = level if len(level.split("_")) == 1 else level.split("_")[-1]
    quarter =  (dt.month - 1) // 3 + 1
    df_bl = pd.DataFrame(Baseline.objects.filter(year=dt.year, quarter=quarter, level=level_n,
                                                 technology=report.technology).values())
    if 'sector' in level:
        miss_sectors = set(df_hi['element']) - set(df_bl['element'])
        df_atoll = atoll_sector_region(report)
        df_atoll = df_atoll[(df_atoll['region'].isna() == False)&(df_atoll['region'] != 'unknown')&(
            df_atoll['sectornotech'].apply(lambda x: True if x in miss_sectors else False))]
        df_bl_region = pd.DataFrame(Baseline.objects.filter(year=dt.year, quarter=quarter, level=level_n.replace('sector', 'region'),
                                                 technology=report.technology).values())
        df_m = df_bl_region.merge(df_atoll, left_on='element', right_on='region', how='left')
        df_m.drop(columns='element', inplace=True)
        df_m.rename(columns={'sectornotech': 'element'}, inplace=True)
        df_bl = pd.concat([df_bl, df_m[df_bl.columns]])
    return df_bl

def user_load_index(dt, report, level, network):
    kpi_ui_li_id = list(ULImprovementLevel.objects.filter(technology=report.technology, level=level.split('_')[-1]).values_list('kpi', flat=True))
    kpi_ui_li_name = list(report.kpis.filter(kpi_id__in=kpi_ui_li_id).values_list('name', flat=True))
    kpi_query = ""
    for kpi in kpi_ui_li_name:
        kpi_query += f", JSONExtractFloat(data, '{kpi}') as index "
    ch = ClickhouseApi("")
    df_data = ch.client.query_dataframe(
        f"""select time, element {kpi_query} from mt_sla 
                    where technology='{report.technology}' and type='kpi' and 
                    layer='{report.layer}' and network in ({','.join(network)}) 
                    and time='{dt}'"""
    )
    ch.close()
    return df_data
def calculate_score(dt, report, level):
    # find health index kpi
    network = [f"'{i}'" for i in report.network.split(',')]
    ch = ClickhouseApi("")
    if "_" in level:
      df_data = user_load_index(dt, report, level, network)
    else:
        df_data = ch.client.query_dataframe(
            f"""select time, element, JSONExtractFloat(data, 'weighted_index') as index from mt_sla 
                where technology='{report.technology}' and type='hi' and 
                layer='{report.layer}' and network in ({','.join(network)}) 
                and time='{dt}'"""
        )
    ch.close()
    if "index" in df_data.columns:
            df_data = df_data[df_data["index"] > 0]
    # derive baseline
    df_bl = find_bl(dt, report, level, df_data)
    # calculate score
    fdf = df_data.merge(df_bl, left_on='element', right_on='element', how='left')
    print(fdf[fdf['tier3'].isna()==True].shape[0])
    fdf = fdf[fdf['tier3'].isna() == False]
    df_tier = pd.DataFrame(TirInformation.objects.all().values('name', 'score'))
    col_name = 'hi_score' if len(level.split('_')) == 1 else level.split('_')[1]+'_score'
    fdf[col_name] = fdf.apply(lambda x: determine_tier_score(x, df_tier), axis=1)
    return fdf[['time', 'element', col_name]]


def tech_score_calculate(dt, report):
    df_hi = calculate_score(dt, report, report.layer)
    df_li = calculate_score(dt, report, f"{report.layer}_li")
    cols_data = ['hi_score', 'li_score']
    if report.technology in ["LMBB", "LFBB", "NMBB"]:
        df_ui = calculate_score(dt, report, f"{report.layer}_ui")
        df = df_hi.set_index(["time", "element"]).join(df_li.set_index(["time", "element"])).join(
            df_ui.set_index(["time", "element"])).reset_index()
        cols_data.append('ui_score')
    else:
        df = df_hi.set_index(["time", "element"]).join(df_li.set_index(["time", "element"])).reset_index()

    df = df.groupby(['time', 'element'])[cols_data].apply(
        lambda x: json.dumps(x.to_dict('records')[0])).reset_index(
        name='data')
    df = prepare_for_insert_clickhouse(df, report)
    df['type'] = 'score'
    delete_data_clickhouse('mt_sla', "", report, [dt], type='score')
    insert_dataframe_batch(df, 'mt_sla')
