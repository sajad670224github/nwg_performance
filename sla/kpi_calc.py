import datetime
import json
import pandas as pd

from ios_input.ios_browser import get_kpi_pm_async
from sla.models import SlaPmModels, ULImprovementLevel
from ios_input.models import AtollData
from ios_input.clickhouse import ClickhouseApi
def get_sla_kpi(dt_, sla_pm, manual_element_list=None, manual_layer=None, special_kpi=False):
    if special_kpi:
        indicators = list(sla_pm.kpis.filter(special_kpi=True).values_list('kpi_id', flat=True))
    else:
        indicators = list(sla_pm.kpis.exclude(special_kpi=True).values_list('kpi_id', flat=True))
    if sla_pm.layer == 'region' and not special_kpi:
        bh_indicator = list(set(sla_pm.kpis.values_list('bh_kpi_id', flat=True)))
        bh_indicator.remove('nan') if 'nan' in bh_indicator else bh_indicator
        indicators = indicators + bh_indicator
    filters = [{"type": "network", "element_list": [i.strip() for i in sla_pm.network.split(',')], "custom": False,
                "custom_name": None}]
    if manual_element_list:
        filters.append({"type": "site", "element_list": manual_element_list, "custom": False, "custom_name": None})
    layer = manual_layer if manual_layer else sla_pm.layer
    payload = {
        "entities": None,
        "filters": filters,
        "indicators": [int(i) for i in indicators],
        "layer": None,
        "technology": sla_pm.technology,
        "vendor": 'Global',
        "end_date": int(dt_.timestamp() * 1000)  - 3.5* 60 * 60 * 1000 + 1 * 24 * 60 * 60 * 1000 - 1,
        "granularity": sla_pm.granularity,
        "start_date": int(dt_.timestamp() * 1000) - 3.5* 60 * 60 * 1000 ,
        "entity_filters": [{"type": layer, "element_list": ['all'], "custom": False, "custom_name": None}],
        "threshold_filters": [],
        "output": "csv",
        "asynchronous": True,
        "fetch_type": "static",
        "ch_cluster" : "old"
    }
    if sla_pm.technology == 'LMBB':
        payload['filters'] = payload['filters'] + [{"type": "layertech", "element_list": ["L1800", "L2100", "L2300", "L2600"], "custom": False, "custom_name": None}]
    print(json.dumps(payload))
    df_kpi = pd.read_csv(get_kpi_pm_async(payload))
    return df_kpi


def average_region_kpi(df_kpi, report):
    kpi_name = list(report.kpis.filter(special_kpi=True).values_list('name', flat=True))
    df_atoll = pd.DataFrame(AtollData.objects.filter(technology=report.technology).values('cell', 'network', 'sectornotech'))
    df_atoll['count'] = df_atoll.groupby(['network', 'sectornotech']).transform('count')
    df_atoll.sort_values('count', inplace=True)
    df_atoll.drop_duplicates(['network', 'sectornotech'], keep='first')
    df = df_kpi.merge(df_atoll, left_on='element', right_on='sectornotech', how='left')
    df['network'] = df['network'].apply(lambda x: 'unknown' if pd.isna(x) else x)
    fdf = df.groupby(['time', 'region'])[kpi_name].apply(lambda x: x.mean())
    return fdf.reset_index()[['time', 'region'] + kpi_name]

def calculate_bh(df, r):
    kpi_ui_li_id = list(ULImprovementLevel.objects.filter(technology=r.technology).values_list('kpi', flat=True))
    kpi_ui_li_name = list(r.kpis.filter(kpi_id__in=kpi_ui_li_id).values_list('name', flat=True))
    df['time0'] = df['time']
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S').dt.normalize()
    df_daily = pd.DataFrame()
    # calculate region daily KPIs
    if kpi_ui_li_name:
        df_daily = df.groupby(['time', 'element'])[kpi_ui_li_name].apply(lambda x: x.sum()).reset_index()
    kpi_name = pd.DataFrame(r.kpis.values('name', 'bh_kpi_name'))
    kpi_name['bh_kpi_name'] =  kpi_name['bh_kpi_name'].fillna("")
    cs_bh_kpis = list(kpi_name[kpi_name['bh_kpi_name'].apply(lambda x: True if 'erlang' in x.lower() else False)]['name'])
    # set the default bh to ps
    ps_bh_kpis = list(kpi_name[kpi_name['bh_kpi_name'].apply(lambda x: False if 'erlang' in x.lower() else True)]['name'])
    bh_kpi_list = list(set(kpi_name['bh_kpi_name']))
    bh_kpi_list.remove("") if "" in bh_kpi_list else bh_kpi_list
    cs_bh_kpi = ps_bh_kpi = None
    for item in bh_kpi_list:
        if 'erlang' in item.lower():
            cs_bh_kpi = item
        else:
            ps_bh_kpi = item
    fdf = pd.DataFrame()
    if ps_bh_kpi:
        df_ps = df[['time0', 'time', 'element'] + ps_bh_kpis]
        # Calculate exact PS BH
        # fdf = df_ps.loc[df.groupby(['time', 'element'])[ps_bh_kpi].idxmax()][df_ps.columns]
        df_ps['h'] = df_ps['time0'].apply(lambda x: x.split(" ")[1].split(":")[0])
        fdf = df_ps[df_ps['h']=='23'][['time', 'element'] + ps_bh_kpis]
    if cs_bh_kpi:
        df_cs = df[['time', 'element']+cs_bh_kpis]
        fdf_cs =  df_cs.loc[df.groupby(['time', 'element'])[cs_bh_kpi].idxmax()][df_cs.columns]
        fdf = fdf.merge(fdf_cs, left_on=['time', 'element'], right_on=['time', 'element'], how='left')
    # add region daily KPIs
    if kpi_ui_li_name:
        fdf.drop(columns=kpi_ui_li_name, inplace=True)
        fdf = fdf.merge(df_daily, left_on=['time', 'element'], right_on=['time', 'element'], how='left')
    return fdf


def prepare_for_insert_clickhouse(df, report):
    df['layer'] = report.layer
    network = 'Irancell'
    network = 'USO' if 'irancell' not in report.network.lower() and 'uso' in report.network.lower() else network
    network = 'Global' if 'irancell' in report.network.lower() and 'uso' in report.network.lower() else network
    df['network'] = network
    df['technology'] = report.technology
    if 'str' in str(set([type(i) for i in df['time']])):
        df['time'] = df['time'].apply(lambda x: pd.to_datetime(x))
    df['created_at'] = datetime.datetime.now().replace(microsecond=0)
    return df


def delete_data_clickhouse(table_name, ch_cluster, report, time_, type='kpi'):
    ch = ClickhouseApi(ch_cluster)
    time_ = str([f"'{i}'" for i in time_]).replace('[', '(').replace(']', ')').replace('"', "")
    total_rows = ch.client.execute(f"""select count(*) from {table_name} 
    WHERE layer='{report.layer}' and type='{type}' and technology='{report.technology}' and time in {time_}""")[0][0]
    if total_rows:
        print(f"""ALTER TABLE {table_name} DELETE WHERE 
        layer='{report.layer}' and technology='{report.technology}' and time in {time_} and type='{type}'""")
        ch.client.execute(f"""ALTER TABLE {table_name} DELETE WHERE 
        layer='{report.layer}' and technology='{report.technology}' and time in {time_} and type='{type}'""")
    ch.close()
    print(f"{'*'*100}\ndeleted {total_rows} rows for {report.technology} technology, {report.layer} layer and {type} type")


def insert_dataframe_batch(df, table_name, ch_cluster="",batch_size=10000):
    ch = ClickhouseApi(ch_cluster)
    df_clickhouse = ch.client.query_dataframe('select * from mt_sla limit 1')
    df = df[df_clickhouse.columns]
    total_rows = df.shape[0]
    for i in range(0, total_rows//batch_size + 1):
        batch = df.iloc[i * batch_size:(i + 1) * batch_size]
        ch.client.execute(f"insert into {table_name} values ", batch.to_dict('records'))
        print(f"Inserted batch {i + 1}: {len(batch)} rows")
    ch.close()
    print(f"{'*'*100}\nTotal inserted: {total_rows} rows")


def calculate_sla_kpi(dt, report):
    print(f"{'*'*100}\nStart report= {report.technology}: {report.layer}")
    df = get_sla_kpi(dt, report)
    if report.layer == 'sector' and report.technology in {'LMBB', 'LFBB'}:
        df['element'] = df['element'].fillna("")
        df["site"] = df.element.map(lambda x: x[:-1])
        site_68 = list(set(df[df['element'].apply(lambda x: True if x[-1] in ('E', 'F', 'G', 'H') else False)]['site']))
        df_68 = get_sla_kpi(dt, report, site_68, 'physical_sector')
        df = df[df['site'].apply(lambda x:False if x in set(site_68) else True)]
        df = pd.concat([df, df_68])
    if report.technology == 'NMBB' and report.layer == 'region':
        df_avg = get_sla_kpi(dt, report, None, manual_layer='sector', special_kpi=True)
        df_avg = average_region_kpi(df_avg, report)
        df = df.merge(df_avg, left_on=['time', 'element'], right_on=['time', 'region'], how='left')
    if report.layer == 'region':
        df = calculate_bh(df, report)
    # save merged KPI in clickhouse
    kpis = list(report.kpis.values_list('name', flat=True))
    df = df[['time', 'element']+kpis]
    fdf = df.groupby(['time', 'element'])[kpis].apply(lambda x: json.dumps(x.to_dict('records')[0])).reset_index(name='data')
    fdf = prepare_for_insert_clickhouse(fdf, report)

    fdf['type'] = 'kpi'
    delete_data_clickhouse('mt_sla', "", report, list(set(fdf['time'])), type='kpi')
    insert_dataframe_batch(fdf, 'mt_sla')


