import datetime
import json
import pandas as pd

from sla.models import TechnologyWeight
from ios_input.clickhouse import ClickhouseApi
from sla.hi_calc import atoll_sector_region
from sla.kpi_calc import insert_dataframe_batch, delete_data_clickhouse
def apply_priority_weights(prio):
    return {"P1": 2, "P2": 1.5, "P3": 1.2, "P4": 1}.get(prio)

def apply_technology_weights(tech):
    return {"GSM": 0.1, "UMTS": 0.15, "LMBB": 0.7, "LFBB": 0, "NMBB": 0.05}.get(tech)


def load_priorities():
    df = pd.read_excel("prio.xlsx")
    df = df.rename(columns={"SITEID": "site_id", "Priority": "priority"})
    df["priority"] = df["priority"].map(lambda x: str(x).replace("Priority ", "P"))
    return df[["site_id", "priority"]]


def determine_quarter_range(dt):
    month = (dt.month - 1) // 3
    print(month)
    start_dt = datetime.date(dt.year, 3 * month + 1, 1)
    end_dt = datetime.date(dt.year + 1, 1, 1) if month > 2 else datetime.date(dt.year, 3 * (month+1) + 1, 1)
    end_dt -= datetime.timedelta(days=1)
    return start_dt, end_dt

    start_month = (quarter - 1) * 3 + 1
    start_day = 1
    end_month = start_month + 2
    end_day = calendar.monthrange(year, end_month)[1]
    start_dt = datetime.date(year, start_month, start_day)
    end_dt = datetime.date(year, end_month, end_day)
    return start_dt, end_dt

def calculate_tech_score(dt, start_dt, end_dt, technology, weight, network):
    net = [f"'{i}'" for i in network]
    ch = ClickhouseApi("")
    df_region = ch.client.query_dataframe(f"""
                select element as region, avg(
                        {weight[0]} * JSONExtractFloat(data, 'hi_score')+
                        {weight[1]} * JSONExtractFloat(data, 'li_score')+
                        {weight[2]} * JSONExtractFloat(data, 'ui_score')
                    ) as region_score from mt_sla where type='score' and technology='{technology}'
                    and layer='region' and network in ({','.join(net)}) 
                    and time between '{start_dt.isoformat()}' and '{end_dt.isoformat()}' 
                    group by element""")
    df_sector = ch.client.query_dataframe(f"""
                select element, avg(
                        {weight[0]} * JSONExtractFloat(data, 'hi_score')+
                        {weight[1]} * JSONExtractFloat(data, 'li_score')+
                        {weight[2]} * JSONExtractFloat(data, 'ui_score')
                    ) score from mt_sla where type='score' and technology='{technology}' and time='{dt}'
                    and layer='sector' and network in ({','.join(net)}) 
                    and time between '{start_dt.isoformat()}' and '{end_dt.isoformat()}' 
                    group by element""")
    df_atoll = atoll_sector_region(technology)
    df_sector = df_sector.merge(df_atoll, left_on='element', right_on="sectornotech", how="inner")
    df_prio = load_priorities()
    df_sector['site'] = df_sector['element'].apply(lambda x: x[:-2])
    df_sector = df_sector.merge(df_prio, left_on='site', right_on='site_id', how='left')
    df_sector['priority'] = df_sector['priority'].fillna('P4')
    df_sector["weight"] = df_sector["priority"].map(apply_priority_weights)
    df_sector["weighted_score"] = df_sector["weight"] * df_sector["score"]
    df_sector = df_sector.groupby("region")[['score', 'weight', 'weighted_score']].sum()
    df_sector["score"] = df_sector["weighted_score"] / df_sector["weight"]
    df_sector.drop(["weighted_score", "weight"], axis=1, inplace=True)
    df_sector = df_sector.rename(columns={"score": f"sector_score"}).reset_index()
    fdf = df_region.merge(df_sector, left_on='region', right_on='region', how='left')
    fdf[f"score"] = 0.5 * (fdf[f"sector_score"] + fdf[f"region_score"])
    fdf['technology'] = technology
    return fdf


def delete_fscore_data_clickhouse(table_name, ch_cluster, time_, type='fscore'):
    ch = ClickhouseApi(ch_cluster)
    time_ = str([f"'{i}'" for i in time_]).replace('[', '(').replace(']', ')').replace('"', "")
    total_rows = ch.client.execute(f"""select count(*) from {table_name} 
    WHERE  type='{type}'  and time in {time_}""")[0][0]
    if total_rows:
        ch.client.execute(f"ALTER TABLE {table_name} DELETE WHERE time in {time_} and type='{type}'")
    ch.close()
    print(f"{'*'*100}\ndeleted {total_rows} rows for  {type} type")


def calculate_final_score(dt, network, _end_dt=None):
    tech_score_list = []
    start_dt, end_dt = determine_quarter_range(dt)
    end_dt = _end_dt if _end_dt else end_dt
    df_weight = pd.DataFrame(TechnologyWeight.objects.all().values())
    for technology in ['GSM', 'UMTS', 'LMBB', 'LFBB', 'NMBB']:
        df =df_weight[df_weight['technology']==technology]
        weight = [df['hi_weight'].values[0], df['li_weight'].values[0], df['ui_weight'].values[0]]
        tech_score_list.append(calculate_tech_score(dt, start_dt, end_dt, technology, weight, network))

    fdf = pd.concat(tech_score_list)
    ## create data for fscore
    df_fscore = fdf.groupby(['region', 'technology'])[['region_score', 'sector_score', 'score']].apply(
        lambda x: json.dumps(x.to_dict('records')[0])).reset_index(name='data')
    fdf['weight'] = fdf['technology'].map(apply_technology_weights)
    fdf['weighted_score'] = fdf['score'] * fdf['weight']
    fdf = fdf[['region', 'technology', 'weighted_score']]
    rdf = fdf.groupby("region")['weighted_score'].sum().reset_index()
    df_mbb_score = rdf.groupby(['region']).apply(
        lambda x: json.dumps(x.to_dict('records')[0])).reset_index(name='data')
    # prepare for clickhouse insert
    df_mbb_score['time'] = df_fscore['time'] = dt
    df_mbb_score['network'] = df_fscore['network'] = ','.join(network)
    df_mbb_score['layer'] = df_fscore['layer'] = 'region'
    df_mbb_score['created_at'] = df_fscore['created_at'] = datetime.datetime.now().replace(microsecond=0)
    df_mbb_score['technology'] = 'unknown'
    df_mbb_score.rename(columns = {'region': 'element'}, inplace=True)
    df_fscore.rename(columns={'region': 'element'}, inplace=True)
    df_mbb_score['type'] = 'mbb_fscore'
    df_fscore['type'] = 'fscore'

    delete_fscore_data_clickhouse('mt_sla', "", [dt], 'fscore')
    insert_dataframe_batch(df_fscore, 'mt_sla')
    delete_fscore_data_clickhouse('mt_sla', "", [dt], 'mbb_fscore')
    insert_dataframe_batch(df_mbb_score, 'mt_sla')
