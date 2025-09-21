import datetime
import json
import pandas as pd
import numpy as np
from pandas import json_normalize

from sla.models import Target
from ios_input.models import AtollData
from ios_input.clickhouse import ClickhouseApi
from sla.kpi_calc import prepare_for_insert_clickhouse, insert_dataframe_batch, delete_data_clickhouse


HI_COEFS = [0, 30, 20, 12, 8, 4, 2, 1]
HI_COL_NAMES = ["min_val", "worst2", "worst1", "worst", "best", "best1", "best2", "max_val"]

def calc_hi(x):
    s = 0
    if pd.isna(x["value"]):
        return np.nan
    negative_kpi = x["min_val"] > x["max_val"]
    # print(x.index, negative_kpi)
    if (negative_kpi and x["value"] >= x["min_val"]) or (not negative_kpi and x["value"] <= x["min_val"]):
        return 0
    for i in range(len(HI_COEFS) - 1):
        col0, col1 = HI_COL_NAMES[i], HI_COL_NAMES[i + 1]
        if not negative_kpi:
            if x["value"] >= x[col1]:
                s += HI_COEFS[i+1] * abs(x[col1] - x[col0])
            else:
                s += HI_COEFS[i+1] * abs(x["value"] - x[col0])
                return s
        else:
            if x["value"] <= x[col1]:
                s += HI_COEFS[i + 1] * abs(x[col1] - x[col0])
            else:
                s += HI_COEFS[i + 1] * abs(x["value"] - x[col0])
                return s
    return s

def atoll_sector_region(report):
    technology = getattr(report, 'technology', report)
    df_atoll = pd.DataFrame(
        AtollData.objects.filter(technology=technology).values('cell', 'region', 'sectornotech'))
    df_atoll['count'] = df_atoll.groupby(['region', 'sectornotech'])['cell'].transform('count')
    df_atoll.sort_values('count', inplace=True, ascending=False)
    df_atoll = df_atoll.drop_duplicates(['region', 'sectornotech'], keep='first')[['region', 'sectornotech']]
    return df_atoll

def daily_hi_calculation(dt, report):
    network = [f"'{i}'" for i in report.network.split(',')]
    df_targets = pd.DataFrame(Target.objects.filter(level=report.layer, technology=report.technology, year=dt.year).values())
    df_kpi_name = pd.DataFrame(report.kpis.all().values('kpi_id', 'name'))
    df_targets = df_targets.merge(df_kpi_name, left_on='kpi', right_on='kpi_id', how='left')
    # find pm kpi
    ch = ClickhouseApi("")
    df_kpi = ch.client.query_dataframe(
        f"""select * from mt_sla 
            where technology='{report.technology}' and type='kpi' and 
            layer='{report.layer}' and network in ({','.join(network)}) 
            and time='{dt}'"""
    )
    ch.close()
    print(f"get kpis from clickhouse {df_kpi.shape}")
    # extract jason to data frame
    df_kpi['json'] = df_kpi['data'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    json_df = json_normalize(df_kpi['json'])
    df_kpi = pd.concat([df_kpi[['time', 'element']], json_df], axis=1)
    df_kpi = df_kpi.fillna(np.nan)
    # add region data if layer is sector
    if report.layer == 'sector':
        cols = list(df_kpi.columns)
        df_atoll = atoll_sector_region(report)
        df_kpi = df_kpi.merge(df_atoll, left_on='element', right_on='sectornotech', how='left')
        df_kpi = df_kpi[cols+['region']]
    else:
        df_kpi['region'] = df_kpi['element']
    # remove unknown region
    df_kpi = df_kpi[(df_kpi['region'].isna() == False) & (df_kpi['region'] != 'unknown')]
    # melt data
    print(f"strat calculating of normalized index")
    cols_melt = ['time', 'element', 'region']
    df_kpi = df_kpi.melt(id_vars=cols_melt)
    fdf = df_kpi.merge(df_targets[['region', 'name', 'weight']+HI_COL_NAMES], left_on=['region', 'variable'], right_on=['region', 'name'], how='left')
    ### This part is added due to local issue and can be removed on server
    fdf1 = fdf.copy()
    fdf = pd.DataFrame()
    steps = 100000
    for i in range(fdf1.shape[0]//steps+1):
        print('*'*100, i)
        fdf0 = fdf1.iloc[steps*i:(i+1)*steps].copy()
        fdf0["raw_index"] = fdf0.apply(calc_hi, axis=1)
        fdf0["max_index"] = abs(
            HI_COEFS[1] * (fdf0["worst2"] - fdf0["min_val"]) +
            HI_COEFS[2] * (fdf0["worst1"] - fdf0["worst2"]) +
            HI_COEFS[3] * (fdf0["worst"] - fdf0["worst1"]) +
            HI_COEFS[4] * (fdf0["best"] - fdf0["worst"]) +
            HI_COEFS[5] * (fdf0["best1"] - fdf0["best"]) +
            HI_COEFS[6] * (fdf0["best2"] - fdf0["best1"]) +
            HI_COEFS[7] * (fdf0["max_val"] - fdf0["best2"])
        )
        fdf = fdf0 if fdf.empty else pd.concat([fdf, fdf0])
    fdf["normalized_index"] = 100 * (fdf["raw_index"] / fdf["max_index"])
    fdf["normalized_index"] = fdf["normalized_index"].map(lambda x: x if pd.isna(x) else min(x, 100))
    fdf["weighted_index"] = fdf["weight"] * fdf["normalized_index"] / 100
    fdf["corr_weight"] = fdf["normalized_index"] * fdf["weight"] / fdf["normalized_index"]
    mask = fdf['normalized_index'] == 0
    fdf.loc[mask, 'corr_weight'] = fdf.loc[mask, 'weight']
    #fdf["corr_weight"] = fdf.apply(lambda x: x['weight'] if x['normalized_index']==0 else x["corr_weight"], axis=1)
    fdf = fdf[["element", "variable", "weighted_index", "weight", "corr_weight"]]
    fdf = fdf.groupby(["element"]).sum(["weighted_index", "corr_weight", "weight"])
    # remove effect of NaN KPIs
    fdf["weighted_index"] = fdf["weighted_index"] * fdf["weight"] / fdf["corr_weight"]
    fdf.drop(["weight", "corr_weight"], axis=1, inplace=True)
    fdf.reset_index(inplace=True)
    fdf["time"] = datetime.datetime(dt.year, dt.month, dt.day, 0, 0)
    #  insert into clickhouse
    fdf = fdf.groupby(['time', 'element'])[['weighted_index']].apply(lambda x: json.dumps(x.to_dict('records')[0])).reset_index(
        name='data')
    fdf = prepare_for_insert_clickhouse(fdf, report)
    fdf['type'] = 'hi'
    delete_data_clickhouse('mt_sla', "", report, [dt], type='hi')
    insert_dataframe_batch(fdf, 'mt_sla')
