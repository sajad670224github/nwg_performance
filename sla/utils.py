import json
import datetime

from ios_input.clickhouse import ClickhouseApi
from ios_input.models import AtollData
from ios_input.ios_browser import get_orm_data

def get_clickhouse_column_information(col, data):
    ch = ClickhouseApi("")
    time_ = [data['start_date'], data['end_date']]
    condition = f" time between '{time_[0]}' and '{time_[1]}' "
    for item in data:
        if data[item] and "_date" not in item:
            elements = data[item]
            elements = [f"'{i.strip()}'" for i in elements.split(',')]
            condition += f""" and {item} in ({','.join(elements)}) """
    limit_query = ""
    if col == 'kpi':
        limit_query = f" limit 1 by network, technology, layer, type"
    select_query = "data" if col == 'kpi' else f"distinct {col}"

    query = f"select {select_query} from mt_sla where {condition} {limit_query}"
    print(query)
    df = ch.client.query_dataframe(query)
    ch.close()
    output = []
    if col == 'kpi':
        output += [list(json.loads(df.iloc[i]['data']).keys()) for i in df.index]
        output = sum(output, [])
        return list(set(output))
    return list(df[col])
def get_clickhouse_kpi_information(type_, time, technology):
    ch = ClickhouseApi("")
    condition = f" time between '{time[0]}' and '{time[1]}' and technology='{technology}' and type='{type_}'"
    query = f"select data from mt_sla where {condition} limit 1"
    print(query)
    df = ch.client.query_dataframe(query)
    ch.close()
    kpis = list(json.loads(df.iloc[0]['data']).keys()) if df.shape[0] > 0 else []
    return kpis


def get_user_elements(request, *args, **kwargs):
    user = request.user
    # TO DO
    # need an API to get user groups
    return ['All']



def get_sla_kpi(date_, technology, network, layer, kpis, elements, type_):
    ch = ClickhouseApi("")
    type_ = [f"'{i.strip()}'" for i in type_.split(',')]
    elements = [f"'{i.strip()}'" for i in  elements.split(',')]
    kpis = [f"{i.strip()}" for i in kpis.split(',')]
    technology = [f"'{i.strip()}'" for i in  technology.split(',')]
    network = [f"'{i.strip()}'" for i in  network.split(',')]
    layer = [f"'{i.strip()}'" for i in  layer.split(',')]
    time_condition = f" time between '{date_[0]}' and '{date_[1]}'"
    kpi_query = ""
    for kpi in kpis:
        kpi_query += f" , JSONExtractFloat(data, '{kpi}') as {kpi}"
    query = (f"select time, element, network, layer, technology, type {kpi_query} from mt_sla where {time_condition} "
             f"and network in ({','.join(network)}) and technology in ({','.join(technology)}) and "
             f"element in ({','.join(elements)}) and layer in ({','.join(layer)}) and type in ({','.join(type_)})")
    print(query)
    df = ch.client.query_dataframe(query)
    ch.close()
    return df


def serializing_chart_output(df):
    if df.empty:
        return df
    df['time'] = df['time'].apply(lambda x:x.isoformat())
    serialized_data = {}
    kpis = set(df.columns) - {"time", "element", "network", "layer", "technology", "type"}
    df['element_out'] = df['element']
    if len(set(df['technology'])) > 1:
        df['element_out'] = df['technology'] +"-"+ df['element_out']
    if len(set(df['network'])) > 1:
        df['element_out'] = df['network'] +"-"+ df['element_out']
    serialized_data['indicators'] = list(kpis)
    serialized_data['values'] = []
    for elem in set(df['element_out']):
        data_dict = {}
        data_dict['element'] = elem
        df_temp = df[df['element_out'] == elem].copy()
        df_t = df_temp.groupby('time')[list(kpis)].apply(lambda x: x.to_dict('records')[0]).reset_index(name='values')
        data_dict['data'] = dict(zip(df_t['time'], df_t['values']))
        serialized_data['values'].append(data_dict)
    return serialized_data