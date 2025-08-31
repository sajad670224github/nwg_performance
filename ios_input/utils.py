import time
import json
import random
import datetime
import pandas as pd
from django import db
import hashlib
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

from ios_input.ios_browser import get_orm_data
from ios_input.models import AtollData


def hash_data_function(x, columns):
    data_ = ''
    for col in columns:
        data_ += str(x[col])
    return hashlib.sha1(json.dumps(data_).encode()).hexdigest()


def atoll_data():
    columns = ["sector", "site", "city", "province", "subregion", "region", "network", "vendor", "technology",
               "sectornotech", "ne"]
    payload = {
        "layer": "cell",
        "model_cols": ["name"] + columns,
        "filters": {}
    }
    df_atoll = get_orm_data(payload)[["name"] + columns]
    df_atoll.drop_duplicates(['name'], inplace=True)
    df_atoll['hash_data'] = df_atoll.apply(lambda x: hash_data_function(x, columns), axis=1)
    return df_atoll

def import_data_to_model(model_, df):
    count_create = 0
    if df.empty:
        return 0
    df_records = df.to_dict('records')
    for record in df_records:
        try:
            now = datetime.datetime.now().replace(microsecond=0, second=0)
            model_fields = {f.name for f in model_._meta.get_fields()}
            filtered_record = {
                k: v for k, v in record.items()
                if k in model_fields and k not in {'created_at', 'updated_at', 'id'} and k in df.columns
            }
            filtered_record['cell'] = record['name']
            filtered_record['created_at'] = now
            filtered_record['updated_at'] = now
            item = model_(**filtered_record)
            item.save()
            count_create += 1
        except Exception as e:
            print(f"the error on saving {record['name']} is {str(e)}")
    return count_create



def update_data_in_model(model_, df):
    update_create = 0
    if df.empty:
        return 0
    df_records = df.to_dict('records')
    for record in df_records:
        try:
            now = datetime.datetime.now().replace(microsecond=0, second=0)
            model_fields = {f.name for f in model_._meta.get_fields()}
            filtered_record = {
                k: v for k, v in record.items()
                if k in model_fields and k not in {'created_at', 'updated_at', 'id', 'hash'} and k in df.columns
            }
            filtered_record['updated_at'] = now
            model_.objects.filter(id=int(record['id'])).update(**filtered_record)
            update_create += 1
        except Exception as e:
            print(f"the error on saving {record['name']} is {str(e)}")
    return update_create


def import_chunk_data_to_model(chunk_size, model_, df, type_):
    count_record = 0
    record_step = chunk_size
    for step in range(df.shape[0] // record_step + 1):
        print(f"progress is {step / (df.shape[0] // record_step + 1) * 100}")
        if type_ == 'create':
            count_record += import_data_to_model(model_, df[step * record_step:(step + 1) * record_step])
        if type_ == 'update':
            count_record += update_data_in_model(model_, df[step * record_step:(step + 1) * record_step])
    return count_record

def update_create_atoll_data():
    df_atoll = atoll_data()
    df_model = pd.DataFrame(AtollData.objects.all().values('id','cell','hash_data'))
    if df_atoll.empty:
        return f"atoll data is empty"
    if df_model.empty:
        return import_chunk_data_to_model(10000, AtollData, df_atoll, 'create')
    fdf = df_atoll.merge(df_model, left_on='name', right_on='cell', how='left', suffixes=['','_x'])
    df_create = fdf[fdf['id'].isna()==True].drop(columns=['id', 'cell', 'hash_data_x'])
    count_create = import_chunk_data_to_model(10000, AtollData, df_create, 'create')
    df_update = fdf[(fdf['hash_data']!=fdf['hash_data_x'])&(fdf['hash_data_x'].isna()==False)].drop(columns=['cell', 'hash_data_x'])
    count_update = import_chunk_data_to_model(10000, AtollData, df_update, 'update')
    return count_create, count_update