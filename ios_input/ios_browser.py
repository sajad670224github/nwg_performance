
import requests
import logging
import datetime
import os
import time
import time
import json
import uuid
from pathlib import Path
from io import StringIO
import pandas as pd
from django.core.cache import cache

from ios_input.logger import LoggerMixin
from nwg_performance import settings

logger_file = LoggerMixin()
logger_file.init_logger()
logging.captureWarnings(True)
now = datetime.datetime.now()
# -----------
username = settings.UAT_CONFIG['username']
password = settings.UAT_CONFIG['password']
otpsecret = settings.UAT_CONFIG['otp_secret']
TOKEN_PATH = settings.UAT_CONFIG['token_path']


def get_token():
    auth_tok = None
    if os.path.isfile(TOKEN_PATH):
        token_age = round(time.time() - os.path.getmtime(TOKEN_PATH), 0)
        seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
        if token_age - (86400 - seconds_since_midnight) < 0:
            with open(TOKEN_PATH, 'r') as infile:
                auth_tok = infile.read()
    else:
        Path(TOKEN_PATH).touch()
    return auth_tok
def get_new_token():
    auth_tok = None
    if not auth_tok:
        auth_server_url = "https://uat.ios.mtnirancell.ir/api/auth/token/2fa/login/"
        verify_url = "https://uat.ios.mtnirancell.ir/api/auth/token/2fa/verify/"
        token_req_payload = {#a:2,
            "password": password, "username": username}

        token_response = requests.post(auth_server_url, data=token_req_payload, verify=False, allow_redirects=False)

        if token_response.status_code != 200:
            logger_file.logger.error("Failed to obtain token from the OAuth 2.0 server")
            raise ValueError("Failed to obtain token from the OAuth 2.0 server")
        else:
            print("Successfuly authorized")
            session_id = token_response.json()["session_id"]
            # print(totp.now() )

            request = {"session_id": session_id, "code": '123456'}
            verify_response = requests.post(verify_url, data=request, verify=False, allow_redirects=False)

            auth_tok = verify_response.json()['auth_token']
            os.remove(TOKEN_PATH)
            with open(TOKEN_PATH, 'w') as outfile:
                outfile.write(auth_tok)
            logger_file.logger.info(
                f"The token has been successfully obtained at {datetime.datetime.now().replace(microsecond=0)}")
    return auth_tok

#(st, et, granularity, layer, tech, vendor, indicators, entities, filters, entity_filters, payload=None)

def remove_cache_data(cache_list):
    if 'all' in [i.lower() for i in cache_list]:
        cache.clear()
    else:
        cache.delete_many(cache_list)



def get_kpi_pm_async(payload):
    payload['asynchronous'] = True
    cache_data = cache.get(f"get_kpi_pm_async.{json.dumps(payload)}")
    if cache_data:
        logger_file.logger.error(f"Reading data from the cache: get_kpi_pm_async.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    kpi_explore_url = 'https://uat.ios.mtnirancell.ir/api/pm/v2/kpi_explore/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    kpi_response_payload = requests.post(kpi_explore_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if kpi_response_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_kpi_pm(payload)
    if kpi_response_payload.status_code != 200:
        logger_file.logger.error(f"PM respond Error: {kpi_response_payload.status_code}: {kpi_response_payload.text}")
        raise ValueError(f"There is error on PM response")
    report_id = json.loads(kpi_response_payload.text)['unique_id']
    pm_report_url = 'https://uat.ios.mtnirancell.ir/api/pm/v2/reports/?page=1'
    while True:
        pm_report_payload = requests.get(pm_report_url, params={'page': 1, 'page_size': 10}, verify=False,
                                         allow_redirects=False, headers=headers)
        data = json.loads(pm_report_payload.text)['results']
        report_exist = False
        for item in data:
            if item['id'] == report_id:
                #print(item)
                report_exist = True
                print(f"progress is {item['progress']}")
                if item['progress'] == 100 and item['path'] and item['state_name']=='Completed':
                    logger_file.logger.info(
                        f"The report has been successfully downloaded at https://uat.ios.mtnirancell.ir{item['path']}")
                    cache.set(f"get_kpi_pm_async.{json.dumps(payload)}", f"https://uat.ios.mtnirancell.ir{item['path']}", 2*60*60)
                    return f"https://uat.ios.mtnirancell.ir{item['path']}"
                elif item['state_name'] == 'Failed':
                    logger_file.logger.error(f"The report hase been {item['state_name']}")
                    raise ValueError(f"The report hase been {item['state_name']}")
                break
        if not report_exist:
            logger_file.logger.error(f"Report id does not exist")
            raise  ValueError(f"report id does not exist")
        time.sleep(10)

def get_kpi_pm(payload):
    payload['asynchronous'] = False
    cache_data = cache.get(f"get_kpi_pm.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_kpi_pm.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    kpi_explore_url = 'https://uat.ios.mtnirancell.ir/api/pm/v2/kpi_explore/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    kpi_response_payload = requests.post(kpi_explore_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if kpi_response_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_kpi_pm(payload)
    if kpi_response_payload.status_code != 200:
        raise ValueError(f"The error on pm is {kpi_response_payload.text}")
    out_text = StringIO(kpi_response_payload.text)
    df = pd.read_csv(out_text)
    logger_file.logger.info(f"The report has been successfully generated")
    cache.set(f"get_kpi_pm.{json.dumps(payload)}", df, 2 * 60 * 60)
    return df

def get_kpi_pm_core(payload):
    cache_data = cache.get(f"get_kpi_pm_core.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_kpi_pm_core.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    kpi_explore_url = 'https://uat.ios.mtnirancell.ir/api/pm_core/kpi_explore/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    kpi_response_payload = requests.post(kpi_explore_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if kpi_response_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_kpi_pm(payload)
    if kpi_response_payload.status_code != 200:
        raise ValueError(f"The error on pm is {kpi_response_payload.text}")
    out_text = StringIO(kpi_response_payload.text)
    df = pd.read_csv(out_text)
    logger_file.logger.info(f"data has been successfully receive for get_kpi_pm_core.{json.dumps(payload)}")
    cache.set(f"get_kpi_pm_core.{json.dumps(payload)}", df, 2 * 60 * 60)
    return df


def get_cm_data(payload):
    cache_data = cache.get(f"get_cm_data.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_cm_data.{json.dumps(payload)}")
        return cache_data

    token = get_token()
    if not token:
        token = get_new_token()
    cm_explore_url = 'https://uat.ios.mtnirancell.ir/api/cm/v2/report/generate/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    cm_response_payload = requests.post(cm_explore_url, json=payload, verify=False,
                                        allow_redirects=False, headers=headers)
    if cm_response_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_kpi_pm(payload)
    if cm_response_payload.status_code != 200:
        print(f"The error on pm is {cm_response_payload.text}")
        return pd.DataFrame()
    report_id = json.loads(cm_response_payload.text)['unique_id']
    cm_report_url = 'https://uat.ios.mtnirancell.ir/api/cm/v2/report/list/'
    while True:
        cm_report_payload = requests.get(cm_report_url, params={'page': 1, 'page_size': 10}, verify=False,
                                         allow_redirects=False, headers=headers)
        data = json.loads(cm_report_payload.text)['results']
        report_exist = False
        for item in data:
            if item['id'] == report_id:
                report_exist = True
                print(f"progress is {item['progress']}")
                if item['progress'] == 100:
                    logger_file.logger.info(
                        f"data has been successfully receive for get_cm_data.{json.dumps(payload)}")
                    cache.set(f"get_cm_data.{json.dumps(payload)}", f"https://uat.ios.mtnirancell.ir{item['path']}", 2 * 60 * 60)
                    return f"https://uat.ios.mtnirancell.ir{item['path']}"
                break
        if not report_exist:
            return f"report id does not exist"
        time.sleep(20)


def get_atoll_data(payload):
    cache_data = cache.get(f"get_atoll_data.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_atoll_data.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    atoll_url = 'https://uat.ios.mtnirancell.ir/api/cell/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    atoll_payload = requests.post(atoll_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if atoll_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_kpi_pm(payload)
    if atoll_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {atoll_payload.text}")
        raise ValueError(f"The error on uat is {atoll_payload.text}.")
    payload['rows'] = json.loads(atoll_payload.text)['recordsFiltered']
    atoll_payload = requests.post(atoll_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    out_data = json.loads(atoll_payload.text)['data']
    cache.set(f"get_atoll_data.{json.dumps(payload)}", pd.DataFrame(out_data), 2 * 60 * 60)
    logger_file.logger.info(f"data has been successfully receive for get_atoll_data.{json.dumps(payload)}")
    return pd.DataFrame(out_data)


def get_pm_kpi_option():
    cache_data = cache.get(f"get_pm_kpi_option")
    if cache_data:
        logger_file.logger.error(f"Reading data from the cache: get_pm_kpi_option")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    kpi_option_url = 'https://uat.ios.mtnirancell.ir/api/pm/v2/kpi_options/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    kpi_option_payload = requests.get(kpi_option_url, verify=False, allow_redirects=False, headers=headers)
    if kpi_option_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_pm_kpi_option()
    if kpi_option_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {kpi_option_payload.text}")
        raise ValueError(f"The error on uat is {kpi_option_payload.text}.")
    payload = json.loads(kpi_option_payload.text)
    cache.set('get_pm_kpi_option', payload, 2 * 60 * 60)
    logger_file.logger.info(f"data has been successfully receive for get_pm_kpi_option")
    return payload


def get_pm_network(technology, vendor):
    payload = {
        'technology': technology,
        'vendor': vendor
    }
    cache_data = cache.get(f"get_pm_network.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_pm_network.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    network_url = 'https://uat.ios.mtnirancell.ir/api/pm/v2/entities/network/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    network_payload = requests.post(network_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if network_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_pm_network(technology, vendor)
    if network_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {network_payload.text}")
        raise ValueError(f"The error on uat is {network_payload.text}.")
    networks = json.loads(network_payload.text)
    cache.set(f"get_pm_network.{json.dumps(payload)}", networks, 2 * 60 * 60)
    logger_file.logger.info(f"data has been successfully receive for get_pm_network.{json.dumps(payload)}")
    return networks


def layer_list(layer, cfg_cols):
    payload = {
        "layer": layer,
        "cfg_cols": cfg_cols
    }
    cache_data = cache.get(f"layer_list.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: layer_list.{json.dumps(payload)}")
        return cache_data
    payload = json.loads(json.dumps(payload))
    token = get_token()
    if not token:
        token = get_new_token()
    network_url = 'https://uat.ios.mtnirancell.ir/api/pm/base_layer/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    network_payload = requests.post(network_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if network_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        layer_list(layer, cfg_cols)
    if network_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {network_payload.text}")
        raise ValueError(f"The error on uat is {network_payload.text}.")
    df_layer = pd.DataFrame(json.loads(json.loads(network_payload.text)))
    cache.set(f"layer_list.{json.dumps(payload)}", df_layer, 2 * 60 * 60)
    logger_file.logger.info(f"data has been successfully receive for layer_list.{json.dumps(payload)}")
    return df_layer

def get_ran_pi(technology='Global', vendor='Global', networks=['Irancell']):
    payload = {
        "technology": technology,
        "vendor": vendor,
        "networks": networks
    }
    cache_data = cache.get(f"get_ran_pi.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_ran_pi.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    ran_pi_url = 'https://uat.ios.mtnirancell.ir/api/pm/v2/indicators/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    ran_pi_payload = requests.post(ran_pi_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if ran_pi_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_ran_pi(technology, vendor, networks)
    if ran_pi_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {ran_pi_payload.text}")
        raise ValueError(f"The error on uat is {ran_pi_payload.text}.")
    ran_pi = pd.DataFrame(json.loads(ran_pi_payload.text))
    cache.set(f"get_ran_pi.{json.dumps(payload)}", ran_pi, 2 * 60 * 60)
    logger_file.logger.info(f"data has been successfully receive for get_ran_pi.{json.dumps(payload)}")
    return ran_pi


def get_orm_data(payload):
    cache_data = cache.get(f"get_orm_data.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_orm_data.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    atoll_url = 'https://uat.ios.mtnirancell.ir/api/pm/model_fields/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}

    orm_model_payload = requests.post(atoll_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if orm_model_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_orm_data(payload)
    if orm_model_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {orm_model_payload.text}")
        raise ValueError(f"The error on uat is {orm_model_payload.text}.")
    orm_payload = json.loads(orm_model_payload.text)
    cache.set(f"get_orm_data.{json.dumps(payload)}", pd.DataFrame(orm_payload), 2 * 60 * 60)
    logger_file.logger.info(f"data has been successfully receive for get_orm_data.{json.dumps(payload)}")
    return pd.DataFrame(orm_payload)

def get_pm_ho_data(payload):
    cache_data = cache.get(f"get_pm_ho_data.{json.dumps(payload)}")
    if cache_data is not None:
        logger_file.logger.error(f"Reading data from the cache: get_pm_ho_data.{json.dumps(payload)}")
        return cache_data
    token = get_token()
    if not token:
        token = get_new_token()
    pm_ho_url = 'https://uat.ios.mtnirancell.ir/api/pm/ho_reports/'
    headers = {'authorization': 'Token ' + token, 'Content-Type': 'application/json'}
    pm_ho_payload = requests.post(pm_ho_url, json=payload, verify=False, allow_redirects=False, headers=headers)
    if pm_ho_payload.status_code == 403:
        _ = get_new_token()
        if not get_token():
            logger_file.logger.error("Fail to obtain new token from IOS.")
            raise ValueError(f"Connection to server is not established.")
        get_orm_data(payload)
    if pm_ho_payload.status_code != 200:
        logger_file.logger.error(f"The error on uat is {pm_ho_payload.text}")
        raise ValueError(f"The error on uat is {pm_ho_payload.text}.")
    out_text = StringIO(pm_ho_payload.text)
    df = pd.read_csv(out_text)
    cache.set(f"get_pm_ho_data.{json.dumps(payload)}",df, 2 * 60 * 60)
    return df