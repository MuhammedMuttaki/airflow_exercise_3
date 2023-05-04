import os
import time
import json
import requests
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


def get_data(**kwargs):
    ticker = kwargs['ticker']
    api_key = "DNUU56O094WYYVLE"
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + ticker + '&apikey='+api_key
    r = requests.get(url)
    
    try:
        data_da = r.json()
        #print(data_da)
        path = "/data_center/data_lake/"
        with open(path + "stock_market_raw_data_" + ticker + "_" + str(time.time()), 'w') as outfile:
            json.dump(data_da, outfile)
    except:
        pass

default_dag_args = {
    'start_date': datetime(2023, 4, 1),
    'email_on_failure': False,
    'email_on_retries': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

with DAG("market_data", schedule_interval = '@daily', catchup = False, default_args = default_dag_args) as dag_python:

    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'ticker' : ['IBM', 'TSCO.LON', 'SHOP.TRT']})