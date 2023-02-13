import logging
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2023, 2, 9)
}

def _api_to_s3(**context):
    logging.info(f"Fetching bikes data")
    URL = f"https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
    response = requests.get(URL).json()
    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_bike_data.json"
    res = requests.get("https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json")
    data = res.json()
    stations = data['data']['stations']
    date = data['lastUpdatedOther']
    record_value = []
    for station in stations:
        data = {
            'date' : date,
            'station_id' : station['station_id'],
            'bike_availables' : station['num_bikes_available']
            } 
        
        record_value.append(data)

    with open(filename, 'w') as f:
        for line in record_value:
            f.write(str(line))

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(
        filename=filename,
        key=filename,
        bucket_name=Variable.get("S3BucketName"),
        replace=True
    )
    
    
with DAG(dag_id="api_to_s3_dag", default_args=default_args, start_date=datetime.now(), schedule_interval="@daily", catchup=False) as dag:
    api_to_s3 = PythonOperator(task_id="api_to_s3", python_callable=_api_to_s3)   
    api_to_s3