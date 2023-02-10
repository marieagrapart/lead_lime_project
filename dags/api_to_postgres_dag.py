from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from api_to_postgres import APIToPostgresOperator


import logging

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}

with DAG(dag_id="api_to_postgres_dag", default_args=default_args, catchup=False) as dag:

    drop_postgres_table = PostgresOperator(
        task_id="drop_postgres_table",
        sql="""
        DROP TABLE IF EXISTS station ;
        """,
        postgres_conn_id="postgres_default",
    )

    api_to_postgres = APIToPostgresOperator(            
        task_id="api_to_postgres",
        schema="PUBLIC",
        table="station",
        postgres_conn_id = "RDS Jesshuan",

    )

    drop_postgres_table >> api_to_postgres