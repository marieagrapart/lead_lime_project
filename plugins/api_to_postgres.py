from airflow.models.baseoperator import BaseOperator
from  airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import logging
import requests

class APIToPostgresOperator(BaseOperator):
    def __init__(
            self,
            schema,
            table,
            postgres_conn_id = "postgres_default",
            **kwargs
        ) -> None:
        super().__init__(**kwargs)
        self.schema: str = schema
        self.table: str = table
        self.postgres_conn_id: str = postgres_conn_id
        
    def execute(self, context):

        URL = 'https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json'
        
        response = requests.get(URL).json()
        filename = "gps_data.json"
        stations_gps = response['data']['stations']
        stations_id = [station['station_id'] for station in stations_gps]
        lat = [station['lat'] for station in stations_gps]
        lon = [station['lon'] for station in stations_gps]
        d = {}
        for (station, lt, ln) in zip(stations_id, lat, lon):
            d[station] = [lt, ln]
        df = pd.DataFrame(d)
        df = df.T.reset_index()
        df.rename(columns={'index': 'station_id', 0: 'lat', 1: 'lon'}, inplace=True)


        logging.info(df.head())

        #Postgre Hook
        engine = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_sqlalchemy_engine()

        #df to SQL
        df.to_sql(self.table, engine, if_exists="replace", index=False)
