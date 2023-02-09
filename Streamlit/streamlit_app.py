# streamlit_app.py

import streamlit as st
import psycopg2
import pandas as pd 
import requests
import plotly.express as px 
import datetime, pytz

st.set_page_config(layout="wide")
# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

THRESHOLD = 3 
conn = init_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
#@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

rows = run_query("SELECT station_id, bike_availables,date from velib_station;")

df1 = pd.DataFrame(columns = ['station', 'velib_availables'])

df1['station'] = [row[0] for row in rows]
df1['velib_availables'] = [row[1] for row in rows]
date = min([row[2] for row in rows])
date = datetime.datetime.fromtimestamp(date, tz=pytz.timezone("Europe/Paris"))

st.write(date)

# station_num = []
# velib_availables = []
# df1 = pd.DataFrame(columns = ['station', 'velib_availables'])

# # Print results.
# for row in rows:
#     station_num.append(row[0])
#     velib_availables.append(row[1])

# df1['station'] = station_num
# df1['velib_availables'] = velib_availables

try: 

    re = requests.get('https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json')
    data = re.json()['data']['stations']

    stations_list = [station['station_id'] for station in data]
    lat_list = [station['lat'] for station in data]
    lon_list = [station['lon'] for station in data]

except: 

    station = run_query("SELECT station_id, lat, lon from station;")

    stations_list = [stat[0] for stat in station]
    lat_list = [stat[1] for stat in station]
    lon_list = [stat[2] for stat in station]

df2 = pd.DataFrame(columns = ['station', 'lat', 'lon'])

df2['station'] = stations_list
df2['lat'] = lat_list
df2['lon'] = lon_list 

df = df1.merge(df2, how='inner', on='station')

df_reduce = df.loc[df['velib_availables'] < THRESHOLD, :]

fig = px.scatter_mapbox(df_reduce, lat=df_reduce['lat'], lon=df_reduce['lon'], size=(THRESHOLD-df_reduce['velib_availables'])*0.001, mapbox_style='open-street-map', zoom=10)

#print(df.loc[df['velib_availables'] < 5, :])

st.plotly_chart(fig,use_container_width=True)
