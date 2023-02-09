# Code inspired from Confluent Cloud official examples library
# https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/producer.py

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import json
import ccloud_lib # Library not installed with pip but imported from ccloud_lib.py
import numpy as np
import time
import http
from datetime import datetime
import requests
import pandas as pd
from datetime import datetime


# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
sr_conf = {
    'url': CONF["schema.registry.url"],
    'basic.auth.user.info': CONF['basic.auth.user.info']
}

TOPIC = "velib_map_good" 

# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
schema_registry_client = SchemaRegistryClient(sr_conf)
producer = Producer(producer_conf)

# Create topic if it doesn't already exist
ccloud_lib.create_topic(CONF, TOPIC)

schema_str = """
{
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "date": {
      "description": "The number type is used for any numeric type, either integers or floating point numbers.",
      "type": "integer"
    },
    "station_id": {
      "description": "The number type is used for any numeric type, either integers or floating point numbers.",
      "type": "integer"
    },
      "bike_availables": {
      "description": "The number type is used for any numeric type, either integers or floating point numbers.",
      "type": "integer"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}"""


serializer = JSONSerializer(schema_str, schema_registry_client)

now = datetime.now()

try:

    while True:

        #now = datetime.now()
        # conn = http.client.HTTPSConnection("realstonks.p.rapidapi.com")

        res = requests.get("https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json")
        data = res.json()

        record_key ='velib'
        stations = data['data']['stations']
        date = data['lastUpdatedOther']

        for station in stations:
          record_value = {
                'date' : date,
                'station_id' : station['station_id'],
                'bike_availables' : station['num_bikes_available']
              } 
          print(station['station_id'])

          producer.produce(
              TOPIC,
              key=record_key,
              value=serializer(record_value, SerializationContext(TOPIC, MessageField.VALUE)),
            )
          time.sleep(0.01)
        time.sleep(60)

 # Interrupt infinite loop when hitting CTRL+C
except KeyboardInterrupt:
    pass
finally:
    producer.flush() # Finish producing the latest event before stopping the whole script