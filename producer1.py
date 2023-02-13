
# Import librairies
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

import ccloud_lib 
import time
from datetime import datetime
import pytz
import requests

# Read the configuration for Confluent Cloud
CONF = ccloud_lib.read_ccloud_config("python.config")

# Set the configuration for the Schema Registry
sr_conf = {
    'url': CONF["schema.registry.url"],
    'basic.auth.user.info': CONF['basic.auth.user.info']
}

# Set the topic name
TOPIC = "velib_map_good" 

# Populate the producer configuration with the Schema Registry parameters
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)

# Create an instance of the Schema Registry client
schema_registry_client = SchemaRegistryClient(sr_conf)

# Create an instance of the Kafka producer
producer = Producer(producer_conf)

# Create the topic in Confluent Cloud
ccloud_lib.create_topic(CONF, TOPIC)

# Define the JSON schema for the records
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

# Create an instance of the JSON serializer using the Schema Registry client
serializer = JSONSerializer(schema_str, schema_registry_client)

# Get the current time
now = datetime.now()

try:

    while True:

        # Make a GET request to the URL to retrieve the data
        res = requests.get("https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json")
        # Decode the response as a JSON object
        data = res.json()

        # Define the key for each record
        record_key ='velib'
        # Extract the data on the bike stations
        stations = data['data']['stations']
        # Extract the last updated timestamp
        date = data['lastUpdatedOther']

        # Keep track of when the loop started producing records
        start_time = time.time()

        # Iterate through each bike station
        for station in stations:
            # Create a dictionary containing the station information to be used as the record's value
            record_value = {
                'date' : date,
                'station_id' : station['station_id'],
                'bike_availables' : station['num_bikes_available']
            }
            # Print the timestamp of when the data was updated
            print(datetime.fromtimestamp(date, tz=pytz.timezone("Europe/Paris")))

            # Produce the record to the specified topic using the defined key and value
            producer.produce(
                TOPIC,
                key=record_key,
                value=serializer(record_value, SerializationContext(TOPIC, MessageField.VALUE)),
            )
            # Sleep for a short period of time to avoid overwhelming the producer with too many requests
            time.sleep(0.001)
        
        # Keep track of when the loop ended producing records
        end_time = time.time()

        # Sleep for the remaining time until the next iteration (60 seconds - the time spent in the loop)
        time.sleep(60 - (end_time - start_time))

# If a keyboard interrupt occurs (when the user presses Ctrl + C), catch the exception
except KeyboardInterrupt:
    print('Producer avorted !...')

# The finally block will always run, regardless of whether or not an exception was raised
finally:
    # Flush any remaining records in the buffer to the topic, ensuring all messages are sent
    producer.flush() 

