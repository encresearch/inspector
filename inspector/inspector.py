import os
import time
import json
from datetime import datetime

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from requests.exceptions import ConnectionError

from inspector.data_examiner import DataExaminer

# INFLUX GLOBALS
DB_HOST = os.getenv("DB_HOST", "influxdb")
DB_PORT = int(os.getenv("DB_PORT", "8086"))
DB_USERNAME = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "sensor_data")

# BROKER GLOBALS
BROKER_HOST = os.getenv("BROKER_HOST", "mqtt.eclipse.org")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
BROKER_KEEPALIVE = int(os.getenv("BROKER_KEEPALIVE", "60"))
BROKER_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "/Inspector")
CALCULATOR_MQTT_TOPIC = "communication/influxdbConverted"
NOTIFIER_MQTT_TOPIC = "data/anomalyDetected"

THRESHOLD_VALUE = [0]


def wait_for_influxdb(db_client):
    """Function to wait for the influxdb service to be available"""
    try:
        db_client.ping()
        print(f"InfluxDBClient Connected | {datetime.now()}")
    except ConnectionError:
        print("InfluxDBClient Connection FAILED: Trying again (1s)")
        time.sleep(1)
        wait_for_influxdb(db_client)


def connect_to_broker(
    client_id,
    host,
    port,
    keepalive,
    on_connect,
    on_message,
    on_publish
):
    """Connect to our MQTT Broker."""
    client = mqtt.Client(client_id=client_id, clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    connection = client.connect(host, port, keepalive)
    return (client, connection)


def main():
    """Main Function houses the main mqtt loop, where the dataExaminers look
    for anomalies, and report them to notifier when a threshold is broken."""

    db_client = InfluxDBClient(
        host=DB_HOST,
        port=DB_PORT,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    wait_for_influxdb(db_client=db_client)

    def on_connect(client, userdata, flags, rc):
        """Function for clients's specific callback when connected to broker"""
        if rc == 0:
            print(f"Connected to MQTT Broker | {datetime.now()}")
            client.subscribe(CALCULATOR_MQTT_TOPIC, 2)
        else:
            print(f"Bad Connection Returned (Code: {rc})")

    def on_publish(client, userdata, result):
        """Function for clients's specific callback when pubslishing message"""
        print(f"Publish #{result} Complete | {datetime.now()}")

    def on_message(client, userdata, message):
        """Function for clients's specific callback when receiving message.

        If an anomaly is detected, a message is published of the form:
        {
            "topic": [
                "test_env/usa/quincy/1",
                "Nitrogen Gas Sensor (MQ131)",
                "test_env/usa/quincy/1_Nitrogen Gas Sensor (MQ131)"
            ],
            "anomaly_status": "Started",
            "location": "test_env/usa/quincy/1",
            "time_init": "2020-07-06T18:52:07.894296Z",
            "time_duration": "N/A"
        }
        """
        initial_time = time.time()
        payload = message.payload.decode()
        print("----------Message Received----------")
        print(datetime.now())

        location, convertedDataIndexes = json.loads(payload)
        for dataPackage in convertedDataIndexes:

            measurement = location + '_' + dataPackage['sensorName']
            numberOfReadings = dataPackage['amountOfNewData']

            current_examiner = None

            query_to_execute = (
               'select * from "{}" group by * order by DESC limit {}'.format(
                   measurement,
                   numberOfReadings
                )
            )
            results = db_client.query(query_to_execute).raw['series']
            data = results[0]['values']

            if measurement in DataExaminer.dataExaminers:
                current_examiner = DataExaminer.dataExaminersAccess[
                    measurement
                ]
            else:
                current_examiner = DataExaminer(
                    dataPackage['sensorName'],
                    location,
                    THRESHOLD_VALUE
                )

            anomalies = current_examiner.examineData(data)

            if len(anomalies) > 0:
                for anomaly in anomalies:
                    client.publish(
                        NOTIFIER_MQTT_TOPIC,
                        anomaly,
                        qos=2
                    )
                    print(f"Sent Anomaly Data | {datetime.now()}")
                    print(anomaly)

        print("Finished Analyizing Batch of Data from ({}) | {}".format(
            location,
            time.time() - initial_time
        ))

    # Establish Connection with the MQTT Broker
    client, connection = connect_to_broker(
        client_id=BROKER_CLIENT_ID,
        host=BROKER_HOST,
        port=BROKER_PORT,
        keepalive=BROKER_KEEPALIVE,
        on_connect=on_connect,
        on_message=on_message,
        on_publish=on_publish
    )
    client.loop_forever()
