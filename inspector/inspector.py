import paho.mqtt.client as mqtt
from datetime import datetime
import time
import math
import json
from influxdb import InfluxDBClient
from DataExaminer import DataExaminer


#Declaring Global Variables
HOST = "iot.eclipse.org"
PORT = 1883
KEEPALIVE = 60

#MQTT Topic to communicate with Notifier
topic =  "data/anomalyDetected"
calculatorCommsTopic = "communication/influxdbConverted"
#MQTT Client ID to remain unique
client_id = "/Inspector"


db_host = 'localhost'#'influxdb' #'localhost'
db_port = 8086
db_username = 'root'
db_password = 'root'
database = 'testing'

thresholdValue = [0]




def wait_for_influxdb(db_client):
    """Function to wait for the influxdb service to be available"""
    try:
        db_client.ping()
        print(f"InfluxDBClient Connected | {datetime.now()}")
        return None
    except:
        print("InfluxDBClient Connection FAILED: Trying again (1s)")
        time.sleep(1)
        wait_for_influxdb(db_client)






def connect_to_broker(client_id, host, port, keepalive, on_connect, on_message, on_publish):
    ''' Params -> Client(client_id=””, clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)
    We set clean_session False, so in case connection is lost, it'll reconnect with same ID '''
    client = mqtt.Client(client_id=client_id, clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    connection = client.connect(host, port, keepalive)
    return (client, connection)


def main():
    ''' Main Function houses the main mqtt loop, where the dataExaminers look for
    anomalies, and report them to notifier when a threshold is broken '''

    db_client = InfluxDBClient(host=db_host, port=db_port, username=db_username, password=db_password, database=database)
    # waits for influxdb service to be active
    wait_for_influxdb(db_client=db_client)


    def on_connect(client, userdata, flags, rc):
        if rc==0:
            print(f"Connected to MQTT Broker | {datetime.now()}")
            client.subscribe(calculatorCommsTopic, 2)
        else:
            print(f"Bad Connection Returned (Code: {rc})")
        pass

    def on_publish(client, userdata, result):
        # Function for clients's specific callback when pubslishing message
        print(f"Publish #{result} Complete | {datetime.now()}")
        pass


    def on_message(client, userdata, message):
        # Function for clients's specific callback when pubslishing message
        initialTime = time.time()
        payload = message.payload.decode()
        print("----------Message Received----------")
        print(datetime.now())

        location, convertedDataIndexes = json.loads(payload)
        #print("ConvertedDataIndexes: " + str(convertedDataIndexes))
        for dataPackage in convertedDataIndexes:

            measurement = location + '_' + dataPackage['sensorName']
            numberOfReadings = dataPackage['amountOfNewData']

            currentExaminer = None

            query_to_execute = f'select * from "{measurement}" group by * order by DESC limit {numberOfReadings}'

            results = db_client.query(query_to_execute).raw['series']
            data = results[0]['values']

            

            #print(str(data[0]) +" " + str(dataPackage['adc']) +" "+ str(dataPackage['channel']))

            if measurement in DataExaminer.dataExaminers:
                currentExaminer = DataExaminer.dataExaminersAccess[measurement]
            else:
                currentExaminer = DataExaminer(dataPackage['sensorName'], location, thresholdValue)

            anomalies = currentExaminer.examineData(data)

            if len(anomalies) > 0:
                for anomaly in anomalies:
                    client.publish("data/anomalyDetected", anomaly, qos=2)
                    print(f"Sent Anomaly Data | {datetime.now()}")

        print(f"Finished Analyizing Batch of Data from ({location}) | {time.time()-initialTime}")


    #Establish Connection to the MQTT Broker
    client, connection = connect_to_broker(client_id=client_id, host=HOST, port=PORT, keepalive=KEEPALIVE, on_connect=on_connect, on_message=on_message, on_publish=on_publish)

    #Begin the connection loop, where within the loop, messages can be sent
    client.loop_forever()
    #client.disconnect()










if __name__ == '__main__':
    main()
