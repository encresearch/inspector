import paho.mqtt.client as mqtt
import datetime
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

thresholdValue = [10]




def wait_for_influxdb(db_client):
    """Function to wait for the influxdb service to be available"""
    try:
        db_client.ping()
        print("connected to db")
        return None
    except:
        print("not yet")
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
            print(f"Connected OK: {client}")
            client.subscribe(calculatorCommsTopic, 2)
        else:
            print(f"Bad Connection Returned (Code: {rc})")
        pass

    def on_publish(client, userdata, result):
        # Function for clients's specific callback when pubslishing message
        print(f"Publish #{result} Complete")
        pass


    def on_message(client, userdata, message):
        # Function for clients's specific callback when pubslishing message
        payload = message.payload.decode()
        print("Message Received")
        #print(payload)
        #print()
        try:
            location, convertedDataIndexes = json.loads(payload)
            #print("ConvertedDataIndexes: " + str(convertedDataIndexes))
            for dataPackage in convertedDataIndexes:
                #print(1)
                #print(dataPackage)

                measurement = location + '_' + dataPackage['sensorName']
                numberOfReadings = dataPackage['amountOfNewData']

                print(measurement)

                currentExaminer = None
                #print(2)

                query_to_execute = f'select * from "{measurement}" group by * order by DESC limit {numberOfReadings}'

                results = db_client.query(query_to_execute).raw['series']
                data = results[0]['values']


                print(str(data[0]) +" " + str(dataPackage['adc']) +" "+ str(dataPackage['channel']))


                print("Queried")

                if measurement in DataExaminer.dataExaminers:
                    #print("if1")
                    currentExaminer = DataExaminer.dataExaminersAccess[measurement]
                else:
                    #print("if2")
                    currentExaminer = DataExaminer(dataPackage['sensorName'], location, thresholdValue)
                    #print("endif2")
                #print("examiners")

                anomalies = currentExaminer.examineData(data)

                print("Examined")


                if len(anomalies) > 0:
                    for anomaly in anomalies:
                        print(str(anomaly))
                        client.publish("data/anomalyDetected", anomaly)
        except Exception as e:
            print(e)
            raise FlowException("Process Exception", e)

    #Establish Connection to the MQTT Broker
    client, connection = connect_to_broker(client_id=client_id, host=HOST, port=PORT, keepalive=KEEPALIVE, on_connect=on_connect, on_message=on_message, on_publish=on_publish)

    #Begin the connection loop, where within the loop, messages can be sent
    client.loop_forever()
    #client.disconnect()





def examineData(topic, location, dataFunction):
    '''
    Function (Coroutine) used to analyze data in realtime. It will
    need to be modified once access to real data from a database is
    impletemented.
    '''

    #Time in seconds from the beginning of the first call to this coroutine
    initialTime = time.time()
    #If the value of the data is greater than this value, an anomaly is detected
    dataThreshold = 0
    #Boolean variable to keep track of whether or not an anomaly has been detected
    thresholdBroken = False
    #Time in seconds when the anomaly is detected
    detectedTime = 0
    #Time stamp of the time when the anomaly is detected
    detectedTimeStamp = ""
    #Time in seconds when the data goes back within the normal value range (below dataThreshold)
    endTime = 0


    while True:
        #Change in time from the beginning of this coroutine, to the current execution
        dt = time.time()-initialTime
        #Current Data Value
        value = dataFunction(dt)

        #If the current data value is greater than the threshold --> Anomaly detected
        if value > dataThreshold:
            #If the previous data was not an anomaly, than this is the beginning of the anomaly
            if thresholdBroken == False:
                thresholdBroken = True
                detectedTime = time.time()
                detectedTimeStamp = str(datetime.datetime.now().strftime("%m-%d-%y %H:%M:%S.%f"))
                inspectorPackageJSON = createJSON(topic, "detected", location, detectedTimeStamp, 0)
                yield inspectorPackageJSON
                continue
        #If the value of data is within normal range
        else:
            #If the previous data analyzed was part of an anomaly --> Anomaly over
            if thresholdBroken == True:
                thresholdBroken = False
                endTime = time.time()
                inspectorPackageJSON = createJSON(topic, "finished", location, detectedTimeStamp, endTime-detectedTime)
                #yield the JSON package after the anomaly
                yield inspectorPackageJSON
                continue
        #If there is no anomaly to report, yield None, and return to the loop
        yield None





if __name__ == '__main__':
    main()
