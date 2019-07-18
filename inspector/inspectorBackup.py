import paho.mqtt.client as mqtt
import datetime
import time
import math
import json
from influxdb import DataFrameClient

#Declaring Global Variables
HOST = "iot.eclipse.org"
PORT = 1883
KEEPALIVE = 60

#MQTT Topic to communicate with Notifier
topic =  "data/anomalyDetected"
#MQTT Client ID to remain unique
client_id = "/Inspector"



#Test Data Reading Functions
def gas_sensor_test(timeSinceStart):
    """ Reads values > 0 every 5 seconds for an interval of 5 seconds"""
    return math.sin(2*math.pi*timeSinceStart/10)

def current_sensor_test(timeSinceStart):
    """ Reads values > 0 every 10 seconds, starting at 5.7 seconds"""
    return math.sin((math.pi/10)*(timeSinceStart - 5.7))

def magnetometer_sensor_test(timeSinceStart):
    """ Reads values > 0 every 5 seconds for an interval of 5 seconds"""
    return 10*((1/3)*math.sin(timeSinceStart/3) + 2*math.cos(timeSinceStart/3)*(math.sin(timeSinceStart/3)**2))

#Location array, for each location, the program will initialize dataExaminers
#LOCATIONS = ["HAITI/SOUTH", "HAITI/NORTH", "USA/CA/SOUTH", "USA/CA/NORTH"]
LOCATIONS = ["USA/Quincy/1",]


def createJSON(topic, anomaly_status, location, time_init, time_duration):
    ''' Helper function to quickly create a JSON String
    with the following format'''
    return json.dumps({
            'topic': topic,
            'anomaly_status': anomaly_status,
            'location': location,
            'time_init': time_init,
            'time_duration': time_duration
           })

def connect_to_broker(client_id, host, port, keepalive, on_connect, on_publish):
    ''' Params -> Client(client_id=””, clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)
    We set clean_session False, so in case connection is lost, it'll reconnect with same ID '''
    client = mqtt.Client(client_id=client_id, clean_session=False)
    client.on_connect = on_connect
    client.on_publish = on_publish
    connection = client.connect(host, port, keepalive)
    return (client, connection)


def main():
    ''' Main Function houses the main mqtt loop, where the dataExaminers look for
    anomalies, and report them to notifier when a threshold is broken '''

    def on_connect(client, userdata, flags, rc):
        if rc==0:
            print(f"Connected OK: {client}")
        else:
            print(f"Bad Connection Returned (Code: {rc})")
        pass

    def on_publish(client, userdata, result):
        # Function for clients's specific callback when pubslishing message
        print(f"Publish #{result} Complete")
        pass

    #Establish Connection to the MQTT Broker
    client, connection = connect_to_broker(client_id=client_id, host=HOST, port=PORT, keepalive=KEEPALIVE, on_connect=on_connect, on_publish=on_publish)

    #Begin the connection loop, where within the loop, messages can be sent
    client.loop_start()

    #Initializing empty dictionary to hold all the dataExaminers based on location/Sensor
    dataExaminers = {}

    #Populating the DataExaminers dictionary
    for location in LOCATIONS:
        temp = []
        temp.append(examineData('gas_sensor', location, gas_sensor_test))
        #temp.append(examineData('current_sensor', location, current_sensor_test))
        #temp.append(examineData('magnetometer_sensor', location, magnetometer_sensor_test))

        dataExaminers[location] = temp


    #Initializing array to hold JSON packages tbat will be sent
    packagesToSend = []

    #Infinte Loop to constantly inspect Data
    while True:

        #Inspect realtime data
        for location in LOCATIONS:
            for dataExaminer in dataExaminers[location]:
                packagesToSend.append(next(dataExaminer))

        #Publish data that contains anomlies
        for package in packagesToSend:
            if package != None:
                #print(package)
                client.publish(topic, package)

        #Remove all JSON Packages from the array for the next time in the loop
        for index in range(len(packagesToSend)):
            packagesToSend.pop()


    #Stop the loop/Disconnect
    client.loop_stop()
    client.disconnect()



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
