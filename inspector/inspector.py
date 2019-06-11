import paho.mqtt.client as mqtt
import datetime
import time
import math
import json

HOST = "iot.eclipse.org"
PORT = 1883
KEEPALIVE = 60
topic = "data/anomaly"
client_id = "/Inspector"

"""
Test Data Reading Functions
"""

def gas_sensor_test(timeSinceStart):
    """ Reads values > 0 every 5 seconds for an interval of 5 seconds"""
    return math.sin(2*math.pi*timeSinceStart/10)

def current_sensor_test(timeSinceStart):
    """ Reads values > 0 every 10 seconds, starting at 5.7 seconds"""
    return math.sin((math.pi/10)*(timeSinceStart - 5.7))

def magnetometer_sensor_test(timeSinceStart):
    """ Reads values > 0 every 5 seconds for an interval of 5 seconds"""
    return 10*((1/3)*math.sin(timeSinceStart/3) + 2*math.cos(timeSinceStart/3)*(math.sin(timeSinceStart/3)**2))

NUM_SENSORS = 3
#LOCATIONS = ["HAITI/SOUTH", "HAITI/NORTH", "USA/CA/SOUTH", "USA/CA/NORTH"]
LOCATIONS = ["USA/Quincy/1",]




def createJSON(topic, location, time_init, time_duration):
    return json.dumps({
            'topic': topic,
            'location': location,
            'time_init': time_init,
            'time_duration': time_duration
           })

def connect_to_broker(client_id, host, port, keepalive, on_connect, on_publish):
    # Params -> Client(client_id=””, clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)
    # We set clean_session False, so in case connection is lost, it'll reconnect with same ID
    client = mqtt.Client(client_id=client_id, clean_session=False)
    client.on_connect = on_connect
    client.on_publish = on_publish
    connection = client.connect(host, port, keepalive)
    return (client, connection)


def main():

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

    client, connection = connect_to_broker(client_id=client_id, host=HOST, port=PORT, keepalive=KEEPALIVE, on_connect=on_connect, on_publish=on_publish)

    client.loop_start()

    dataExaminers = {}

    for location in LOCATIONS:
        temp = []
        #temp.append(examineData('gas_sensor', location, gas_sensor_test))
        temp.append(examineData('current_sensor', location, current_sensor_test))
        #temp.append(examineData('magnetometer_sensor', location, magnetometer_sensor_test))

        dataExaminers[location] = temp



    packagesToSend = []

    while True:

        for location in LOCATIONS:
            for dataExaminer in dataExaminers[location]:
                packagesToSend.append(next(dataExaminer))

        for package in packagesToSend:
            if package != None:
                #print(package)
                client.publish(topic, package)

        for index in range(len(packagesToSend)):
            packagesToSend.pop()

    client.loop_stop()

    client.disconnect()



def examineData(topic, location, dataFunction):

    initialTime = time.time()
    dataThreshold = 0
    thresholdBroken = False
    detectedTime = 0
    detectedTimeStamp = ""
    endTime = 0


    while True:

        dt = time.time()-initialTime
        #value = 5 * math.sin(dt)
        #value = 7 * math.sin(math.cos(dt)*dt/4)
        #period = 10
        #value = math.sin(2*math.pi*dt/period)
        value = dataFunction(dt)
        #print(value)

        if value > dataThreshold:
            if thresholdBroken == False:
                thresholdBroken = True
                detectedTime = time.time()
                detectedTimeStamp = str(datetime.datetime.now().strftime("%m-%d-%y %H:%M:%S.%f"))
        else:
            if thresholdBroken == True:
                thresholdBroken = False
                endTime = time.time()
                inspectorPackageJSON = createJSON(topic, location, detectedTimeStamp, endTime-detectedTime)

                yield inspectorPackageJSON
        yield None





if __name__ == '__main__':
    main()
