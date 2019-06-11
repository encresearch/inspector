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
        print("Published")
        pass

    client, connection = connect_to_broker(client_id=client_id, host=HOST, port=PORT, keepalive=KEEPALIVE, on_connect=on_connect, on_publish=on_publish)

    client.loop_start()

    dataExaminer = examineData()

    while True:

        packageToSend = next(dataExaminer)
        if packageToSend != None:
            print(packageToSend)
            client.publish(topic, packageToSend)

    client.loop_stop()

    client.disconnect()



def examineData():

    initialTime = time.time()
    dataThreshold = 0
    thresholdBroken = False
    detectedTime = 0
    endTime = 0

    while True:

        dt = time.time()-initialTime
        #value = 5 * math.sin(dt)
        #value = 7 * math.sin(math.cos(dt)*dt/4)
        period = 10
        value = math.sin(2*math.pi*dt/period)
        #print(value)

        if value > dataThreshold:
            if thresholdBroken == False:
                thresholdBroken = True
                detectedTime = time.time()
        else:
            if thresholdBroken == True:
                thresholdBroken = False
                endTime = time.time()
                inspectorPackageJSON = createJSON('gas_sensor', 'How to do location?', detectedTime-initialTime, endTime-detectedTime)

                yield inspectorPackageJSON
        yield None





if __name__ == '__main__':
    main()
