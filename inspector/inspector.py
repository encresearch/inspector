import paho.mqtt.client as mqtt
import datetime
import time
import math
import json

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

                inspectorPackageDict = {
                                        'topic': 'gas_sensor',
                                        'location': 'NA-EAST',
                                        'time_init': detectedTime-initialTime,
                                        'time_duration': endTime-detectedTime
                                        }
                yield inspectorPackageDict
                #initialTime = time.time()
        yield None



broker = "iot.eclipse.org"
port = 1883
keepalive = 60

today= str(datetime.datetime.now().strftime("%m-%d-%y %H:%M"))

inspectorPackageDict = {
                    'topic': 'gas_sensor',
                    'location': 'NA-EAST',
                    'time_init': today,
                    'time_duration': 12
                    }

inspectorPackageJSON = json.dumps(inspectorPackageDict)

topic = "data/anomaly"
client = mqtt.Client("notiferTestENC")
client.connect(host=broker, keepalive=keepalive, port=port)

client.loop_start()
dataExaminer = examineData()
while True:

    packageToSend = next(dataExaminer)
    if packageToSend != None:
        print(packageToSend)
        #client.publish(topic, inspectorPackageJSON)
