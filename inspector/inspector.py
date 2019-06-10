import paho.mqtt.client as mqtt
import datetime
import json

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
client.publish(topic, inspectorPackageJSON)
