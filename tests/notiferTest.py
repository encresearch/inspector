import paho.mqtt.client as mqtt
import datetime
import time
import json

#Testing Data
today= str(datetime.datetime.now().strftime("%m-%d-%y %H:%M"))

inspectorPackageDict = {
                    'topic': 'gas_sensor',
                    'location': 'NA-EAST',
                    'time_init': today,
                    'time_duration': 12
                    }
inspectorPackageJSON = json.dumps(inspectorPackageDict)

#Setup in env variable?
broker = "iot.eclipse.org"
port = 1883
keepalive = 60

def on_message(client, userdata, msg):
    topic = msg.topic
    m_decode = str(msg.payload.decode("utf-8", "ignore"))
    m_in = json.loads(m_decode)
    print("\n",m_in,"\n")

def on_log(client, userdata, level, buf):
    import time
    print(str(time.time()) + " log: " + buf)

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print(f"Connected OK: {client}")
    else:
        print(f"Bad Connection Returned (Code: {rc})")
def on_disconnect(client, userdata, rc):
    print("Client Disconnected")

topic = "data/anomaly"
client = mqtt.Client("notiferTestENC")
client.on_connect = on_connect
client.on_log = on_log
client.on_message = on_message
client.on_disconnect = on_disconnect
client.connect(host=broker, keepalive=keepalive, port=port)
time.sleep(.5)




client.loop_start()
client.subscribe(topic, qos=1)
time.sleep(.5)
client.publish(topic, inspectorPackageJSON)

testIndex = 0


while True:
    time.sleep(1)
    if testIndex > 15:
        break
    testIndex+= 1
client.loop_stop()
client.disconnect()
