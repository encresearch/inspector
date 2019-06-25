from influxDB_connect_test import Database
import time
import datetime
import math

def connectToDatabase():
    db = None
    USER = "postgres"
    PASSWORD = "password"
    HOST = "127.0.0.1"
    PORT = "5432"
    DATABASE = "ENC_Earthquake_Sensor_Data_Test"

    while True:
    	db = Database(USER, PASSWORD, HOST, PORT, DATABASE)

    	if db.connected == False:
    		print("Unable to connect: Trying again in 1s")
    		time.sleep(1)
    	else:
    		break
    return db

db = connectToDatabase()

def sensorFunction1(t):
    return 5 * math.sin(2 * math.pi * t / 1000)

def sensorFunction2(t):
    return 5 * math.sin(2 * math.pi / 500 * (t - 100))

def sensorFunction3(t):
    return 5 * math.sin(2 * math.pi / 5000 * (t - 1000))

def sensor_data_generator(_frequency, sensorFunction):
    initialTime = time.time()

    lastDataReadingTime = time.time()
    currentTime = 0
    dt = 0

    period = 1/_frequency

    lastDataReadingTime -= period

    while True:
        currentTime = time.time()
        dt = currentTime - lastDataReadingTime

        if dt >= period:
            t = currentTime-initialTime
            yield (sensorFunction1(t), sensorFunction2(t), sensorFunction3(t))
            lastDataReadingTime = time.time()
        else:
            yield None


testGenerator = sensor_data_generator(10, sensorFunction1)

dataEntered = 0
while True:

    data = next(testGenerator)


    if data != None:
        now = str(datetime.datetime.now().strftime("%m-%d-%y %H:%M:%S.%f"))
        db.addData(time.time(), now, data[0], data[1], data[2])


        if dataEntered % 100 == 0:
            print(dataEntered)
            print(data)
        dataEntered += 1
        pass







#now = str(datetime.datetime.now().strftime("%m-%d-%y %H:%M:%S.%f"))




#db.addData(time.time(), now,5.4, 8.3, 3.4)
