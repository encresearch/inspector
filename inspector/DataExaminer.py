import datetime
import time
import json
#time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1347517370))

class DataExaminer():

    dataExaminers = []
    dataExaminersAccess = {}

    def __init__(self, sensorName, location,ThresholdValue, mode=0):
        self.location = location
        self.sensorName = sensorName

        #print("Measurements thing init")
        self.measurementName = location + "_" + sensorName
        DataExaminer.dataExaminers.append(self.measurementName)
        DataExaminer.dataExaminersAccess[self.measurementName] = self
        #print("Measureend")

        self.thresholdValue = 0
        self.lowerThreshold = 0
        self.higherThreshold = 0

        self.initialTime = time.time()
        self.anomalyDetectedTime = None
        self.anomalyEndTime = None
        self.thresholdBroken = False
        self.influxTimeStamp_start = None
        self.influxTimeStamp_end = None

        self.anomalyFunction = lambda a: a+10

        #print("Modes")
        if mode == 0:
            #print("beginModes")
            self.thresholdValue = ThresholdValue[0]
            #print("MidModes")
            self.anomalyFunction = self.greaterThan
            #print("EndModes")
        elif mode == 1:
            self.thresholdValue = ThresholdValue[0]
            self.anomalyFunction = self.lessThan
        elif mode == 2:
            self.thresholdValue = ThresholdValue[0]
            self.anomalyFunction = self.equal
        elif mode == 3:
            self.lowerThreshold = ThresholdValue[0]
            self.higherThreshold = ThresholdValue[1]
            self.anomalyFunction = self.inRange
        elif mode == 4:
            self.lowerThreshold = ThresholdValue[0]
            self.higherThreshold = ThresholdValue[1]
            self.anomalyFunction = self.outOfRange
        else:
            if len(thresholdValue) == 1:
                self.thresholdValue = ThresholdValue[0]
                self.anomalyFunction = self.greaterThan
            elif len(thresholdValue) == 2:
                self.lowerThreshold = ThresholdValue[0]
                self.higherThreshold = ThresholdValue[1]
                self.anomalyFunction = self.outOfRange
            else:
                self.thresholdValue = 0
                self.anomalyFunction = self.greaterThan


    def greaterThan(self, value):
        return value > self.thresholdValue
    def lessThan(self, value):
        return value < self.thresholdValue
    def equal(self, value):
        return value == self.thresholdValue
    def inRange(self, value):
        return value > self.lowerThreshold and value < self.higherThreshold
    def outOfRange(self, value):
        return value < self.lowerThreshold or value > self.higherThreshold

    def examineData(self, dataArray):
        anomalies = []
        for data in dataArray:
            timestamp, value = data

            if self.anomalyFunction(value):
                if self.thresholdBroken == False:
                    self.thresholdBroken = True
                    self.influxTimeStamp_start = timestamp
                    #2019-07-18T11:33:02.155326976Z
                    #self.anomalyDetectedTime = DateTime.strftime(string,"%Y-%m-%DT%H:%M:$S.")
                    self.anomalyDetectedTime = time.time()
                    
                    anomalies.append(self.createJSON(topic=[self.location, self.sensorName, self.measurementName], anomaly_status="Started", location=self.location, time_init=self.influxTimeStamp_start, time_duration="N/A"))

            else:
                if self.thresholdBroken == True:
                    self.thresholdBroken = False
                    self.influxTimeStamp_end = timestamp
                    self.anomalyEndTime = time.time()
                    anomalies.append(self.createJSON(topic=[self.location, self.sensorName, self.measurementName], anomaly_status="Ended", location=self.location, time_init=self.influxTimeStamp_start, time_duration=self.anomalyEndTime-self.anomalyDetectedTime))


        return anomalies


    def createJSON(self, topic, anomaly_status, location, time_init, time_duration):
        ''' Helper function to quickly create a JSON String
        with the following format'''
        return json.dumps({
                'topic': topic,
                'anomaly_status': anomaly_status,
                'location': location,
                'time_init': time_init,
                'time_duration': time_duration
               })
