"""
def testGenerator():
    x = 5
    while x > 0:
        yield x
        x -= 1
    yield 9

a = testGenerator()

while True:
    print(next(a))
    input()


sensorDict = {}
locs = ['a', 'b', 'c']

for loc in locs:
    temp = []
    temp.append(1)
    temp.append(2)
    temp.append(3)

    sensorDict[loc] = temp

print (sensorDict)
"""
inspectorPackageDict = {
        'topic': 'gas_sensor',
        'location': 'USA/Quincy/1',
        'time_init': '10:31.123',
        'time_duration': 12.234
       }
print(inspectorPackageDict)




print(messageToSend)
