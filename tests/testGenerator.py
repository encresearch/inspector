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
"""

sensorDict = {}
locs = ['a', 'b', 'c']

for loc in locs:
    temp = []
    temp.append(1)
    temp.append(2)
    temp.append(3)

    sensorDict[loc] = temp

print (sensorDict)
