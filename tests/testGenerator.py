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
