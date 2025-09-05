from client.client import DistroClient
from client.clientUtils import WorkProcessor
from testConverters import SimpleJSONConverter

class SimpleSumProcessor(WorkProcessor):
    def __init__(self):
        super().__init__()
    
    def processWorkload(self, workload):
        sum = 0
        for item in workload:
            sum = sum + item
        return [sum]

def main():
    print("Hello from distcomp!")
    client = DistroClient(enableRandomDelay=True, lowerBound=2, upperBound=10)
    converter = SimpleJSONConverter()
    processor = SimpleSumProcessor()
    client.startClient("127.0.0.1",8080, processor=processor, workloadConverter=converter, workResultConverter=converter)


if __name__ == "__main__":
    main()