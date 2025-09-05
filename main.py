import server.server as s
import server.serverUtils as su
from testConverters import SimpleJSONConverter

class SimpleSupplier(su.WorkSupplier):
    def __init__(self):
        super().__init__()
        self.workSupply = []
        for i in range(1000):
            self.workSupply.append(i)
    
    def getWorkPackage(self):
        tmpList = []
        for i in range(100):
            try:
                tmpList.append(self.workSupply.pop(0))
            except IndexError:
                pass
        return tmpList

class SimpleAggregator(su.WorkAggregator):
    def __init__(self):
        super().__init__()
        self.finalResult = 0 
    def aggregateWork(self, workResults):
        sum = 0
        for result in workResults:
            sum = sum + result
        self.finalResult = sum
        

def main():
    print("Hello from distcomp!")
    aggr = SimpleAggregator()
    supplier = SimpleSupplier()
    server = s.DistroServer()
    converter = SimpleJSONConverter()
    server.startServer(8080,supplier,aggr, workloadConverter=converter, workResultConverter=converter, selfClosing=True)
    print(f"Result: {aggr.finalResult}")


if __name__ == "__main__":
    main()
