from client.clientUtils import WorkByteConverter, WorkProcessor
import socket
import time
import random

BUFFER_SIZE = 1024*1024 # 1MB
class DistroClient:
    def __init__(self, enableRandomDelay = False, lowerBound = 0.0, upperBound = 10.0):
        self._enableDelay = enableRandomDelay
        self._lowerBound = lowerBound
        self._upperBound = upperBound
        pass

    def _clientUpdateListener(self):
        self._soc.settimeout(5)
        clientRunning = True
        while clientRunning:
            try:
                data = self._soc.recv(BUFFER_SIZE)
                print("Data received!")
                if data[0] == ord('1'):
                    totalExpectedLength = int.from_bytes(data[1:5])
                    data = data[5:]
                    print(f"Data: {data}")
                    print(f"Expected length: {totalExpectedLength}")
                    print(f"Data length: {len(data)}")
                    while totalExpectedLength > len(data):
                        data = data + self._soc.recv(BUFFER_SIZE)
                    print("Finished reading data")
                    result = self._processor.processWorkload(self._workloadConverter.fromBytes(data))
                    data = self._workResultConverter.toBytes(result)
                    data = bytes("1", encoding="utf-8") + len(data).to_bytes(4) + data 
                    if self._enableDelay:
                        delay = random.random() * (self._upperBound - self._lowerBound) + self._lowerBound
                        time.sleep(delay)
                    self._soc.sendall(data)
                    print("Sent answer!")
                else:
                    clientRunning = False
            except socket.timeout:
                pass

    def startClient(
            self, host:str, port:int, processor: WorkProcessor,
            workloadConverter: WorkByteConverter, workResultConverter:WorkByteConverter):
        self._processor = processor
        self._workloadConverter = workloadConverter
        self._workResultConverter = workResultConverter
        self._soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._soc.connect((host,port))
        print("Connection established!")
        self._clientUpdateListener()
        self._soc.close()