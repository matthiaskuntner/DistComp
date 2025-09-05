from .serverUtils import WorkAggregator, WorkSupplier, WorkByteConverter
import socket
import threading

import time
BUFFER_SIZE = 1024*1024 # 1MB



class DistroServer:
    def __init__(self):
        self._clientListLock = threading.Lock()
        self._resultListLock = threading.Lock()
        self._clientConnections = []
        self._workResults = []

    def _listenForConnections(self, soc: socket.socket):
        soc.settimeout(3)
        while self.serverRunning:
            try:
                #print(f"Clients side: {self._clientConnections}")
                conn, addr = soc.accept()
                print("Client connected!")
                lock = threading.Lock()
                with self._clientListLock:
                    self._clientConnections.append({
                        "client":ClientConnection(
                            connection=conn, 
                            workResultCollector=self._workResults,
                            resultListLock=self._resultListLock,
                            clientLock=lock,
                            workloadConverter=self._workloadConverter, 
                            workResultConverter=self._workResultConverter
                            ),
                        "clientLock": lock
                        })
            except socket.timeout:
                pass

    def startServer(
            self, port, supplier: WorkSupplier, 
            aggregator: WorkAggregator, workloadConverter: WorkByteConverter,
            workResultConverter: WorkByteConverter ,selfClosing:bool = True):
        self.serverRunning = True
        self._workloadConverter = workloadConverter
        self._workResultConverter = workResultConverter
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        soc.bind(("0.0.0.0",port))
        soc.listen()
        serverThread = threading.Thread(target=self._listenForConnections, args=[soc])
        serverThread.start()
        while self.serverRunning:
            workPackage = supplier.getWorkPackage()
            print(workPackage)
            if len(workPackage) == 0:
                print("Work done! No more work supplied!")
                if selfClosing:
                    self.serverRunning = False
                    print("Wait for all clients to finish work...")
                    self._awaitAllResponses()
                    print("Received all client updates!")
                    print(f"Aggregating work results: {self._workResults}")
                    with self._resultListLock:
                        aggregator.aggregateWork(self._workResults)
                    print("Finished aggregating results!")
                    print("Terminating clients...")
                    self._terminateClients()
                    print("Finished terminating clients!")
                    print("Ending Server thread...")
                    serverThread.join()
                    print("Server successfully shut down!")
            else:
                workPackageDistributed = False
                while not workPackageDistributed:
                    with self._clientListLock:
                        #print(f"Clients main: {str(self._clientConnections)}")
                        #print("From main thread: "+ str(id(self._clientConnections)))
                        for clientDict in list(self._clientConnections):
                            client = clientDict["client"]

                            with clientDict["clientLock"]:
                                #print(f"Client available: {client.isAvailable}")
                                isAvailable = client.isAvailable

                            if not workPackageDistributed and isAvailable:
                                client.sendWork(workPackage)
                                workPackageDistributed = True
                    time.sleep(2)
        soc.close()
    
    def _awaitAllResponses(self):
        allClientsFinished = False
        while not allClientsFinished:
            allClientsFinished = True
            with self._clientListLock:
                for clientDict in self._clientConnections:
                    client = clientDict["client"]
                    with clientDict["clientLock"]:
                        if not client.isAvailable:
                            allClientsFinished = False

    def _terminateClients(self):
        for clientDict in self._clientConnections:
            client = clientDict["client"]
            client.terminateConnection()
            #self._clientConnections.remove(clientDict)


class ClientConnection:
    def __init__(self, connection:socket.socket, workResultCollector:list, resultListLock:threading.Lock, clientLock: threading.Lock, workloadConverter:WorkByteConverter, workResultConverter: WorkByteConverter):
        self._connection = connection
        self.isAvailable = True
        self._workResultList = workResultCollector
        self._workloadConverter = workloadConverter
        self._workResultConverter = workResultConverter
        self._resultListLock = resultListLock
        self._clientLock = clientLock
        self._listenerCancellationRequested = False
        self._workerThread = threading.Thread(target=self._listenForClientUpdate)
        self._workerThread.start()

    def sendWork(self, workload: list):
        data = self._workloadConverter.toBytes(workload)
        #Encode the data with 1 at the start, to signal work data, then the length of the data afterwards
        data = bytes("1", encoding="utf-8") + len(data).to_bytes(4) + data 
        with self._clientLock:
            self._connection.sendall(data)
            self.isAvailable = False

    def terminateConnection(self):
        data = bytes("0", encoding="utf-8")
        with self._clientLock:
            self._connection.sendall(data)
            self.isAvailable = False
            #with self._listenerCancellationRequestedLock:
            self._listenerCancellationRequested = True
            self._workerThread.join()
            self._connection.close()

    def _listenForClientUpdate(self):
        #Read operation stops every 5 seconds to check if thread was requested to cancel
        self._connection.settimeout(5)
        #cancelationRequested = self._listenerCancellationRequested
        while not self._listenerCancellationRequested:
            try:
                data = self._connection.recv(BUFFER_SIZE)
                #First byte is '1' if the data is work results. 
                # 0 if administrative message
                print("Client sent data!")
                if len(data) == 0:
                    #Handle client disconnect
                    break
                if data[0] == ord('1'):
                    totalExpectedLength = int.from_bytes(data[1:5])
                    data = data[5:]
                    while totalExpectedLength > len(data):
                        data = data + self._connection.recv(BUFFER_SIZE)
                    print(f"Result: {self._workResultConverter.fromBytes(data)}")
                    
                    with self._resultListLock:
                        for item in self._workResultConverter.fromBytes(data):
                            self._workResultList.append(item)
                    print(f"Current full result list: {self._workResultList}")
                    with self._clientLock:
                        self.isAvailable = True
            except socket.timeout:
                pass
            #cancelationRequested = self._listenerCancellationRequested

            
