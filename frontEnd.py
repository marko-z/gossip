import Pyro4
import time
import threading
import random

@Pyro4.expose
class frontEnd():
    def __init__(self):
        self.timestamp = [0,0,0]
        self.queryResponse = []
        self.frontEndName = "frontEnd"
        self.frontEndURI = ""
        self.clientURI = ""
        self.updateIDbuffer = 0
        self.updateID = 0

        frontEndDaemon = threading.Thread(target=self.networkStart)

        #exposing front end server
        print("Launching front end daemon...")
        frontEndDaemon.start()

        #fiding the client
        self.clientURI = self.findClient()

        #finding a functional replica
        self.replicaURI = self.findNextReplicaManager()

        print("Front end ready.")

    def findClient(self):
        print("Searching for client...")
        with Pyro4.locateNS() as ns:
            while True:
                for name, uri in ns.list(prefix="Client").items(): #there's probably a better way of solving this
                        print("Client found")
                        return uri
                else:
                    time.sleep(3)
                    print("Client not found. Retrying...")
        

    #replica manager connector
    def findNextReplicaManager(self):
        print("Searching for replica manager...")
        while True:
            with Pyro4.locateNS() as ns:
                    for name, uri in ns.list(prefix="replica.manager").items():
                        print("Replica manager found")
                        return uri
                    else:
                        time.sleep(3)
                        print("Replica manager not found. Retrying..")
    
    def timestampMergeWith(self,ts,into_ts):
        for i in range(3):
            if ts[i] > into_ts[i]:
                into_ts[i]=ts[i]

    #query forwarder
    def query(self,query):
        while True:
            try:
                with Pyro4.Proxy(self.replicaURI) as manager:
                    print("Received query from client: {} Forwarding to replica manager...".format(query))
                    manager.queryRequest(query, self.timestamp) 
                break
            except Pyro4.errors.ConnectionClosedError:
                print("Target replica manager disappeared. Hold on.")
                self.findNextReplicaManager()
                print("Resuming forwarding of the previous query request..")

    #update forwarder
    def update(self, update):
        while self.updateID == self.updateIDbuffer:
            self.updateID = random.randint(1,10000)
        
        print("Received update request from client: {} Forwarding to replica manager...".format(update))
        print("My current timestamp is",self.timestamp)

        while True:    
            try:
                with Pyro4.Proxy(self.replicaURI) as manager:
                    uniqueTS = manager.updateRequest(update, self.timestamp[:], self.updateID) 
                    self.timestampMergeWith(uniqueTS,self.timestamp)
                    print("Received confirmation from replica manager. Now my timestamp is",self.timestamp)
                break
            except Pyro4.errors.ConnectionClosedError:
                print("Target replica manager disappeared. Hold on.")
                self.findNextReplicaManager()
                print("Resuming forwarding of the previous update request..")
        self.updateIDbuffer = self.updateID

    #query response receiver
    @Pyro4.expose
    def queryReceive(self, queryResponse):
        with Pyro4.Proxy(self.clientURI) as client:
            print("Received query response from replica manager: {} Forwarding to client...".format(queryResponse))
            client.receiveQueryResponse(queryResponse)
            
    # def emitterCentral():
    #     while True:
    #         uri = findNextReplicaManager()
    #         if uri:
    #             while True:
    #                 try:
    #                     startEmitterLoop(uri)
    #                 except Pyro4.errors.communicationError:
    #                     print("Lost replica, manager, trying another one...")
    #                     break
    #                 #add a case for overloaded replica manager
    #         else:
    #             print("No replica managers found")

    def networkStart(self):
        with Pyro4.Daemon() as daemon:
            self.frontEndURI = daemon.register(self)
            with Pyro4.locateNS() as ns:
                ns.register(self.frontEndName,self.frontEndURI)
            print("\n Front end daemon ready.")
            daemon.requestLoop()

def main():
    frontEndInstance = frontEnd()
        
            
if __name__=="__main__":
    main()