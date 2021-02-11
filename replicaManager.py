import Pyro4
import time
import random
import threading
import csv
import re
import copy
import sys

@Pyro4.expose
class replicaManager():

    def __init__(self):
        self.queryQueue = []
        self.replicaTimestamp = [0,0,0]
        self.valueTimestamp = [0,0,0]
        self.log = []
        self.executedOperations = []
        self.manager_number = -1
        self.queryResultQueue = []
        self.managerName = "xxx"
        self.managerURI = ""
        self.movieDictionary = {}

        try:
            monitor = threading.Thread(target=self.queueProcessor)
            daemon = threading.Thread(target=self.replicaDaemon)

            daemon.start()
            monitor.start()

            self.moviePopulator()
            self.replicaEmitter()
        except KeyboardInterrupt:
            with Pyro4.locateNS() as ns:
                ns.remove(name=self.managerName)
                sys.exit()

    def moviePopulator(self):
        print("Processing movie file...")
        with open('movies.csv') as csv_file:
            csv_reader = csv.reader(csv_file,delimiter=",")
            for row in csv_reader:
                self.movieDictionary[row[1]] = 3.0 
        print("Movie file processed")

    #compare ts to replica value timestamp
    def timestampLessOrEqualTo(self, ts1, ts2):
        for i in range(3):
            if ts1[i] > ts2[i]:
                return False
        return True

    def timestampMergeWith(self,ts,into_ts):
        for i in range(3):
            if ts[i] > into_ts[i]:
                into_ts[i]=ts[i]

    
    def appendInOrder(self,newLogEntry,targetLog):
        i=0
        while i < len(targetLog):
            if not self.timestampLessOrEqualTo(targetLog[i][1],newLogEntry[1]):
                break
            i += 1
        targetLog.insert(i,newLogEntry)

    #query receiver
    @Pyro4.expose
    @Pyro4.oneway
    def queryRequest(self,query,FEtimestamp): #separate thread for this?, no-response type
        self.queryQueue.append((query, FEtimestamp))
        print("Query received. Current query queue:",self.queryQueue)

    #update receiver
    @Pyro4.expose
    def updateRequest(self, updateOp, FEtimestamp, updateID): #handle incoming update request from front end
        
        
        self.replicaTimestamp[self.manager_number] += 1 #this 'update' timestamp
        
        uniqueTS = FEtimestamp[:]

        uniqueTS[self.manager_number] = self.replicaTimestamp[self.manager_number]

        logEntry = [self.manager_number, uniqueTS, updateOp, FEtimestamp, updateID]
        self.appendInOrder(logEntry,self.log)
        print("Update received and appended to update queue. Current update queue:",self.log)
        print("Sending unique log entry timestamp back to front end.")
        return uniqueTS 
    
    #whisper call from other replica manager
    @Pyro4.expose
    @Pyro4.oneway
    def whisperReceive(self, incomingReplicaLog, incomingReplicaTimestamp): #the listening process must be outside the class
        print("Received whisper message from another replica manager, I will now update my log and my replica timestamp.")
        for logEntry in incomingReplicaLog:
            if not self.timestampLessOrEqualTo(logEntry[1],self.replicaTimestamp): #I hope I'm comparing the right timestamps
                self.appendInOrder(logEntry,self.log) #append but in a special manneer

        self.timestampMergeWith(incomingReplicaTimestamp,self.replicaTimestamp)
        print("My current replica timestamp is",self.replicaTimestamp,"and my current value timestamp is",self.valueTimestamp)

    
    #monitor and processor of update and query queues
    def queueProcessor(self): #separate thread, handle updates and queries
        i=0
        while True:
            if self.log:
                if i % 8 == 0:
                    print("The replica manager log is non-empty.")
                if self.log[0][4] not in self.executedOperations:
                    if self.timestampLessOrEqualTo(self.log[0][3],self.valueTimestamp):
                        print("Stable update found in the replica log:",self.log[0][2],"Processing...")
                        self.movieDictionary[self.log[0][2][0]] = self.log[0][2][1] 
                        
                        self.timestampMergeWith(self.log[0][1],self.valueTimestamp)
                        
                        self.executedOperations.append(self.log[0][4])
                        print("Done.")
            if self.queryQueue:
                if self.timestampLessOrEqualTo(self.queryQueue[0][1],self.valueTimestamp):  #valueTimestamp should grow with updates
                    print("Processing the query...")
                    query = self.queryQueue[0][0]
                    queryRE = re.compile(".{{0,30}}{}.{{0,30}}".format(query), re.IGNORECASE)
                    queryResult = []
                    found=False
                    number=0
                    for key in self.movieDictionary:
                        if number == 5:
                            break
                        if re.match(queryRE,key):
                            queryResult.append((key,self.movieDictionary[key]))
                            found=True
                            number+=1

                    if found==False:
                        queryResult.append((None, query))
                    self.queryResultQueue.append(queryResult)
                    self.queryQueue.pop(0)

            # whisper processing?
            time.sleep(3)
            i+=1

    #replica manager finder
    def replicaPopulator(self):
        found = False
        print("Searching for other replica managers...")
        managerURIs=[]  
        with Pyro4.locateNS() as ns:
            while found == False:
                for name, uri in ns.list(prefix="replica.manager").items():
                    if name != self.managerName:
                        managerURIs.append(uri)
                        print("Manager found")
                        found=True
                if found == False:
                    print("No other replica managers found. Retrying...")
                    time.sleep(3)
        return managerURIs

    #front end finder
    def frontEndLocator(self):
        found = False
        print("Searching for front end...")
        with Pyro4.locateNS() as ns:
            while found==False:
                for name, uri in ns.list(prefix="frontEnd").items(): #there's probably a better way of solving this
                        frontEndURI = uri
                        print("Front end found.")
                        found=True
                        break
                else:
                    print("Front end not found. Retrying...")
                    time.sleep(3)
            
        return frontEndURI

    #pyro object call receiver
    def replicaDaemon(self): 
        print("Starting replica daemon...")
        with Pyro4.Daemon() as daemon:
            self.managerURI = daemon.register(self)

            with Pyro4.locateNS() as ns: 
                
                #purely for naming purposes
                print("Determining public replica name...")
                manager_numbers = []
                for name in ns.list(prefix="replica.manager").keys():
                    manager_numbers.append(name[-1])

                for i in range(3):
                    if str(i) not in manager_numbers:
                        self.managerName = "replica.manager" + str(i)
                        ns.register(self.managerName,self.managerURI)
                        self.manager_number = i
                        print("Replica manager name:", self.managerName) 
                        break

            daemon.requestLoop()
    
    #whisper emitter and query response interface
    def replicaEmitter(self):
           
            #populate managers
            managerURIs = self.replicaPopulator()
            frontEndURI = self.frontEndLocator()

            print("Replica manager ready.")
            
            i=0
            while True: 
                try:
                    #send out whispers to other managers
                    if i % 6 == 0:
                        for uri in managerURIs:
                            with Pyro4.Proxy(uri) as manager:
                                manager.whisperReceive(self.log,self.replicaTimestamp)
                except Pyro4.errors.CommunicationError:
                    print("Message whisper recipient disappeared. Repopulating replica managers...")
                    self.replicaPopulator()

                #query response to the front end

                if self.queryResultQueue:
                    with Pyro4.Proxy(frontEndURI) as frontEnd: #frontEnd messages
                        frontEnd.queryReceive(self.queryResultQueue[0])
                        self.queryResultQueue.pop(0)
                
                time.sleep(3)
                i+=1

def main():
    localManager = replicaManager()
    print("Done.")
        
            
if __name__=="__main__":
    main()
