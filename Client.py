import re
import time
import Pyro4
import threading
class Client:
    def __init__(self):
        self.queryResponseQueue = [] #list of list of tuples
        self.letters = ['A','B','C','D','E']
        self.queryResponseDictionary = {}
        self.clientName = "Client"
        self.clientURI = ""

        try:
            clientDaemon = threading.Thread(target=self.networkStart)
            clientDaemon.start()
            
            self.locateFrontEnd()
            time.sleep(2)

            print("Client ready. Type 'help' for commands.")
            self.inputController()

        except KeyboardInterrupt:
            with Pyro4.locateNS as ns:
                ns.remove(name="Client")

    def locateFrontEnd(self):
        while True:
            print('Locating front end server...')
            with Pyro4.locateNS() as ns:
                for name, uri in ns.list(prefix="frontEnd").items():
                    self.frontEndURI = uri
                    print('Front end server located...')
                    return
                else:
                    time.sleep(3)
                    print('Front end server not located, retrying...') 
                    
    def networkStart(self):
        print('Starting client daemon...')
        with Pyro4.Daemon() as daemon:
            self.clientURI = daemon.register(self)
            with Pyro4.locateNS() as ns:
                ns.register(self.clientName,self.clientURI)
            print("Client daemon ready...")
            daemon.requestLoop()
            
    def queryResponsePrinter(self):
        if self.queryResponseQueue:
            print(self.queryResponseQueue[0][0][0])
            self.queryResponseDictionary.clear()
            letter_index = 0

            if self.queryResponseQueue[0][0][0] == None:
                print("Server could not find any matches for the given query: "+self.queryResponseQueue[0][0][1])
            else:
                
                for movie, rating in self.queryResponseQueue[0]:
                    print(self.letters[letter_index]+" Title: " + movie + ", Rating: "+ str(rating))
                    self.queryResponseDictionary[self.letters[letter_index]]=movie
                    letter_index+=1

            self.queryResponseQueue.pop(0)

    @Pyro4.expose
    def receiveQueryResponse(self, queryResponse):
        self.queryResponseQueue.append(queryResponse)
        print("Query response received, type 'read' to read the response\n> ",end="")

    def inputController(self):
        while True:
            userInput = input("> ")
            userInput = re.sub(' +',' ',userInput) 
            userInput = userInput.split()
            if not userInput:
                continue
            
            if userInput[0] == 'help':
                print()
                self.printHelp()

            elif userInput[0] == 'read':
                if self.queryResponseQueue:
                    self.queryResponsePrinter()
                else:
                    print("\nNo query response ready")
                    continue

            elif userInput[0] == 'search':
                query = userInput[1]

            
                with Pyro4.Proxy(self.frontEndURI) as frontEnd:
                    frontEnd.query(query)
                    print("\nQuery sent: "+query)
                    continue
            

            elif userInput[0] == 'rate':

                if userInput[1] in self.letters:
                    if self.queryResponseDictionary:
                        if self.queryResponseDictionary[userInput[1]]:
                            userInput[1] = self.queryResponseDictionary[userInput[1]]
                        else:
                            print("\nProvided letter could not be paired with a movie title. Type 'help' for list of commands.")
                            continue
                with Pyro4.Proxy(self.frontEndURI) as frontEnd:
                    frontEnd.update((userInput[1], float(userInput[2])))
                    print("\nUpdate sent:\nMovie: "+ userInput[1] +", Rating: "+userInput[2])
                    continue
            else:
                print("Unrecognized command/query formatting. Type 'help' for list of commands.")

        
    def printHelp(self):
        print('''Avaiable commands:
search *search term* - perform a server search for movies matching the search term. 
Client informs the user once query comes back. Query result can then be accessed with 'read' command.

read - print the latest movie search result(can be used only once per result)

rate *search term OR query result letter* *rating (float number)* - update the movie rating of the FIRST match for the
search term or, if following an earlier query result and a letter is A-E supplied instead of search term
(corresponding to an appropriate query result), client autofills the appropriate movie name and sends that
instead, e.g. rate B 4.5

help - print this manual''')


def main():
    client = Client()
    print('Done.')

if __name__ == "__main__":
    main()