
Make sure following python packages are installed: Pyro4, time, random, threading, csv, re, copy, sys. 
With Pyro4 installed, launch the name server first (important!) by typing Pyro4-ns in separate console window. With nameserver open, order of launching the different parts shouldn't matter. Replica manager will compain if it doesn't find another manager, so launch at least 2. No time pressure for launching, the different parts of the program will just keep complaining until everyone can detect everyone. Once all parts are launched (in separate console windows/tabs), use the client to write commands. Write help in the client for a complete tutorial. A typical sequence of commands would be 'search ab' followed by 'read', followed by 'rate A 4.5' or 'rate sabrina 3.0'. Servers will print various messages to the screen, reflecting their internal states.

Design:
Each replica manager consists of 3 threads, one for monitoring and processing its log and query queue based on internal state (value timestamp); one for serving incoming calls like replica whispers and client requests forwarded by the front end server; and one for emitting whisper messages and responses to client requests. Query requests are handled through regex search of the movie list.

Front end server forwards client requests and replica manager responses. Adds its timestamp onto requests before passing them to replica managers, and updates it whenever update response is received. Should in theory detect and handle lost connections to replica managers (the exception falls through the handler for an unknown reason and goes to the client)

Client gives a text interface to generate requests (search ... corresponds to a query and rate ... ... corresponds to an update)