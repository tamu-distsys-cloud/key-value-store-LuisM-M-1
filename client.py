import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.

        #unique client id is used for idendtifying who has sent the request
        self.client_id =nrand() # use the nrand that is in this file

        # then counter to labe each request made by this client
        # prof said to differentiate repeated messages
        self.request_id= 0

        # lock is also needed for ordering of requestid
        self.lock = threading.Lock()

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        # basic get method
        args = GetArgs(key)
        ## keep tryin until the get works, tkaes into acount unreilable networks
        while True:
            try: 
                reply = self.servers[0].call("KVServer.Get", args)
                if reply is not None:
                    return reply.value
            except Exception:
                continue


        return ""

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        # assigning a unique id
        with self.lock:
            self.request_id +=1

        args = PutAppendArgs(key, value)

        # try all servers until get a response
        while True:
            for x in range(0, len(self.servers)):
                try:
                    reply = self.servers[x].call("KVServer."+op, args)
                    # need to add something to pick either put or append
                    if reply is not None:
                        return reply.value if op == "Append" else ""
                    
                except Exception:
                    continue

            return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
