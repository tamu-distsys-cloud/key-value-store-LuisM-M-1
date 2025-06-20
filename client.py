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

        #unique client id is used for idendtifying who has sent the request
        self.client_id =nrand() # use the nrand that is in this file

        # then counter to labe each request made by this client
        # prof said to differentiate repeated messages
        self.request_id= 0

        # lock is also needed for ordering of requestid
        self.lock = threading.Lock()

    def getReplicaServers(self, key:str) -> List[ClientEnd]:
        # return the servers for this key
        # follows dynamo style apartitioning
        nreplicas = getattr(self.cfg, 'nreplicas', 1)
        try:
            key_int= int(key) # convert key to integer
        except ValueError:
            # set to 0 if key isn't numeric
            key_int = 0

        # determine
        nshards = len(self.servers)
        shard = key_int % nshards

        # return replica group
        replica_group = []
        for i in range(0, nreplicas):
            serverIndex = (shard + i) % nshards
            replica_group.append(self.servers[serverIndex])

        return replica_group

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
        # more advanced get method:

        args = GetArgs(key)
        replica_group = self.getReplicaServers(key)
        ## keep tryin until the get works, tkaes into acount unreilable networks
        while True:
            for server in replica_group:
                # for every server inside replica group try to get a reply
                try:
                    reply = server.call("KVServer.Get", args)
                    if reply is not None and getattr(reply, "err", "") != "ERR_WRONG_GROUP":
                        return reply.value
                except Exception:
                    continue


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
            req_id = self.request_id
            self.request_id +=1

        args = PutAppendArgs(key, value, self.client_id, req_id)
        replica_group = self.getReplicaServers(key)

        # try all servers until get a response
        while True:
            for server in replica_group:
                # again try each server insid ethe replica group
                try:
                    reply = server.call("KVServer." + op, args)
                    if reply is not None and getattr(reply, "err", "") != "ERR_WRONG_GROUP":
                        return reply.value if op == "Append" else ""
                except Exception:
                    continue



    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
