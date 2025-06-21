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

        # unique client id is generated
        self.client_id =nrand() # use the nrand that is in this file

        # then counter to labe each request made by this client
        # prof said to differentiate repeated messages
        self.request_id= 0

        # need the lock in case of concurrent client threads
        self.lock = threading.Lock()


    # added Definiton for Task : Static Sharding and Replication ---------------------
    def getReplicaServers(self, key:str) -> List[ClientEnd]:
        # prof mentioned we should use static partitioning
        # this implementation follows amazon dynamo
        # static paritioning: int(key) % n-shards
        nreplicas = getattr(self.cfg,'nreplicas', 1)
        try:
            key_int= int(key) # convert key to integer
        except ValueError:
            # set to 0 if key isn't numeric
            key_int = 0

        # determine
        nshards = len(self.servers)
        shard = key_int%nshards

        # return replica group
        replicaGroup = []
        for i in range(0, nreplicas):

            serverIndex = (shard +i) % nshards
            replicaGroup.append(self.servers[ serverIndex ])

        return replicaGroup


    # BASE CODE ----------------------------------------------------------------------
    
    def get(self, key: str) -> str:
        # Fetch the current value for a key.
        # Returns "" if the key does not exist.
        # Keeps trying forever in the face of all other errors.
        

        args = GetArgs(key)
        replicaGroup = self.getReplicaServers(key)
        ## This retry loop keeps trying until a server works 
        # essentially handles unreliable networks
        while True:
            for server in replicaGroup:
                # for every server inside replica group try to get a reply
                try:
                    reply = server.call("KVServer.Get", args)

                    # only accept valid responses
                    if reply is None:
                        continue

                    if getattr(reply, "err", "") == "WRONG_SHARD":
                        continue

                    # if no error then
                    # return the value
                    return reply.value
                except Exception:
                    continue


    # Shared by Put and Append.
    def put_append(self, key: str, value: str, op: str) -> str:
        # assigning a unique id
        with self.lock:
            req_id = self.request_id
            self.request_id +=1

        args = PutAppendArgs(key, value, self.client_id, req_id)
        replicaGroup = self.getReplicaServers(key)

        # try all servers until get a response
        while True:
            for server in replicaGroup:
                # again try each server insid ethe replica group
                try:
                    # similar logic to the modified get
                    reply = server.call("KVServer." + op, args)
                    # skip if reply is missing or 
                    # if server is not the right shard
                    if reply is None:
                        continue

                    if getattr(reply, "err","") == "WRONG_SHARD":
                        continue

                    # return actual val
                    if op == "Append":

                        return reply.value
                    else:
                        return ""
                except Exception:
                    continue



    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
