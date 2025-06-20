import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_id, request_id):
        self.key = key
        self.value = value
        # new stuff
        self.client_id = client_id
        self.request_id = request_id

class PutAppendReply:
    # Add definitions here if needed 
    def __init__(self, value="", err =""):
        self.value = value
        self.err = err # used for error notificatoin

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value="", err=""):
        self.value = value
        self.err = err

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # might change this dict later for clarity
        self.stored_values = {}
        # keep track of the last result for each client request
        self.handled_requests = {}


    # SHARDING and REPLICA LOGIC
    # following static paritioning logic that was established by Amazon Dynamo
    # https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

    def shard_id(self, key:str) ->int :
        # determine shard assignment based on key
        # key -> int(key) % n shards
        try:
            return int(key) % self.cfg.nservers
        except ValueError:
            return 0
    
    def replica_group(self , shard: int) -> list[int]:
        # return list of rservers for this shard
        # the first one is the primary, and the rest are simply backups
        replicas = []
        for i in range(0, self.cfg.nreplicas):
            serverIndex = (shard+i) % self.cfg.nservers
            replicas.append(serverIndex)
        return replicas
    
    def is_replica(self, shard:int) -> bool:
        # this is used to check if the server is a replica
        # given the shard

        # first get the list of server indeces
        replica_servers = self.replica_group(shard)

        # then find the index in relation to the other servers
        my_index = self.cfg.kvservers.index(self)

        # finally check if the server is a replica for the shard
        return my_index in replica_servers

    def is_primary(self, shard: int) -> bool:
        # primary replica is the first server in the overall replica group
        primary_index = shard # shard num equlas the primary's index

        my_index = self.cfg.kvservers.index(self) # get index of server

        return my_index == primary_index # compare indexes to check if it is the primary

    # forward
    def forward(self, op:str, args:Any) -> Any:
        # forwad operation to primary replic
        primary = self.cfg.kvservers[self.shard_id(args.key)]
        fn = getattr(primary, op)
        return fn(args)
    

        
    # CLIENT REQUEST HANDLERS #----------------------------------------

        


    def Get(self, args: GetArgs):
        # reply = GetReply("")
        shard = self.shard_id(args.key)

        # reject equest if server isn't a replica
        if not self.is_replica(shard):
            return GetReply(err="ERR_WRONG_GROUP")
        
        # if replica but not primary
        # forward to leader
        if not self.is_primary(shard):
            return self.forward("Get", args)
        
        # now as a primary
        # can properly handle request
        with self.mu:
            value= self.stored_values.get(args.key, "")
            return GetReply(value=value)


    def Put(self, args: PutAppendArgs):
        # new shard logic

        shard = self.shard_id(args.key)

        # reject request if not replica
        if not self.is_replica(shard):
            return PutAppendReply(err="ERR_WRONG_GROUP")

        # if not the primary
        #forward to the primary
        if not self.is_primary(shard):
            return self.forward("Put", args)

        # once primary, able to process request
        with self.mu:
            key = (args.client_id, args.request_id)
            if key in self.handled_requests:
                return PutAppendReply(value=self.handled_requests[key])
            self.stored_values[args.key] = args.value
            self.handled_requests[key] = ""  # empty string for Put
            return PutAppendReply()
        


    def Append(self, args: PutAppendArgs):
         # # added shard logic for passing 6th test case
        shard = self.shard_id(args.key)


        # again, reject if not a replica
        if not self.is_replica(shard):
            return PutAppendReply(err="wrong shard")

        # if not primary, forward it
        if not self.is_primary(shard):
            return self.forward("Append", args)

        # if primary == true
        # process it
        with self.mu:
            key = (args.client_id, args.request_id)
            if key in self.handled_requests:
                return PutAppendReply(value=self.handled_requests[key])
            old_value = self.stored_values.get(args.key, "")
            self.stored_values[args.key] = old_value + args.value
            self.handled_requests[key] = old_value
            return PutAppendReply(old_value)

    
# REFERENCES AND AI ACKNOELEDGEMENT

# server.py implements many ideas that were spearheaded by amazon dynamo
# things such as static sharding and primary based forwarding
# "Dynamo: Amazon’s Highly Available Key-value Store", SOSP 2007
# https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

# Chatgpt was used to help understand logic such as:
# - how shard ownership works. understand server indexing
# - static partitioning 
# - correct behavior for servers under "simulated" network failures "simulated"
    


