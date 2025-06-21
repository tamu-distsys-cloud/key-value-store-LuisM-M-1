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
        # new stuff ----------------
        self.client_id = client_id 
        self.request_id = request_id # used to differentiate operations

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
        # added var for error messaging
        self.err = err

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # dict used for key value pairs
        # prof recommended to use this instead of doing Paxos or 2 phase commit
        self.kv_store = {}
        
        self.processed_requests = {}


    # SHARDING and REPLICA LOGIC
    # following static paritioning logic that was established by Amazon Dynamo
    # https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

    def shard_id(self, key:str):
        # determine shard assignment based on key
        # prof said to hash key using int(key) % shards
        try:
            return int(key) %  self.cfg.nservers
        except ValueError: # if not numeric
            return 0 # default to shard 0
    
    def getReplicaGroup(self , shard: int) -> list[int]:
        # return list of servers for this particular shard
        # the first one is the primary, and the rest are simply backups
        replicas = []

        for i in range( 0, self.cfg.nreplicas ):
            serverIndex = ( shard+i) % self.cfg.nservers
            replicas.append(serverIndex)

        return replicas
    
    def is_replica(self, shard:int):
        # this is used to check if the server is a replica
        # given the shard

        # first get the list of server indeces
        replica_servers = self.getReplicaGroup(shard)

        # then find the index in relation to the other servers
        my_index = self.cfg.kvservers.index(self)

        # finally check if the server is a replica for the shard
        if my_index in replica_servers:
            return True
        else:
            return False

    def is_primary(self, shard: int):
        # primary replica is the first server in the overall replica group of the given shard
        # based on profs guidance, where index == shard is the primary
        primaryServerIndex = shard 

        myServerIndex = self.cfg.kvservers.index(self) 

        return myServerIndex == primaryServerIndex # returns true if is is the primary

    # forward definition
    def forward(self, op:str, args:Any) -> Any:
        # forward operation to primary replica
        # prof mentioned in class, that the primary (aka leader) gets to make updates
        primary = self.cfg.kvservers[self.shard_id(args.key)]

        fn = getattr(primary, op)
        return fn(args)
    
        
    # CLIENT REQUEST HANDLERS #----------------------------------------

    def Get(self, args: GetArgs):
        # reply = GetReply("")
        shard = self.shard_id(args.key)

        # reject request if server isn't a replica
        if not self.is_replica(shard):
            return GetReply(err="WRONG_SHARD")
        
        # forward to primary if its a backup
        if not self.is_primary(shard):
            return self.forward("Get", args)
        
        # can properly handle request as a primary
        with self.mu:
            value= self.kv_store.get(args.key, "")
            return GetReply(value=value)

    def Put(self, args: PutAppendArgs):
        # newer shard logic
        shard = self.shard_id( args.key )

        # reject request if not replica
        if not self.is_replica(shard):

            return PutAppendReply(err="WRONG_SHARD")

        #forward to the primary if not main replica
        if not self.is_primary(shard):
            return self.forward("Put", args)

        # if primary, process write requuest
        with self.mu:
            req_key = (args.client_id, args.request_id)

            # handle duplicate requests
            if req_key in self.processed_requests:
                return PutAppendReply( value=self.processed_requests[req_key] )
            
            
            self.kv_store[args.key] = args.value
            self.processed_requests[req_key] = ""  # empty string for Put

            return PutAppendReply()
        
    def Append(self, args: PutAppendArgs):
         # # added shard logic for passing 6th test case
        shard = self.shard_id(args.key)


        # again, reject if not a replica
        if not self.is_replica(shard):
            return PutAppendReply(err="WRONG_SHARD")

        # if not primary, forward it
        if not self.is_primary(shard):
            return self.forward("Append", args)

        # if primary then it can be processed
        with self.mu:
            req_key = (args.client_id, args.request_id)

            # handle duplicate requests similar to put
            if req_key in self.processed_requests:
                return PutAppendReply(value = self.processed_requests[req_key])
            
            old_value = self.kv_store.get(args.key, "")
            self.kv_store[args.key] = old_value + args.value

            # save the original value instead
            self.processed_requests[req_key] = old_value

            return PutAppendReply(old_value)

    
# REFERENCES AND AI ACKNOWLEDGEMENT

# server.py implements many ideas that were spearheaded by amazon dynamo
# things such as static sharding and primary based forwarding
# "Dynamo: Amazon’s Highly Available Key-value Store", SOSP 2007
# https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

# Chatgpt was used to help understand logic such as:
# - static partitioning 
# - correct behavior for servers under "simulated" network failures
# - and overall helping me understand how Amazon Dynamo implements forward requests to the primary replica
    


