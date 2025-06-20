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
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # might change this dict later for clarity
        self.stored_values = {}
        # keep track of the last result for each client request
        self.handled_requests = {}

    # def primary shard index
    # convert to an integer
    #   return 0

    # def get primary
    # 


    def Get(self, args: GetArgs):
        reply = GetReply("")

        # grab value for the key if it exists
        # otherwise return an empty string
        with self.mu: # using lock for concurrency
            value = self.stored_values.get(args.key, "")
            reply.value = value
        return reply

    def Put(self, args: PutAppendArgs):

        # return reply
        with self.mu:
            # identify reqeust using clinet id and request id
            key = (args.client_id, args.request_id)
            # if request was processed, return cached response
            if key in self.handled_requests:
                return PutAppendReply(self.handled_requests[key])

            # if ne request updat ethe key
            self.stored_values[args.key] = args.value

            # cache the reply to find duplicates
            self.handled_requests[key] = "" # empty string
        return PutAppendReply("")

    def Append(self, args: PutAppendArgs):

        with self.mu:
            key = (args.client_id, args.request_id)
            
            # if this is a repeated request, return old value (before the append)
            if key in self.handled_requests:
                return PutAppendReply(self.handled_requests[key])

            old_value = self.stored_values.get(args.key, "")

            # updating the value to be old+new
            
            self.stored_values[args.key] = old_value + args.value
            # 
            self.handled_requests[key] = old_value
        return PutAppendReply(old_value)


