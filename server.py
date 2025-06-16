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
    def __init__(self, key, value):
        self.key = key
        self.value = value

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

        # Your definitions here.
        # could potentially use a dictionary for key and value pairs
        self.stored_values = {}


    def Get(self, args: GetArgs):
        reply = GetReply("")

        # Your code here.

        # grab value for the key if it exists
        # otherwise return an empty string
        with self.mu:
            value = self.stored_values.get(args.key, "")
            reply.value = value


        

        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply("")

        # Your code here.
        # set key to th eappropriate value
        with self.mu:
            self.stored_values[args.key] = args.value

        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        # get old value, if key is not inndictionary
        # concatenate new value
        with self.mu:
            old_value = self.stored_values.get(args.key, "")

            # self.stored_values[args.key] += args.value
            self.stored_values[args.key] = old_value + args.value


        return reply
