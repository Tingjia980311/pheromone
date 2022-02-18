import sys
from client import PheromoneClient
from proto.common_pb2 import *
from proto.operation_pb2 import *
import time
import os

# client = PheromoneClient('a1f7912a3bf404bbcba15e1367345190-1317238364.us-east-1.elb.amazonaws.com', 'ad0dc9175284b4cdf80bf7c8ed5ffa8c-352615241.us-east-1.elb.amazonaws.com', '18.212.247.15', thread_id=0)
client = PheromoneClient('a76f932efee2e443dac595ea461892dd-833596863.us-east-1.elb.amazonaws.com', 'a328893e2f58f495182144eac3c658d1-1350364802.us-east-1.elb.amazonaws.com', '54.88.41.113', thread_id=0)

app_name = 'func-chain'
# client.put_kvs_object("aaa","bbb")
# res = client.get_kvs_object("k_anna_write8")

dependency = (['write_func'], ['read_func'], DIRECT)
client.register_app(app_name, ['write_func', 'read_func'], [dependency])

res = client.call_app(app_name, [('write_func', [100000000])], synchronous=True)

print(res)

