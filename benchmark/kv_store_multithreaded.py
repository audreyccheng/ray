import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import time

parser = argparse.ArgumentParser()
parser.add_argument(
	"--num-clients", help="number of client actors used", default=1, type=int)
parser.add_argument(
	"--num-servers", help="number of server actors used", default=1, type=int)
parser.add_argument(
	"--num-requests", help="number of requests per client", default=10000, type=int)

@ray.remote
class Server(object):
	def __init__(self):
		self.kvstore = {}

	def put(self, key, value):
		self.kvstore[key] = value

	def get(self, key):
		if key in self.kvstore:
			return self.kvstore[key]
		return None

	def delete(self, key):
		return self.kvstore.pop('key', None)

@ray.remote
class Client(object):
	def __init__(self, servers):
		self.servers = servers

	def run_op(self):
		rand_val = np.random.rand()
		rand_server = randrange(len(self.servers))
		if rand_val < 0.5:
			self.servers[rand_server].put.remote(randrange(100), rand_val)
		else:
			self.servers[rand_server].get.remote(randrange(100))

if __name__ == "__main__":
	args = parser.parse_args()

	ray.init()

	# server = Server.remote()
	servers = [Server.remote() for _ in range(args.num_servers)]
	# client = Client.remote(servers)
	clients = [Client.remote(servers) for _ in range(args.num_clients)]

	tstart = time.time()
	# for _ in range(args.num_requests):
	# 	ray.get(client.run_op.remote())
	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests)]
	ray.get(refs)

	tend = time.time()
	print("time: ", tend - tstart)
	print("throughput: ", args.num_requests / (tend - tstart))

