import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import sys
import time
import threading

@ray.remote(max_restarts=1, max_task_retries=1)
class Server(object):
	def __init__(self):
		self.kvstore = {}
		# self.lock = threading.Lock()

	def put(self, key, value):
		self.kvstore[key] = value
		# self.lock.acquire()
		# try:
		# 	self.kvstore[key] = value
		# finally:
		# 	self.lock.release()

	def get(self, key):
		val = None
		if key in self.kvstore:
			val = self.kvstore[key]
		return val
		# self.lock.acquire()
		# try:
		# 	if key in self.kvstore:
		# 		val = self.kvstore[key]
		# finally:
		# 	self.lock.release()
		# return val

	# def delete(self, key):
	# 	return self.kvstore.pop('key', None)

@ray.remote(num_cpus=0)
class Client(object):
	def __init__(self, servers, num_requests):
		self.servers = servers
		self.num_requests = num_requests

	async def run_op(self):
		rand_val = np.random.rand()
		rand_server = randrange(len(self.servers))
		if rand_val < 0.5:
			return await self.servers[rand_server].put.remote(randrange(self.num_requests), rand_val)
		else:
			return await self.servers[rand_server].get.remote(randrange(self.num_requests))

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-clients", default=1, type=int)
	parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-clients-per-node", default=1, type=int)
	parser.add_argument("--num-servers-per-node", default=1, type=int)
	parser.add_argument("--num-requests", default=10000, type=int)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	servers = [Server.remote() for _ in range(args.num_servers)]
	clients = [Client.remote(servers, args.num_requests) for _ in range(args.num_clients)]

	tstart = time.time()
	# for _ in range(args.num_requests):
	# 	ray.get(client.run_op.remote())
	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]

	# ray.get(refs)
	print(ray.get(ray.get(refs)))
	# for i in range(10):
	# 	print(ray.get(refs[i:i*1000]))

	tend = time.time()
	print("time: ", tend - tstart)
	print("throughput: ", args.num_requests / (tend - tstart))

