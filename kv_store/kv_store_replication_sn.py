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

# Concurrency: 1. use async event loop or 2. use multithreading
# Backup server can be multithreading or single actor, depending on if it is bottleneck
@ray.remote(max_restarts=-1, max_task_retries=-1)
class Server(object):
	def __init__(self, backup, index):
		tstart = time.time()
		self.backup = backup
		self.kvstore = ray.get(backup.restore_primary.remote())
		self.index = index
		tend = time.time()
		print("backup size: ", len(self.kvstore))
		print("recovery time: ", tend - tstart)

	def put(self, key, value):
		# if self.index == 0 and np.random.rand() < 0.00001: #len(self.kvstore) > 5000:
		# 	# print(ray.get(self.backup.check_restarted.remote()))
		# 	if not ray.get(self.backup.check_restarted.remote()):
		# 		sys.exit()
		# self.kvstore[key] = value
		if key in self.kvstore:
			self.kvstore[key] += " " + value
		else:
			self.kvstore[key] = value
		# await self.backup.put.remote(key, value) by making BackupServer an AsyncActor
		ray.get(self.backup.put.remote(key, value))
		# print("server size: ", len(self.kvstore))

	def get(self, key):
		if key in self.kvstore:
			return self.kvstore[key]
		return None

	def size(self):
		return len(self.kvstore)

@ray.remote
class BackupServer(object):
	def __init__(self):
		self.kvstore = {}
		self.restarted = False
		self.restart_count = 0

	def put(self, key, value):
		if key in self.kvstore:
			self.kvstore[key] += " " + value
		else:
			self.kvstore[key] = value
		# self.kvstore[key] = value

	def get(self, key):
		if key in self.kvstore:
			return self.kvstore[key]
		return None

	def check_restarted(self):
		return self.restarted

	def restore_primary(self):
		self.restart_count += 1
		if self.restart_count == 2:
			self.restarted = True
		return self.kvstore
	
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

	# server = Server.remote()
	backup_servers = [BackupServer.options(max_concurrency=1).remote() for i in range(args.num_servers)]
	servers = [Server.options(max_concurrency=10).remote(backup_servers[i], i) for i in range(args.num_servers)] # options(max_concurrency=10000).
	# client = Client.remote(servers)
	clients = [Client.remote(servers, args.num_requests) for i in range(args.num_clients)]

	tstart = time.time()
	# for _ in range(args.num_requests):
	# 	ray.get(client.run_op.remote())
	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]
	ray.get(refs)

	tend = time.time()
	print("time: ", tend - tstart)
	print("throughput: ", args.num_requests / (tend - tstart))

