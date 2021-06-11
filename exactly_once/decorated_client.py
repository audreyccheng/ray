import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import sys
import time

def exactly_once_method(filename, server):
	def exactly_once_fn(fn):
		directory = os.path.dirname(os.path.realpath(__file__))
		path = os.path.join(directory, filename)
		server_path = os.path.join(directory, "private_" + filename)

		def wrapped_fn(self, *args):
			if request_id in wrapped_fn.requests:
				return wrapped_fn.requests[request_id]

			wrapped_fn.request_count += 1

			# get result from server
			result = fn(self, *args)
			wrapped_fn.requests[request_id] = result

			# save checkpoint
			f = open(path, "a")
			f.write(str(request_id) + " " + str(result) + "\n")
			f.close()

			fp = open(server_path, "a")
			fp.write(str(request_id) + " " + str(ray.get(server.get_state.remote())) + "\n")
			fp.close()
			
			return result

		wrapped_fn.request_count = 0		
		wrapped_fn.requests = {}
		return wrapped_fn
	return exactly_once_fn

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Server(object):
	def __init__(self, exactly_once):
		# self.requests = {} # k: request_id, v: (value, final value)
		self.value = 0
		# self.directory = os.path.dirname(os.path.realpath(__file__))
		# self.path = os.path.join(self.directory, "checkpoint_server.txt")
		self.exactly_once = exactly_once

		# if self.exactly_once:
		# 	try:
		# 		f = open(self.path, 'r')
		# 		for line in f:
		# 			vals = line.rstrip().split(" ")
		# 			assert(len(vals) == 3)
		# 			self.requests[vals[0]] = (vals[1], vals[2])
		# 			self.value += float(vals[2])
		# 		f.close()
		# 	except FileNotFoundError:
		# 		print('Checkpoint does not exist')

	
	def undecorated_add(self, value):
		self.value += float(value)
		return self.value

	# return state from actor that needs to be saved
	def get_state(self):
		return self.value

	def restore_state(self, value):
		self.value = value

@ray.remote(max_restarts=1, max_task_retries=1)
class Client(object):
	def __init__(self, client_id, server):
		self.client_id = client_id
		self.server = server
		self.request_count = 0
		self.requests = {} # k: request_id, v: (value, final value)

	@exactly_once_method(filename="server_checkpoint.txt", server=self.server)
	def run_op(self):
		# rand_key = "key" #str(np.random.rand())
		rand_val = np.random.rand()
		request_id = str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		# self.requests[request_id] = (rand_val, 0)
		return self.server.undecorated_add.remote(request_id, rand_val)

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-clients", default=1, type=int)
	# parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-requests", default=3, type=int)
	parser.add_argument("--exactly-once", action="store_true")
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	server = Server.remote(args.exactly_once)
	clients = [Client.remote(i, server) for i in range(args.num_clients)]

	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]

	print("final values: ", ray.get(ray.get(refs)))

	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))


