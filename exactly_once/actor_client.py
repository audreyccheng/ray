import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import sys
import time

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Server(object):
	def __init__(self, exactly_once):
		self.requests = {} # k: request_id, v: (value, final value)
		self.value = 0
		self.directory = os.path.dirname(os.path.realpath(__file__))
		self.path = os.path.join(self.directory, "checkpoint_server.txt")
		self.exactly_once = exactly_once

		if self.exactly_once:
			try:
			    f = open(self.path, 'r')
			    for line in f:
			    	vals = line.rstrip().split(" ")
			    	assert(len(vals) == 3)
			    	self.requests[vals[0]] = (vals[1], vals[2])
			    	self.value += float(vals[2])
			    f.close()
			except FileNotFoundError:
			    print('Checkpoint does not exist')

	def add(self, request_id, value):
		# if this request exists, return final value
		if request_id in self.requests:
			return self.requests[request_id][1]	 

		self.value += float(value)
		self.requests[request_id] = (value, self.value)

		# save checkpoint
		if self.exactly_once:
			f = open(self.path, "a")
			f.write(str(request_id) + " " + str(value) + " " + str(self.value) + "\n")
			f.close()

		print("server request: ", request_id, ", computed value: ", self.value)
		time.sleep(5)

		return self.value

@ray.remote(max_restarts=1, max_task_retries=1)
class Client(object):
	def __init__(self, client_id, server, exactly_once):
		self.client_id = client_id
		self.server = server
		self.request_count = 0
		self.requests = {} # k: request_id, v: (value, final value)
		self.exactly_once = exactly_once
		self.directory = os.path.dirname(os.path.realpath(__file__))
		self.path = os.path.join(self.directory, "checkpoint_client.txt")

		if self.exactly_once:
			try:
			    f = open(self.path, 'r')
			    for line in f:
			    	vals = line.rstrip().split(" ")
			    	assert(len(vals) == 3)
			    	self.requests[vals[0]] = (vals[1], vals[2])
			    	self.request_count += 1
			    f.close()
			except FileNotFoundError:
			    print('Checkpoint does not exist')

	def run_client_op(self, request_id, value):
		if request_id in self.requests:
			return self.requests[request_id][1]
		client_request_id = request_id + ":" + str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		
		server_value = ray.get(self.server.add.remote(client_request_id, value))
		final_value = float(server_value) + 1
		self.requests[request_id] = (value, final_value)

		# save checkpoint
		if self.exactly_once:
			f = open(self.path, "a")
			f.write(str(request_id) + " " + str(value) + " " + str(final_value) + "\n")
			f.close()

		print("client request: ", request_id, ", computed value: ", final_value)
		time.sleep(5)

		return final_value

@ray.remote(max_restarts=1, max_task_retries=1)
class NestedClient(object):
	def __init__(self, client_id, client):
		self.client_id = client_id
		self.client = client
		self.request_count = 0
		# self.requests = {} # k: request_id, v: (value, final value)

	def run_op(self):
		rand_val = np.random.rand()
		request_id = str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		# self.requests[request_id] = (rand_val, 0)
		return self.client.run_client_op.remote(request_id, rand_val)

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-nested-clients", default=1, type=int)
	# parser.add_argument("--num-clients", default=1, type=int)
	# parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-requests", default=3, type=int)
	parser.add_argument("--exactly-once", action="store_true")
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	server = Server.remote(args.exactly_once)
	client = Client.remote(0, server, args.exactly_once)
	nested_clients = [NestedClient.remote(i, client) for i in range(args.num_nested_clients)]

	refs = []
	for nclient in nested_clients:
		refs += [nclient.run_op.remote() for _ in range(args.num_requests // args.num_nested_clients)]

	print("final values: ", ray.get(ray.get(refs)))

	if args.exactly_once:
		os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "checkpoint_server.txt"))
		os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "checkpoint_client.txt"))
