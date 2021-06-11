import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import signal
import sys
import time

@ray.remote
class ControlActor(object):
	def __init__(self, fail, num_requests):
		self.fail = fail
		self.num_requests = num_requests
		self.server_requests = 0
		self.client_requests = 0

	def receive_server_msg(self, pid):
		print("Got server msg from ", pid)
		self.server_requests += 1
		if self.fail and self.server_requests == self.num_requests:
			os.kill(pid, signal.SIGKILL)

	def receive_client_msg(self, pid):
		print("Got client msg from ", pid)
		self.client_requests += 1

	def get_server_count(self):
		return self.server_requests

	def get_client_count(self):
		return self.client_requests

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Server(object):
	def __init__(self, use_checkpoint, control_actor):
		self.requests = {} # k: request_id, v: (value, final value)
		# self.store = {}
		self.value = 0
		self.directory = os.path.dirname(os.path.realpath(__file__))
		self.path = os.path.join(self.directory, "checkpoint.txt")
		self.control_actor = control_actor

		if use_checkpoint:
			try:
			    f = open(self.path, 'r')
			    for line in f:
			    	vals = line.rstrip().split(" ")
			    	assert(len(vals) == 3)
			    	self.requests[vals[0]] = (float(vals[1]), float(vals[2]))
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
		f = open(self.path, "a")
		f.write(str(request_id) + " " + str(value) + " " + str(self.value) + "\n") #self.store[request_id]
		f.close()

		ray.get(self.control_actor.receive_server_msg.remote(os.getpid()))

		print("request: ", request_id, ", computed value: ", self.value)
		# time.sleep(5)

		return self.value #self.store[request_id]

@ray.remote(max_restarts=1, max_task_retries=1)
class Client(object):
	def __init__(self, client_id, server, control_actor):
		self.client_id = client_id
		self.server = server
		self.request_count = 0
		self.requests = {} # k: request_id, v: (value, final value)
		self.control_actor = control_actor

	def run_op(self):
		# rand_key = "key" #str(np.random.rand())
		rand_val = np.random.rand()
		request_id = str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		self.requests[request_id] = (rand_val, 0)
		return self.server.add.remote(request_id, rand_val)

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

	control = ControlActor.remote(True, 2)
	server = Server.remote(args.exactly_once, control)
	clients = [Client.remote(i, server, control) for i in range(args.num_clients)]

	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]

	print("final values: ", ray.get(ray.get(refs)))

	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "checkpoint.txt"))
