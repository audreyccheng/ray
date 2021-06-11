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

# constructor decorator
def exactly_once_init(exactly_once, filename):
	def exactly_once_init_method(init_fn):
		directory = os.path.dirname(os.path.realpath(__file__))
		path = os.path.join(directory, filename)
		server_path = os.path.join(directory, "private_" + filename)

		def wrapped_init(self, *args):
			init_fn(self, *args)
			if exactly_once:
				# restore requests
				try:
					f = open(path, 'r')
					for line in f:
						vals = line.rstrip().split(" ")
						assert(len(vals) == 2)
						self.requests[vals[0]] = vals[1]
						# self.value += float(vals[2])
						# print("recovered request id {} value {}", vals[0], vals[1])
					f.close()
				except FileNotFoundError:
					# print('Server checkpoint does not exist')
					pass

				# restore actor private state
				try:
					f = open(server_path, 'r')
					for line in f:
						vals = line.rstrip().split(" ")
						assert(len(vals) == 2)
						self.restore_state(vals[1])
						# print("recovered value: {}", vals[1])
					f.close()
				except FileNotFoundError:
					# print('Private server checkpoint does not exist')
					pass

		return wrapped_init
	return exactly_once_init_method

# method decorator
def exactly_once_method(filename):
	def exactly_once_fn(fn):
		directory = os.path.dirname(os.path.realpath(__file__))
		path = os.path.join(directory, filename)
		server_path = os.path.join(directory, "private_" + filename)

		def wrapped_fn(self, request_id, *args):
			# if request_id in wrapped_fn.requests:
			# 	return wrapped_fn.requests[request_id]
			if request_id in self.requests:
				return self.deserialize_method_value(self.requests[request_id])

			# get result from server
			result = fn(self, request_id, *args)
			# wrapped_fn.requests[request_id] = result
			self.requests[request_id] = str(result)

			# save request
			f = open(path, "a")
			f.write(str(request_id) + " " + self.serialize_method_value(result) + "\n")
			f.close()

			# save private actor state
			fp = open(server_path, "a")
			fp.write(str(request_id) + " " + self.get_state() + "\n")
			fp.close()

			# notify control server
			if filename == "server_checkpoint.txt":
				ray.get(self.control_actor.receive_server_msg.remote(os.getpid()))
			elif filename == "client_checkpoint.txt":
				ray.get(self.control_actor.receive_client_msg.remote(os.getpid()))

			# print("wrote to files {} {}", path, server_path)
			# time.sleep(5)

			return result
		# wrapped_fn.request_id = 0
		# wrapped_fn.requests = {}
		return wrapped_fn
	return exactly_once_fn

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Server(object):
	@exactly_once_init(exactly_once=True, filename="server_checkpoint.txt")
	def __init__(self, control_actor):
		self.control_actor = control_actor
		self.value = 0
		self.requests = {}

	@exactly_once_method(filename="server_checkpoint.txt")
	def add(self, request_id, value):
		self.value += float(value)
		# print("server computed value: ", self.value)
		return self.value

	def serialize_method_value(self, value):
		return str(value)

	def deserialize_method_value(self, value):
		return float(value)

	# return state (as string) from actor that needs to be saved
	def get_state(self):
		return str(self.value)

	# set actor state via args
	def restore_state(self, value):
		self.value = float(value)

@ray.remote(max_restarts=1, max_task_retries=1)
class Client(object):
	@exactly_once_init(exactly_once=True, filename="client_checkpoint.txt")
	def __init__(self, client_id, server, control_actor):
		self.client_id = client_id
		self.server = server
		self.request_count = 0
		self.requests = {} # k: request_id, v: (value, final value)
		self.control_actor = control_actor

	@exactly_once_method(filename="client_checkpoint.txt")
	def run_client_op(self, request_id, value):
		client_request_id = "c" + str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		
		server_value = ray.get(self.server.add.remote(client_request_id, value))
		final_value = float(server_value) + 1
		# print("client computed value: ", final_value)
		return final_value

	def serialize_method_value(self, value):
		return str(value)

	def deserialize_method_value(self, value):
		return float(value)

	def get_state(self):
		return str(self.request_count)

	def restore_state(self, value):
		self.request_count = int(value)

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

@ray.remote
class ControlActor(object):
	def __init__(self, fail_server, fail_client, num_requests):
		self.fail_server = fail_server
		self.fail_client = fail_client
		self.num_requests = num_requests
		self.server_requests = 0
		self.client_requests = 0

	def receive_server_msg(self, pid):
		print("***** Got server msg from ", pid)
		self.server_requests += 1
		if self.fail_server and self.server_requests == self.num_requests:
			os.kill(pid, signal.SIGKILL)

	def receive_client_msg(self, pid):
		print("----- Got client msg from ", pid)
		self.client_requests += 1
		if self.fail_client and self.client_requests == self.num_requests:
			os.kill(pid, signal.SIGKILL)

	def get_server_count(self):
		return self.server_requests

	def get_client_count(self):
		return self.client_requests

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-nested-clients", default=1, type=int)
	# parser.add_argument("--num-clients", default=1, type=int)
	# parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-requests", default=3, type=int)
	# parser.add_argument("--exactly-once", action="store_true")
	parser.add_argument("--fail-after", default=1, type=int)
	parser.add_argument("--fail-server", action="store_true")
	parser.add_argument("--fail-client", action="store_true")
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	control = ControlActor.remote(args.fail_server, args.fail_client, args.fail_after)
	server = Server.remote(control)
	client = Client.remote(0, server, control)
	nested_clients = [NestedClient.remote(i, client) for i in range(args.num_nested_clients)]

	refs = []
	for nclient in nested_clients:
		refs += [nclient.run_op.remote() for _ in range(args.num_requests // args.num_nested_clients)]

	print("final values: ", ray.get(ray.get(refs)))

	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))
	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "private_server_checkpoint.txt"))
	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "client_checkpoint.txt"))
	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "private_client_checkpoint.txt"))


