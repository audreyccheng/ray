import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
from ray import cloudpickle
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
				self.requests = {}
				self.request_count = 0
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

				# restore requests
				try:
					f = open(path, 'r')
					for line in f:
						vals = line.rstrip().split(" ")
						# print(vals[0])
						# print(vals[1])
						assert(len(vals) == 2)
						self.requests[vals[0]] = float(vals[1]) #cloudpickle.loads(vals[1])
						self.value = float(vals[1])
						# self.value += float(vals[2])
						# print("recovered request id {} value {}", vals[0], vals[1])
					f.close()
				except FileNotFoundError:
					# print('Server checkpoint does not exist')
					pass

				

		return wrapped_init
	return exactly_once_init_method

def exactly_once_method(checkpoint_freq, filename):
	def exactly_once_fn(fn):
		directory = os.path.dirname(os.path.realpath(__file__))
		path = os.path.join(directory, filename)
		server_path = os.path.join(directory, "private_" + filename)

		def wrapped_fn(self, request_id, *args):
			# if request_id in wrapped_fn.requests:
			# 	return wrapped_fn.requests[request_id]
			if request_id in self.requests:
				return self.requests[request_id]

			# get result from server
			result = fn(self, request_id, *args)
			# wrapped_fn.requests[request_id] = result
			self.requests[request_id] = result
			self.request_count += 1

			# overwrite old requests if we checkpoint
			if (self.request_count % checkpoint_freq) == 0:
				f = open(path, "w")
				f.write(str(request_id) + " " + str(result) + "\n") #cloudpickle.dumps(result))
				f.close()
			else:
				f = open(path, "a")
				f.write(str(request_id) + " " + str(result) + "\n") #cloudpickle.dumps(result))
				f.close()

			if (self.request_count % checkpoint_freq) == 0:
				# save private actor state
				fp = open(server_path, "w")
				fp.write(str(request_id) + " " + self.get_state() + "\n")
				fp.close()

			# notify control server
			# if filename == "server_checkpoint.txt":
			# 	ray.get(self.control_actor.receive_server_msg.remote(os.getpid()))
			# elif filename == "client_checkpoint.txt":
			# 	ray.get(self.control_actor.receive_client_msg.remote(os.getpid()))

			# print("wrote to files {} {}", path, server_path)
			# time.sleep(3)

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
		# self.requests = {}

	@exactly_once_method(checkpoint_freq=1000, filename="server_checkpoint.txt")
	def decorated_add(self, request_id, value):
		self.value += float(value)
		# print("server computed value: ", self.value)
		return self.value

	# def serialize_method_value(self, value):
	# 	return str(value)

	# def deserialize_method_value(self, value):
	# 	return float(value)

	# return state (as string) from actor that needs to be saved
	def get_state(self):
		return str(self.value)

	# set actor state via args
	def restore_state(self, value):
		self.value = float(value)

@ray.remote(max_restarts=1, max_task_retries=1)
class Client(object):
	def __init__(self, client_id, server):
		self.client_id = client_id
		self.server = server
		self.request_count = 0
		self.requests = {} # k: request_id, v: (value, final value)

	def run_op(self):
		# rand_key = "key" #str(np.random.rand())
		rand_val = np.random.rand()
		request_id = str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		self.requests[request_id] = (rand_val, 0)
		return self.server.decorated_add.remote(request_id, rand_val)

@ray.remote
class ControlActor(object):
	def __init__(self, fail, num_requests):
		self.fail = fail
		self.num_requests = num_requests
		self.server_requests = 0
		self.client_requests = 0

	def receive_server_msg(self, pid):
		# print("Got server msg from ", pid)
		self.server_requests += 1
		if self.fail and self.server_requests == self.num_requests:
			os.kill(pid, signal.SIGKILL)

	def receive_client_msg(self, pid):
		# print("Got client msg from ", pid)
		self.client_requests += 1

	def get_server_count(self):
		return self.server_requests

	def get_client_count(self):
		return self.client_requests

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-clients", default=1, type=int)
	# parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-requests", default=3, type=int)
	# parser.add_argument("--exactly-once", action="store_true")
	parser.add_argument("--fail-after", default=1, type=int)
	parser.add_argument("--fail", action="store_true")
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	control = ControlActor.remote(args.fail, args.fail_after)
	server = Server.remote(control)
	clients = [Client.remote(i, server) for i in range(args.num_clients)]

	tstart = time.time()
	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]
	results = ray.get(ray.get(refs))
	tend = time.time()
	print("time: ", tend - tstart)
	print("throughput: ", args.num_requests / (tend - tstart))

	# print("final values: ", results)

	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))
	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "private_server_checkpoint.txt"))
	


