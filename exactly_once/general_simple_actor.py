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
		# server_path = os.path.join(directory, "private_" + filename)

		def wrapped_init(self, *args, **kwargs):
			init_fn(self, *args)
			if exactly_once:
				self.requests = {}
				self.request_count = 0
				# restore requests
				try:
					tstart = time.time()
					f = open(path, 'r')
					count = 0
					for line in f:
						vals = line.rstrip().split(" ")
						# print(vals[0])
						# print(vals[1])
						assert(len(vals) >= 2)
						self.requests[vals[0]] = float(vals[1]) #cloudpickle.loads(vals[1])
						if len(vals) == 3:
							self.restore_state(vals[2])
						# else:
						# 	time.sleep(0.1)
							# self.value = float(vals[2])
						# else:
							self.replay(vals[1])
							# self.value += float(vals[1])
						count += 1
					print("Total lines read: ", count)
					f.close()
					tend = time.time()
					print("Recovery time: ", tend - tstart)
				except FileNotFoundError:
					pass

		return wrapped_init
	return exactly_once_init_method

def exactly_once_method(checkpoint_freq, exactly_once, filename):
	def exactly_once_fn(fn):
		directory = os.path.dirname(os.path.realpath(__file__))
		path = os.path.join(directory, filename)
		# server_path = os.path.join(directory, "private_" + filename)

		def wrapped_fn(self, request_id, *args, **kwargs):
			# print(kwargs)
			# request_id = kwargs.get("request_id")
			# print(request_id)
			if exactly_once:
				if request_id in self.requests:
					return self.requests[request_id]

			# get result from server
			result = fn(self, request_id, *args)

			if exactly_once:
				self.request_count += 1
				# wrapped_fn.requests[request_id] = result
				self.requests[request_id] = result

				checkpoint_str = ""
				if self.request_count % checkpoint_freq == 0:
					checkpoint_str = self.get_state()
				# write_str = str(request_id) + " " + str(result)
				# if checkpoint_freq > 1:
				write_str = str(request_id) + " " + str(result) + " " + checkpoint_str + "\n"
				if self.large_write and (self.request_count % checkpoint_freq) == 0:
					write_str = str(request_id) + " " + str(result) + " " + checkpoint_str + " "
					for i in range(10000):
						write_str += "random-string-to-take-up-space"
					write_str += "\n"

				# overwrite old requests if we checkpoint
				if (self.request_count % checkpoint_freq) == 0 and checkpoint_freq > 1:
					f = open(path, "w")
					f.write(write_str) #cloudpickle.dumps(result))
					f.close()
				else:
					f = open(path, "a")
					f.write(write_str) #cloudpickle.dumps(result))
					f.close()

			# notify control server
			# if not self.no_control:
			# 	if filename == "server_checkpoint.txt":
			# 		fail_now = ray.get(self.control_actor.receive_server_msg.remote(os.getpid()))
			# 		if fail_now:
			# 			ray.get(self.control_actor.add_latencies.remote(self.latencies))
			# 			ray.get(self.control_actor.kill_server.remote(os.getpid()))
			# 	elif filename == "client_checkpoint.txt":
			# 		ray.get(self.control_actor.receive_client_msg.remote(os.getpid()))

			return result
		return wrapped_fn
	return exactly_once_fn

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Server(object):
	@exactly_once_init(exactly_once=False, filename="server_checkpoint.txt")
	def __init__(self, control_actor, large_write, long_compute, no_control):
		self.control_actor = control_actor
		self.control_actor.receive_server_msg.remote(os.getpid())
		self.value = 0
		self.latencies = []
		self.large_write = large_write
		self.long_compute = long_compute
		self.no_control = no_control

	@exactly_once_method(checkpoint_freq=1, exactly_once=False, filename="server_checkpoint.txt")
	def decorated_add(self, request_id, value, request_start):
		# print('got client request')
		self.value += float(value)
		# print("server computed value: ", self.value)
		self.latencies.append(time.time() - request_start)
		# print("latencies length: ", len(self.latencies))
		if self.long_compute:
			time.sleep(0.001)
		return self.value

	# return state (as string) from actor that needs to be saved
	def get_state(self):
		return str(self.value)

	# set actor state via args
	def restore_state(self, value):
		self.value = float(value)

	def replay(self, req):
		if self.long_compute:
			time.sleep(0.001)
		self.value += float(req)

	def get_latencies(self):
		# print("requested latencies")
		return self.latencies

@ray.remote(max_restarts=1, max_task_retries=1)
class Client(object):
	def __init__(self, client_id, server, request_rate, batch_size):
		self.client_id = client_id
		self.server = server
		self.request_count = 0
		self.requests = {} # k: request_id, v: (value, final value)
		self.batch_interval = 1 / request_rate
		self.batch_size = batch_size

	def run_op(self):
		# rand_key = "key" #str(np.random.rand())
		request_start = time.time()
		rand_val = np.random.rand()
		request_id = str(self.client_id) + ":" + str(self.request_count)
		self.request_count += 1
		self.requests[request_id] = (rand_val, 0)
		return self.server.decorated_add.remote(request_id, rand_val, request_start)

	def run_batches(self, num_batches):
		results = []
		start = time.time()
		next_batch = start
		for i in range(num_batches):
			# diff = next_batch - time.time()
			# if diff > 0:
			# 	print("diff ", diff)
			# 	time.sleep(diff)

			request_start = time.time()
			rand_val = np.random.rand()
			request_id = str(self.client_id) + ":" + str(self.request_count)
			self.request_count += 1
			self.requests[request_id] = (rand_val, 0)
			ref = self.server.decorated_add.remote(request_id, rand_val, request_start)
			# print('sent client request ', self.request_count)
			results.append(ref)
		
			# print("len results ", len(results))
			if i % 2 == 0 and len(results) >= self.batch_size * 4:
				done_ids, results = ray.wait(results, num_returns=len(results) // 2)
				# print(len(results))
			next_batch += self.batch_interval

		return ray.get(results)


@ray.remote
class ControlActor(object):
	def __init__(self, fail, num_requests):
		self.fail = fail
		self.failed = False
		if not self.fail:
			self.failed =True
		self.num_requests = num_requests
		self.server_requests = 0
		self.client_requests = 0
		self.latencies = []
		self.server = None

	def set_server(self, server):
		self.server = server

	def receive_server_msg(self, pid):
		# print("Got server msg from ", pid)
		# self.server_requests += 1
		if not self.failed:
			time.sleep(3) #3) 4) #8)  #1 #(1.05)
			self.latencies += ray.get(self.server.get_latencies.remote())
			os.kill(pid, signal.SIGKILL)
		self.failed = True
		# if self.fail and self.server_requests == self.num_requests:
		# 	return True
		# return False

	def add_latencies(self, latencies):
		# print("adding latencies ", latencies)
		self.latencies += latencies

	def kill_server(self, pid):
		os.kill(pid, signal.SIGKILL)

	def receive_client_msg(self, pid):
		# print("Got client msg from ", pid)
		self.client_requests += 1

	def get_server_count(self):
		return self.server_requests

	def get_client_count(self):
		return self.client_requests

	def get_latencies(self):
		return self.latencies

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-clients", default=1, type=int)
	# parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-requests", default=3, type=int)
	# parser.add_argument("--exactly-once", action="store_true")
	parser.add_argument("--fail-after", default=1, type=int)
	parser.add_argument("--fail", action="store_true")
	parser.add_argument("--output", action="store_true")
	parser.add_argument("--output-file", default="", type=str)
	parser.add_argument("--large-write", action="store_true")
	parser.add_argument("--long-compute", action="store_true")
	parser.add_argument("--no-control", action="store_true")
	parser.add_argument("--batch-rate", default=20, type=int)
	parser.add_argument("--batch-size", required=True, type=int)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	control = ControlActor.remote(args.fail, args.fail_after)
	server = Server.remote(control, args.large_write, args.long_compute, args.no_control)
	ray.get(control.set_server.remote(server))
	clients = [Client.remote(i, server, args.batch_rate, args.batch_size) for i in range(args.num_clients)]

	tstart = time.time()
	refs = []
	for client in clients:
		refs += [client.run_batches.remote(args.num_requests // args.num_clients)]
		# refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]
	# results = ray.get(ray.get(refs))
	results = ray.get(refs)
	tend = time.time()
	print("time: ", tend - tstart)
	print("throughput: ", args.num_requests / (tend - tstart))
	# if args.fail:
	latencies = ray.get(control.get_latencies.remote())
	latencies += ray.get(server.get_latencies.remote())
	if args.output:
		# os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), args.output_file + ".txt"))
		for result in latencies:
			with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), args.output_file + ".txt"), 'a+') as f:
				f.write("{}\n".format(result))
	print("latencies length: ", len(latencies))
	# print("done")
	# print("final values: ", results)

	os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))
	# os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "private_server_checkpoint.txt"))



