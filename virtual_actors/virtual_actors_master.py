import random
import ray
import time
from uhashring import HashRing
import sys
import os

class ActorClass(object):
	def __init__(self):
		self.counter = 0

	def foo(self, num):
		self.counter += 1 + num
		return self.counter

# Physical actor that supports virtual actors
# Functions should mirror those of virtual actors
# Will need to wrap functions and pass key of virtual actor
# This should be done programatically eventually for users
@ray.remote
class PhysicalActor(object):
	def __init__(self, actor_class):
		# Key: string (name of virtual actor), value: virtual actor instance
		# Instances of virtual actors only exist on physical actor
		self.virtual_actors = {}
		self.actor_class = actor_class

	# Only synchronous functions currently supported
	def execute_task(self, key, method_name, *args):
		# Create virtual actor instance if key not found
		if not key in self.virtual_actors:
			virtual_actor = self.actor_class(key)
			self.virtual_actors[key] = virtual_actor
		# Execute function on virtual actor
		return getattr(self.virtual_actors[key], method_name)(*args)

@ray.remote(max_restarts=-1, max_task_retries=-1)
class VirtualActorGroup(object):
	def __init__(self, num_physical_actors, actor_class):
		# Key: string (name of virtual actor), value: physical actor handle
		self.actor_handles = {}
		# ActorClass type of actor group
		self.actor_class = actor_class
		self.physical_actor_names = ["PhysicalActor-" + actor_class.__name__ + str(i) for i in range(num_physical_actors)]
		self.log = "log.txt"
		self.failed = False
		# Check if this is a clean start
		# if not os.path.exists(self.log):
		# 	# Create log file if it doesn't exist
		# 	with open(self.log, 'w'): pass
			# Allocate physical actors during construction for now, autoscaling for later
		self.physical_actors = [PhysicalActor.options(name=self.physical_actor_names[i], lifetime="detached",
			).remote(
			self.actor_class) for i in range(len(self.physical_actor_names))]
		# else:
		# 	self.failed = True
		# 	# Get running physical actors
		# 	self.physical_actors = []
		# 	for actor in self.physical_actor_names:
		# 		self.physical_actors.append(ray.get_actor(actor))
		# 	# Read in actor handle mappings from log
		# 	log = open(self.log,"r")
		# 	line = log.readline()
		# 	while line:
		# 		mapping = line.split(":")
		# 		# Check that mapping is in format key:physical-actor-name
		# 		if len(mapping) == 2 and mapping[1] in self.physical_actor_names:
		# 			actor_index = self.physical_actor_names.index(mapping[1])
		# 			self.actor_handles[mapping[0]] = self.physical_actors[actor_index]
		# 		line = log.readline()
		# 	log.close()
		self.hash_ring = HashRing(nodes=self.physical_actors)
		self.count = 0

	# Return physical actor corresponding to the key of a virtual actor instance
	# Allocate key to physical actor if it is not yet assigned
	def get_physical_actor(self, key):
		self.count += 1
		if self.count == -1 and not self.failed: # Set number to trigger failure
			sys.exit()
		if not key in self.actor_handles:
			physical_actor = self.hash_ring.get_node(key)
			self.actor_handles[key] = physical_actor
			# Write corresponding actor name to disk
			actor_index = self.physical_actors.index(physical_actor)
			actor_name = self.physical_actor_names[actor_index]
			# log = open(self.log, "a")
			# log.write(key + ":" + actor_name + "\n")
			# log.close()
		return [self.actor_handles[key]]

	# Remove log file when closing master
	# def close(self):
	# 	os.remove(self.log)


# General client class that will send requests for 
# specified actor_class
# @ray.remote
class Client:
	def __init__(self, master):
		# random.seed(time.time())
		# self.key = "client-" + str(random.randrange(100))
		self.master = ray.get_actor(master)
		# Cache physical actor so that master only needs to be contacted once
		# Key: string (name of virtual actor), value: physical actor handle
		self.actor_handles = {}

	def __getattr__(self, method_name, *args):
		return PhysicalActorMethod(
			self.master,
			self.actor_handles,
			method_name,
			*args)

class PhysicalActorMethod:
	def __init__(self, master, actor_handles, method_name):
		self._master = master
		self.actor_handles = actor_handles
		self._method_name = method_name

	# Key called by user of Client
	def __call__(self, key, *args):
		# Get the appropriate physical actor.
		if not key in self.actor_handles:
			physical_actor = ray.get(self._master.get_physical_actor.remote(key))[0]
			self.actor_handles[key] = physical_actor
		return self.actor_handles[key].execute_task.remote(key, self._method_name, *args)

if __name__ == "__main__":
	ray.init()

	virtual_actor_group = VirtualActorGroup.options(
		name="VirtualActorGroup", lifetime="detached").remote(2, Client)

	clients = [Client.remote(virtual_actor_group, ActorClass) for _ in range(5)]

	refs = []
	for client in clients:
		refs += [client.send_request.remote("foo", 1)]
	print(ray.get(refs))

	ray.get(virtual_actor_group.close.remote())
	

