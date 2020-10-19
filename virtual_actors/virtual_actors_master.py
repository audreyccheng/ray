import random
import ray 
import time
from uhashring import HashRing

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
	def __init__(self):
		# Key: string (name of virtual actor), value: virtual actor instance
		# Instances of virtual actors only exist on physical actor
		self.virtual_actors = {}

	# Only synchronous functions currently supported
	def execute_task(self, actor_class, key, method_name, *args):
		# Create virtual actor instance if key not found
		if not key in self.virtual_actors:
			virtual_actor = actor_class()
			self.virtual_actors[key] = virtual_actor
		# Execute function on virtual actor
		return getattr(self.virtual_actors[key], method_name)(*args)

@ray.remote
class VirtualActorGroup(object):
	def __init__(self, num_physical_actors):
		# Key: string (name of virtual actor), value: physical actor
		self.actor_handles = {}
		self.physical_actor_names = ["PhysicalActor-" + str(i) for i in range(num_physical_actors)]
		# Allocate physical actors during construction for now, autoscaling for later
		self.physical_actors = [PhysicalActor.options(name=actor_name).remote(
			) for actor_name in self.physical_actor_names]
		self.hash_ring = HashRing(nodes=self.physical_actor_names)

	# Return physical actor corresponding to the key of a virtual actor instance
	# Allocate key to physical actor if it is not yet assigned
	def get_physical_actor(self, key):
		if not key in self.actor_handles:
			physical_actor = self.hash_ring.get_node(key)
			self.actor_handles[key] = physical_actor
		return self.actor_handles[key]


@ray.remote
class Client:
	def __init__(self, master, actor_class):
		random.seed(time.time())
		self.key = "client-" + str(random.randrange(100))
		self.master = master
		self.actor_class = actor_class
		self.actor_name = ray.get(master.get_physical_actor.remote(self.key))
		self.actor = ray.get_actor(self.actor_name)
		# self.actor =  ray.get(master.get_physical_actor.remote(self.key))

	def send_request(self, method_name, *args):
		return ray.get(self.actor.execute_task.remote(self.actor_class, self.key, method_name, *args))

if __name__ == "__main__":
	ray.init()

	virtual_actor_group = VirtualActorGroup.options(
		name="VirtualActorGroup", lifetime="detached").remote(2)

	clients = [Client.remote(virtual_actor_group, ActorClass) for _ in range(5)]

	refs = []
	for client in clients:
		refs += [client.send_request.remote("foo", 1)]
	print(ray.get(refs))

