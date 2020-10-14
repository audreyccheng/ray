import ray 

class ActorClass(object):
	def __init__(self):
		self.counter = 0

	def foo(self):
		self.counter += 1
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

	def foo(self, key):
		# Create virtual actor instance if key not found
		if not key in self.virtual_actors:
			virtual_actor = ActorClass()
			self.virtual_actors[key] = virtual_actor
		return self.virtual_actors[key].foo()


class VirtualActorGroup(object):
	def __init__(self, actor_class):
		# ActorClass type of actor group
		self.actor_class = actor_class
		# Key: string (name of virtual actor), value: physical actor
		self.actor_handles = {}
		# Allocate physical actors during construction for now, autoscaling for later
		self.physical_actors = [PhysicalActor.remote() for _ in range(1)]

	# Defaultdict API
	# 1. Caller responsible for submitting actor task to physical actor (simpler)
	# 2. Define Foo class on actor group: user doesn't need to deal with
	#    physical actor handles
	def foo(self, key):
		if not key in self.actor_handles:
			# Many options to route virtual actors to physical ones
			physical_actor = self.physical_actors[0]
			self.actor_handles[key] = physical_actor
		return self.actor_handles[key].foo.remote(key)

ray.init()
virtual_actor_group = VirtualActorGroup(ActorClass)
val = ray.get(virtual_actor_group.foo("key"))
print(val)