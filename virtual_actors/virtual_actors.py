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

	def execute_task(self, method_name, key):
		# Create virtual actor instance if key not found
		if not key in self.virtual_actors:
			virtual_actor = ActorClass()
			self.virtual_actors[key] = virtual_actor
		# Execute function on virtual actor
		return getattr(self.virtual_actors[key],
				method_name)()

class VirtualActorGroup(object):
	def __init__(self, actor_class):
		# ActorClass type of actor group
		self.actor_class = actor_class
		# Key: string (name of virtual actor), value: physical actor
		self.actor_handles = {}
		# Allocate physical actors during construction for now, autoscaling for later
		self.physical_actors = [PhysicalActor.remote() for _ in range(1)]

	def __getattr__(self, method_name):
		return PhysicalActorMethod(
			self,
			method_name)

class PhysicalActorMethod:
	def __init__(self, virtual_actor_group, method_name):
		self._v_actor_gp = virtual_actor_group
		self._method_name = method_name

	def __call__(self, key, *args):
		# Call method_name on the appropriate physical actor.
		# System takes care of routing client request to physical actor
		#  (vs. returning the physical actor handle to the client and
		#  having the client execute directly on the physical actor)	
		if not key in self._v_actor_gp.actor_handles:
			# Many options to route virtual actors to physical ones
			physical_actor = self._v_actor_gp.physical_actors[0]
			self._v_actor_gp.actor_handles[key] = physical_actor
		physical_actor = self._v_actor_gp.actor_handles[key]
		return physical_actor.execute_task.remote(self._method_name, key)

ray.init()
virtual_actor_group = VirtualActorGroup(ActorClass)
val = ray.get(virtual_actor_group.foo("key"))
print(val)
