import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import time

@ray.remote
class Server(object):
	def __init__(self):
		self.kvstore = {}

	def put(self, key, value):
		self.kvstore[key] = value

	def get(self, key):
		val = None
		if key in self.kvstore:
			val = self.kvstore[key]
		return val

	# def delete(self, key):
	# 	return self.kvstore.pop('key', None)

@ray.remote(num_cpus=0)
class Client(object):
	def __init__(self, servers, num_requests):
		self.servers = servers
		self.num_requests = num_requests

	async def run_op(self):
		rand_val = np.random.rand()
		rand_server = randrange(len(self.servers))
		if rand_val < 0.5:
			return await self.servers[rand_server].put.remote(randrange(self.num_requests), rand_val)
		else:
			return await self.servers[rand_server].get.remote(randrange(self.num_requests))

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run the key-value store.")
	
	parser.add_argument("--num-clients", default=1, type=int)
	parser.add_argument("--num-servers", default=1, type=int)
	parser.add_argument("--num-clients-per-node", default=1, type=int)
	parser.add_argument("--num-servers-per-node", default=1, type=int)
	parser.add_argument("--num-requests", default=10000, type=int)
	parser.add_argument("--num-nodes", required=True, type=int)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init(address="auto")

	nodes = [node for node in ray.nodes() if node["Alive"]]
	while len(nodes) < args.num_nodes:
		time.sleep(1)
		print("{} nodes found, waiting for nodes to join".format(len(nodes)))
		nodes = [node for node in ray.nodes() if node["Alive"]]

	import socket
	ip_addr = socket.gethostbyname(socket.gethostname())
	node_resource = "node:{}".format(ip_addr)

	for node in nodes:
		if node_resource in node["Resources"]:
			if "head" not in node["Resources"]:
				ray.experimental.set_resource("head", 100, node["NodeID"])

	for node in nodes:
		for resource in node["Resources"]:
			if resource.startswith("client") or resource.startswith("server") or resource.startswith("worker"):
				ray.experimental.set_resource(resource, 0, node["NodeID"])

	nodes = [node for node in ray.nodes() if node["Alive"]]
	print("All nodes joined")
	for node in nodes:
		print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))

	head_node = [node for node in nodes if "head" in node["Resources"]]
	assert len(head_node) == 1
	head_ip = head_node[0]["NodeManagerAddress"]
	nodes.remove(head_node[0])

	client_resources = ["client:{}".format(i) for i in range(args.num_clients // args.num_clients_per_node)]
	server_resources = ["server:{}".format(i) for i in range(args.num_servers // args.num_servers_per_node)]
	owner_ip = None
	owner_resource = None
	node_index = 0

	# for resource in server_resources:
	# 	if "CPU" not in head_node[0]["Resources"]:
	# 		continue

	# 	print("Assigning", resource, "to node", head_node[0]["NodeID"], head_node[0]["Resources"])
	# 	ray.experimental.set_resource(resource, 100, head_node[0]["NodeID"])

	# for i in range(len(client_resources)):
	# 	node = nodes[i % len(nodes)]
	# 	if "CPU" not in node["Resources"]:
	# 		continue

	# 	print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
	# 	ray.experimental.set_resource(resource, 100, node["NodeID"])

	assert len(nodes) >= len(client_resources) + len(server_resources)
	for node, resource in zip(nodes, client_resources + server_resources):
		if "CPU" not in node["Resources"]:
			continue

		print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
		ray.experimental.set_resource(resource, 100, node["NodeID"])

	# server = Server.remote()
	servers = [Server.options(resources={server_resources[i % len(server_resources)]: 1},
		max_concurrency=64).remote() for i in range(args.num_servers)] # options(max_concurrency=10000).
	# client = Client.remote(servers)
	clients = [Client.options(resources={client_resources[i % len(client_resources)]: 1}).remote(
            servers, args.num_requests) for i in range(args.num_clients)]

	tstart = time.time()
	# for _ in range(args.num_requests):
	# 	ray.get(client.run_op.remote())
	refs = []
	for client in clients:
		refs += [client.run_op.remote() for _ in range(args.num_requests // args.num_clients)]
	ray.get(refs)

	tend = time.time()
	print("time: ", tend - tstart)
	print("throughput: ", args.num_requests / (tend - tstart))

