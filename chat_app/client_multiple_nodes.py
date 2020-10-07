#!/usr/bin/env python

import asyncio
import datetime
import numpy as np
import ray
import time
import websockets

URI = 'ws://172.31.22.176:8080/'

@ray.remote
class ChatClient(object):
	def __init__(self, uri, name, chat_name):
		self.uri = uri
		self.name = name
		self.chat_name = chat_name

	async def send_requests(self, num_requests):
		async with websockets.connect(self.uri) as websocket:
			# Send name and chat_name to server
			await websocket.send(self.name)
			await websocket.send(self.chat_name)
			# Receive welcome messages
			print(await websocket.recv())
			print(await websocket.recv())
			print(await websocket.recv())
			print(await websocket.recv())

			for _ in range(num_requests):
				time = str(int(datetime.datetime.now().timestamp()*1000))
				message = time + ' ' + self.name
				await websocket.send(message)

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run clients for chat app.")
	
	parser.add_argument("--num-nodes", required=True, type=int)
	parser.add_argument("--num-clients", default=2, type=int)
	parser.add_argument("--num-clients-per-node", default=1, type=int)
	parser.add_argument("--num-requests", default=20, type=int)
	parser.add_argument("--chat-name", required=True, type=str)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init(address='172.31.22.176:6379') # Address of head node

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
			if resource.startswith("client") or resource.startswith("batcher") or resource.startswith("worker"):
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

	assert len(nodes) >= len(client_resources)
	for node, resource in zip(nodes, client_resources):
		if "CPU" not in node["Resources"]:
			continue

		print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
		ray.experimental.set_resource(resource, 100, node["NodeID"])

	clients = [ChatClient.options(resources={client_resources[i % len(client_resources)]: 1}).remote(
		URI, "name", args.chat_name) for i in range(args.num_clients)]

	tstart = time.time()
	refs = []
	num_requests = args.num_requests // args.num_clients
	for client in clients:
		refs += [client.send_requests.remote(num_requests)]
	ray.get(refs)
	tstop = time.time()
	print("time: ", tstop-tstart)
	print("throughput: ", args.num_requests / (tstop-tstart))




