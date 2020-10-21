import asyncio
import datetime
import numpy as np
import ray
import sys
import time
import websockets

@ray.remote
class ChatGroup(object):
	def __init__(self, chat_group_name):
		self.chat_group_name = chat_group_name
		self.clients = []

	def add_client(self, client):
		self.clients.append(client)

	def send_chat(self, chat, sending_client):
		# refs = []
		# for client in self.clients:
		# 	if client != sending_client:
		# 		refs += [client.recieve_chat.remote(chat)]
		# ray.get(refs)
		# [client.recieve_chat.remote(chat) for client in self.clients]
		for client in self.clients:
			client.recieve_chat.remote(chat)

@ray.remote
class ChatClient(object):
	def __init__(self, name, chat_group):
		self.name = name
		self.chat_group = chat_group
		self.chat = ""

	def send_chat(self, chat):
		self.chat += chat
		self.chat_group.send_chat.remote(chat, self)
		# return chat

	def recieve_chat(self, chat):
		self.chat += chat



if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run clients for chat app.")
	
	parser.add_argument("--num-nodes", required=True, type=int)
	parser.add_argument("--num-chats", default=1, type=int)
	parser.add_argument("--num-clients", default=2, type=int)
	parser.add_argument("--num-requests", default=5000, type=int)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init(address="auto", ignore_reinit_error=True)

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

	client_resources = ["client:{}".format(i) for i in range(args.num_clients)]
	chat_resources = ["client:{}".format(i) for i in range(args.num_chats)]

	assert len(nodes) >= len(client_resources) + len(chat_resources)
	for node, resource in zip(nodes, client_resources + chat_resources):
		if "CPU" not in node["Resources"]:
			continue

		print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
		ray.experimental.set_resource(resource, 100, node["NodeID"])


	# virtual_actor_group = ray.get_actor("VirtualActorGroup")
	# virtual_actor_group = va.VirtualActorGroup.options(
	# 	name="VirtualActorGroup", lifetime="detached").remote(2)

	chat_group = ChatGroup.options(max_concurrency=10, resources={chat_resources[0 % len(chat_resources)]: 1}).remote("chat_group")
	chat_clients = [ChatClient.options(max_concurrency=10, resources={client_resources[i % len(client_resources)]: 1}).remote(
		"client-" + str(i), chat_group) for i in range(args.num_clients)]
	# chat_group = va.Client.options(max_concurrency=1).remote(virtual_actor_group, ChatGroup)
	# chat_clients = [va.Client.options(max_concurrency=1).remote(virtual_actor_group, ChatClient) for _ in range(args.num_clients)]

	# ray.get(chat_group.send_request.remote("setup", "chat_group"))
	# ray.get([client.send_request.remote("setup", "client", chat_group) for client in chat_clients])


	tstart = time.time()
	refs = []
	num_requests = args.num_requests // args.num_clients
	for client in chat_clients:
		for _ in range(num_requests):
			refs += [client.send_chat.remote("hello")]
	ray.get(refs)
	tstop = time.time()
	print("time: ", tstop-tstart)
	print("throughput: ", args.num_requests / (tstop-tstart))

	# ray.get(virtual_actor_group.close.remote())
