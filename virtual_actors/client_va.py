import asyncio
import datetime
import numpy as np
import ray
import sys
import time
import websockets

# sys.path.append('../virtual_actors')
import virtual_actors_master as va

class ChatGroup(object):
	def __init__(self, chat_group_name):
		self.chat_group_name = chat_group_name
		# Store name of clients as strings
		self.clients = []
		# Pass in name of master of chat clients
		self.va_client = va.Client("chat_clients")

	def send_chat(self, chat, sending_client):
		if not sending_client in self.clients:
			self.clients.append(sending_client)
		for client in self.clients:
			self.va_client.push_chat(client, "chat")
			# client.send_request.remote("recieve_chat", chat)
			# client.recieve_chat.remote(chat)

class ChatClient(object):
	def __init__(self, client_name):
		self.client_name = client_name
		self.chat = ""
		# Pass in name of master of chat groups
		self.va_client = va.Client("chat_rooms")

	def send_chat_group(self, chat_group, chat):
		# self.chat += chat
		self.va_client.send_chat(chat_group, chat, self.client_name)
		# self.chat_group.send_request.remote("send_chat", chat, self)
		# return chat

	def push_chat(self, chat):
		self.chat += chat

	def get_chat(self):
		return self.chat



if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Run clients for chat app.")
	
	# parser.add_argument("--num-chats", default=1, type=int)
	parser.add_argument("--num-clients", default=1, type=int)
	parser.add_argument("--num-requests", default=1000, type=int)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	chat_room_master = va.VirtualActorGroup.options(
		name="chat_rooms", lifetime="detached").remote(1, ChatGroup)

	chat_client_master = va.VirtualActorGroup.options(
		name="chat_clients", lifetime="detached").remote(1, ChatClient)

	# Name of chat clients
	clients = ["client" + str(i) for i in range(args.num_clients)]

	# Start virtual actor client to make requests
	chat_clients = va.Client("chat_clients")
	# Make sure all actors have been started
	ray.get([chat_clients.send_chat_group(client, "group", "chat" + client) for client in clients])
	tstart = time.time()
	reqs = []
	num_requests = args.num_requests // args.num_clients
	for _ in range(num_requests):
		reqs += [chat_clients.send_chat_group(client, "group", "chat" + client) for client in clients]
	ray.get(reqs)
	tstop = time.time()
	# time.sleep(5)
	# print(ray.get([chat_clients.get_chat(client) for client in clients]))
	
	print("time: ", tstop-tstart)
	print("throughput: ", args.num_requests / (tstop-tstart))

	# ray.get(virtual_actor_group.close.remote())
