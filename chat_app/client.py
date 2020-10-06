#!/usr/bin/env python

import asyncio
import datetime
import numpy as np
import ray
import time
import websockets

URI = 'ws://localhost:8080/'

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
	
	parser.add_argument("--num-clients", default=2, type=int)
	parser.add_argument("--num-requests", default=20, type=int)
	args = parser.parse_args()
	parser = argparse.ArgumentParser()

	ray.init()

	clients = [ChatClient.remote(URI, "name", "chat_name") for _ in range(args.num_clients)]

	tstart = time.time()
	refs = []
	num_requests = args.num_requests // args.num_clients
	for client in clients:
		refs += [client.send_requests.remote(num_requests)]
	ray.get(refs)
	tstop = time.time()
	print("time: ", tstop-tstart)
	print("throughput: ", args.num_requests / (tstop-tstart))




