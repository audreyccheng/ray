#!/usr/bin/env python3

import asyncio
import datetime
import ray
import sys
import websockets

ray.init()
sys.path.insert(1, '../kv_store')
import kv_store_replication_sn as kv_store


chats = {} # {chat_name: {websocket: name}}
PORT = 8080
LISTEN_ADDRESS = ('0.0.0.0', PORT)
backup_server = kv_store.BackupServer.options(max_concurrency=1).remote()
kv_store_server = kv_store.Server.options(
	name="ServerActor", lifetime="detached", max_concurrency=10).remote(backup_server, 0)
kv_server = ray.get_actor("ServerActor") # populate to global variable for this particular webserver
# Store the set of clients {chat_name: {websocket: name}} connected to this server in kv-store
# ray.get(kv_server.put.remote(PORT, {}))

async def client_handler(websocket, path, **kwargs):
	print('New client', websocket)
	print(' ({} existing chats)'.format(len(chats)))

	# The first two lines from the client are the name and chat name
	name = await websocket.recv()
	chat_name = await websocket.recv()
	log = await kv_server.get.remote(chat_name)

	if not chat_name in chats:
		chats[chat_name] = {}

	await websocket.send('Welcome to the chat session {}, {}'.format(chat_name, name))
	await websocket.send('There are {} other users connected: {}'.format(
		len(chats[chat_name]),list(chats[chat_name].values())))
	await websocket.send('Previous messages: {}'.format(log))

	# Notify all clients in this chat of new client
	chats[chat_name][websocket] = name
	await asyncio.wait([client.send(name + ' has joined the chat') for client, _ in chats[chat_name].items()])

	# Handle messages from this client
	while True:
		try:
			message = await websocket.recv()
			await kv_server.put.remote(chat_name, '<br>{}: {}'.format(name, message))

			# Send message to all clients
			await asyncio.wait([client.send('{}: {}'.format(name, message)) for client, _ in chats[chat_name].items()])
			# await asyncio.wait([client.send('{}: {} - {}'.format(name, timestamp, dt)) for client, _ in chats[chat_name].items()])

		except:
			# Remove client from chat_name group
			their_name = chats[chat_name][websocket]
			del chats[chat_name][websocket]
			print('Client closed connection', websocket)
			for client, _ in chats[chat_name].items():
				await client.send(their_name + ' has left the chat')


start_server = websockets.serve(client_handler, *LISTEN_ADDRESS)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
