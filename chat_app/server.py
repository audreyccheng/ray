#!/usr/bin/env python3

import asyncio
import datetime
import ray
import sys
import websockets

ray.init()
sys.path.insert(1, '../kv_store')
import kv_store_replication_sn as kv_store

# The set of clients connected to this server
chats = {} #: { chat_name: {websocket: name}}
LISTEN_ADDRESS = ('0.0.0.0', 8080)
backup_server = kv_store.BackupServer.options(max_concurrency=1).remote()
kv_store_server = kv_store.Server.options(
	name="ServerActor", lifetime="detached", max_concurrency=10).remote(backup_server, 0)

def timestamp_to_float(ts):
	return int(datetime.datetime.fromtimestamp(float(ts)/1000.0).timestamp()*1000)

async def client_handler(websocket, path):
	print('New client', websocket)
	print(' ({} existing chats)'.format(len(chats)))

	# The first two lines from the client are the name and chat name
	name = await websocket.recv()
	chat_name = await websocket.recv()
	kv_server = ray.get_actor("ServerActor")
	log = ray.get(kv_server.get.remote(chat_name))

	if not chat_name in chats:
		chats[chat_name] = {}

	await websocket.send('Welcome to the chat session {}, {}'.format(chat_name, name))
	await websocket.send('There are {} other users connected: {}'.format(
		len(chats[chat_name]),list(chats[chat_name].values())))
	await websocket.send('Previous messages: {}'.format(log))

	chats[chat_name][websocket] = name
	for client, _ in chats[chat_name].items():
		await client.send(name + ' has joined the chat')

	# Handle messages from this client
	while True:
		try:
			message = await websocket.recv()
			timestamp = await websocket.recv()
			dt = timestamp_to_float(timestamp)
			ray.get(kv_server.put.remote(name, message))
			ray.get(kv_server.put.remote(chat_name, '<br>{}: {}'.format(name, message)))
			val = ray.get(kv_server.get.remote(chat_name))

			# Send message to all clients
			for client, _ in chats[chat_name].items():
				await client.send('{}: {}'.format(name, message))
				# await client.send('{}: val {} ts {}'.format(name, val, dt))
		except:
			# Remove client from chat_name group
			their_name = chats[chat_name][websocket]
			del chats[chat_name][websocket]
			print('Client closed connection', websocket)
			for client, _ in chats[chat_name].items():
				await client.send(their_name + ' has left the chat')


start_server = websockets.serve(client_handler, *LISTEN_ADDRESS, ping_interval=None)


asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
