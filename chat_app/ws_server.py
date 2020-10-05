#!/usr/bin/env python

# WS server example

import asyncio
import ray
import websockets
import kv_store_async as kv_store

async def hello(websocket, path):
    name = await websocket.recv()
    print(f"< {name}")

    greeting = f"Hello {name}!"

    await websocket.send(greeting)
    print(f"> {greeting}")

if __name__ == "__main__":
	ray.init()

	start_server = websockets.serve(hello, "localhost", 8765)

	kv_store_server = kv_store.Server.options(name="CounterActor", lifetime="detached", max_concurrency=10).remote() 

	asyncio.get_event_loop().run_until_complete(start_server)
	asyncio.get_event_loop().run_forever()