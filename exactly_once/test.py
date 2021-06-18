import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
from random import randrange
import ray
import sys
import time
import unittest

from test_decorator import Client, Server, ControlActor


class TestSum(unittest.TestCase):
	def test_at_least_once(self):
		"""
		Test that request is retried if exactly_once is not set
		"""
		ray.init()

		fail = True
		exactly_once = False
		control = ControlActor.remote(fail, 2)
		server = Server.remote(exactly_once, control)
		clients = [Client.remote(i, server) for i in range(1)]

		refs = []
		for client in clients:
			refs += [client.run_op.remote() for _ in range(5 // 1)]

		ray.get(ray.get(refs))

		self.assertEqual(ray.get(control.get_server_count.remote()), 6)

		# ray.shutdown()
		# os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))

	def test_exactly_once(self):
		"""
		Test that request is not retried if exactly_once is set
		"""
		# ray.init()

		fail = True
		exactly_once = True
		control = ControlActor.remote(fail, 2)
		server = Server.remote(exactly_once, control)
		clients = [Client.remote(i, server) for i in range(1)]

		refs = []
		for client in clients:
			refs += [client.run_op.remote() for _ in range(5 // 1)]

		ray.get(ray.get(refs))


		self.assertEqual(len(ray.get(server.get_requests.remote())), 5)
		self.assertEqual(ray.get(control.get_server_count.remote()), 5)

		# ray.shutdown()
		os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))

if __name__ == '__main__':
	unittest.main()
	# ray.init()

	# fail = True
	# exactly_once = True
	# control = ControlActor.remote(fail, 2)
	# server = Server.remote(exactly_once, control)
	# clients = [Client.remote(i, server) for i in range(1)]

	# refs = []
	# for client in clients:
	# 	refs += [client.run_op.remote() for _ in range(5 // 1)]

	# ray.get(ray.get(refs))

	# print("request count: ", len(ray.get(server.get_requests.remote())))
	# print("server count: ", ray.get(control.get_server_count.remote()))
	# self.assertEqual(ray.get(control.get_server_count.remote()), 4)

	# os.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server_checkpoint.txt"))

# python3 -m unittest test 