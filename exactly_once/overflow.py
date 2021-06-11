
				# restore actor private state
				# try:
				# 	f = open(server_path, 'r')
				# 	for line in f:
				# 		vals = line.rstrip().split(" ")
				# 		assert(len(vals) == 2)
				# 		self.restore_state(vals[1])
				# 		break
				# 		# print("recovered value: {}", vals[1])
				# 	f.close()
				# except FileNotFoundError:
				# 	# print('Private server checkpoint does not exist')
				# 	pass

							# if request_id in wrapped_fn.requests:
			# 	return wrapped_fn.requests[request_id]


									# print("recovered request id {} value {}", vals[0], vals[1])

														# print('Server checkpoint does not exist')



				# if checkpoint_freq > 1 and (self.request_count % checkpoint_freq) == 0:
				# 	# save private actor state
				# 	fp = open(server_path, "w")
				# 	for i in range(0,100):
				# 		fp.write(str(request_id) + " " + self.get_state() + "\n")
				# 	fp.close()

			# print("wrote to files {} {}", path, server_path)
			# time.sleep(3)
		# wrapped_fn.request_id = 0
		# wrapped_fn.requests = {}


	# def serialize_method_value(self, value):
	# 	return str(value)

	# def deserialize_method_value(self, value):
	# 	return float(value)

				# print("getting latencies ")
			# print(self.server.get_latencies.remote())
			# print(ray.get(self.server.get_latencies.remote()))
			# self.latencies += ray.get(self.server.get_latencies.remote())
			# print(self.latencies)
			# os.kill(pid, signal.SIGKILL)

			
	# def set_server(self, server):
	# 	# print("server now set ", server)
	# 	self.server = server
		# print("getting latencies ")
		# print(self.server.get_latencies.remote())
		# print(ray.get(self.server.get_latencies.remote()))
