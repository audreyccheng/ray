import asyncio
import cv2
import numpy as np
import os
import ray
from random import randrange
import sys
from tensorflow import keras
import tensorflow as tf
from time import process_time 
import time
import signal

from keras.preprocessing.image import load_img 
from keras.preprocessing.image import img_to_array 
from keras.applications.imagenet_utils import decode_predictions 
import matplotlib.pyplot as plt 
from keras.applications.resnet50 import ResNet50 
from keras.applications import resnet50
from PIL import Image
num_clients = 1
num_requests_per_client = 100
num_preprocess_calls = 1
batch_size = 10
failure_percentage = 1

@ray.remote
def preprocess(img):
    numpy_image = img_to_array(img) 
    image_batch = np.expand_dims(numpy_image, axis = 0) 
    processed_img = resnet50.preprocess_input(image_batch.copy())
    return processed_img

@ray.remote
def preprocess2(img):
    return img

# Use an actor to keep the model in GPU memory.
@ray.remote(num_cpus=0, num_gpus=0) #, max_restarts=-1, max_task_retries=-1)
class Worker:
    def __init__(self, worker_fp):
        self.worker_fp = worker_fp
        self.model = resnet50.ResNet50(input_shape=(224, 224, 3),weights = 'imagenet')

    def start(self):
        empty_img = np.empty([1, 224, 224, 3])
        predictions = self.model.predict(empty_img)
        print("worker alive")

    def predict(self, *batch):
        # The actual work.
        # print("work")
        # Fail with probability based on parameter
        rand = np.random.rand()
        if rand < self.worker_fp:
            sys.exit(-1)

        # Predict and return results
        #img_batch = np.array(batch).reshape(len(batch),224, 224,3)
        #predictions = self.model.predict(img_batch) 
        predictions = []
        for i in range(len(batch)):
            predictions.append(1)
        time.sleep(0.1)
        return predictions

    def predict2(self, *batch):
        #print("work")
        rand = np.random.rand() 
        if rand < self.worker_fp:
        #    print("worker failed")
            sys.exit()
        time.sleep(0.1)
        return batch

@ray.remote #(max_restarts=-1, max_task_retries=-1)
class Batcher:
    def __init__(self, batch_size, batcher_fp, worker_fp, pass_by_value, num_workers, worker_resource):
        self.batcher_fp = batcher_fp
        self.workers = []
        for i in range(num_workers):
            self.workers.append(Worker.options(resources={worker_resource: 1}).remote(worker_fp))
        self.batch_size = batch_size
        # self.queue = asyncio.Queue(maxsize=batch_size)
        self.queue = []
        self.pass_by_value = pass_by_value
        self.predictions = []
        # print("batch size", batch_size)
        # self.results = []
        # self.futures = []

    def start(self):
        print("batcher alive")
        ray.get([worker.start.remote() for worker in self.workers])

    def request(self, img_refs):
        # print("future ", img_refs[0])
        # loop = asyncio.get_event_loop()
        # fut = loop.create_future()
        # self.futures.append(fut)

        # print("batch put", img_refs[0])
        if self.pass_by_value:
            self.queue.append(img_refs)
            # await self.queue.put(img_refs)
        else:
            self.queue.append(img_refs[0])
            # await self.queue.put(img_refs[0])
        # print(self.queue.full())
        # print("put done")
        # print(self.queue.qsize())

        # if self.queue.qsize() == self.batch_size:
        if len(self.queue) == self.batch_size:
            # print("batch")
            batch = []
            # futures = []
            for i in range(self.batch_size):
                batch.append(self.queue.pop(0))
                # futures.append(self.futures.pop(0))
            #for b in batch:
                #print("job ", b)
            tmp_worker = self.workers.pop(0)
            self.workers.append(tmp_worker)
            self.predictions.append(self.workers[0].predict.remote(*batch))
            # for pred, future in zip(predictions, futures):
            #     #print("result ", pred)
            #     future.set_result(pred)
            return
            # self.futures.clear()
            # return predictions[len(predictions) - 1]
        # else:

        rand = np.random.rand() 
        if rand < self.batcher_fp:
          #  print("batcher failed")
            pid = os.getpid()
            os.kill(pid, signal.SIGKILL)
        return
        # return await fut
    def complete_requests(self):
        results = ray.get(self.predictions)
        return results


# Async actor to concurrently serve Batcher requests
#fashion_mnist = keras.datasets.fashion_mnist
#(train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
# print(test_images.shape)
@ray.remote #(max_restarts=1, max_task_retries=1)
class Client:
    # One batcher per client
    def __init__(self, batcher, pass_by_value, worker_resource):
        self.batcher = batcher
        self.pass_by_value = pass_by_value
        self.worker_resource = worker_resource
        self.img = load_img('/home/ubuntu/ray/benchmarks/img.jpg', target_size = (224, 224))

    def start(self):
        print("client alive")
        ray.get(self.batcher.start.remote())

    def run_concurrent(self):
        # print("started")
        img_ref = preprocess.options(resources={self.worker_resource: 1}).remote(self.img)
        # rand_int = randrange(100) 
        if self.pass_by_value:
            ref = ray.get(self.batcher.request.remote(img_ref))
        else:
            ref = ray.get(self.batcher.request.remote([img_ref]))
        return
            #print("ref ", ref)
            #print("job ", job_int)
            #assert(ref == job_int)
        # print("finished")

    def get_results(self):
        print("getting results")
        results = ray.get(self.batcher.complete_requests.remote())
        return results
        

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--num-total-requests", required=True, type=int)
    parser.add_argument("--num-preprocessors", required=True, type=int)
    parser.add_argument("--num-clients", required=True, type=int)
    parser.add_argument("--num-batchers", required=True, type=int)
    parser.add_argument("--num-workers", required=True, type=int)
    parser.add_argument("--batch-size", required=True, type=int)
    parser.add_argument("--num-preprocessors-per-node", default=1, type=int)
    parser.add_argument("--num-clients-per-node", default=1, type=int)
    parser.add_argument("--num-batchers-per-node", default=1, type=int)
    parser.add_argument("--num-workers-per-node", default=1, type=int)
    parser.add_argument("--batcher-fp", default=0, type=float)
    parser.add_argument("--worker-fp", default=0, type=float)
    parser.add_argument("--pass-by-value", action="store_true")
    args = parser.parse_args()

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
    # assert len(nodes) >= len(video_resources) + num_owner_nodes, ("Found {} nodes, need {}".format(len(nodes), len(video_resources) + num_owner_nodes))

    preprocessor_resources = ["preprocessor:{}".format(i) for i in range(
        args.num_preprocessors // args.num_preprocessors_per_node)]
    client_resources = ["client:{}".format(i) for i in range(args.num_clients // args.num_clients_per_node)]
    batcher_resources = ["batcher:{}".format(i) for i in range(args.num_batchers // args.num_batchers_per_node)]
    worker_resources = ["worker:{}".format(i) for i in range(args.num_batchers)]
    worker_ip = None
    worker_resource = None
    owner_ip = None
    owner_resource = None
    node_index = 0
    # Assign clients and batchers to head node
    # for resource in (client_resources + batcher_resources):
    #     if "CPU" not in head_node[0]["Resources"]:
    #         continue

    #     print("Assigning", resource, "to node", head_node[0]["NodeID"], head_node[0]["Resources"])
    #     ray.experimental.set_resource(resource, 100, head_node[0]["NodeID"])

    # Assign worker nodes
    assert len(nodes) >= len(worker_resources)
    for node, resource in zip(nodes, worker_resources):
        if "CPU" not in node["Resources"]:
            continue

        print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
        ray.experimental.set_resource(resource, 100, node["NodeID"])

    for i in range(len(client_resources)):
        resource = client_resources[i]
        node = nodes[i % args.num_batchers]
        if "CPU" not in node["Resources"]:
            continue

        print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
        ray.experimental.set_resource(resource, 100, node["NodeID"])

    for i in range(len(batcher_resources)):
        resource = batcher_resources[i]
        node = nodes[i % args.num_batchers]
        if "CPU" not in node["Resources"]:
            continue

        print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
        ray.experimental.set_resource(resource, 100, node["NodeID"])

    batchers = [Batcher.options(resources={batcher_resources[i % len(batcher_resources)]: 1}).remote(
        args.batch_size, args.batcher_fp, args.worker_fp, args.pass_by_value, (args.num_workers // args.num_batchers),
        worker_resources[i % args.num_batchers]) for i  in range(args.num_batchers)]
    #client = Client.remote()
    #preprocessors = [Preprocess.options(resources={client_resources[i % len(client_resources)]: 1}).remote()]
    clients = [Client.options(resources={client_resources[i % len(client_resources)]: 1}).remote(
        batchers[i % args.num_batchers], args.pass_by_value,
        worker_resources[i % args.num_batchers]) for i in range(args.num_clients)]
    #[clients = Client.remote() for _ in range(numClients)]

    # Start up clients
    for client in clients:
        ray.get(client.start.remote())

    # Measure throughput
    #tstart = process_time()
    tstart = time.time()
    results = []
    for client in clients:
        [client.run_concurrent.remote() for _ in range(args.num_total_requests // args.num_clients)]
        # for _ in range(args.num_total_requests):
        #     ray.get(client.run_concurrent.remote())

    results = ray.get(clients[0].get_results.remote())
        # results += [client.run_concurrent.remote() for _ in range(args.num_total_requests // args.num_clients)]
    # ray.get(results)
    #tstop = process_time() 
    tstop = time.time()
    print("time: ", tstop-tstart)
    print("throughput: ", args.num_total_requests / (tstop-tstart))

# results = ray.get(client.get_results.remote())
# print(results)

# `ObjectRef`
# img_ref = [preprocess.remote(...) for _ in range(100)]
# b = Batcher.remote()
# # Sends the object ref, but the batcher actor never sees the image data.
# ref = b.request.remote([img_ref])

# Code TODO:
# - Add a `Client` actor that will drive the benchmark. We should be able to
# specify some target request load (in terms of total requests or requests/s),
# and then the program should start as many of these client actors as needed to
# create that load.
# - Return the Worker.foo response from Batcher.request
#   - Can use async actors to allow concurrently serving other
#   Batcher.requests.
# - Test it on your laptop.
# - Try a failure experiment (by killing the batcher and/or worker actor's
# PID).

# Failure experiment where task will work after one restart and will
# fail after subsequent restarts
# for _ in range(2):
#     try:
#         ray.get(client.run_concurrent.remote())
#     except ray.exceptions.RayActorError:
#         print('FAILURE')

