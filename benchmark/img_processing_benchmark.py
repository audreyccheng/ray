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
ray.init(num_gpus=1, ignore_reinit_error=True)

num_clients = 1
num_requests_per_client = 1
num_preprocess_calls = 1
batch_size = 1
failure_percentage = 1

if len(sys.argv) > 1:
    if len(sys.argv) != 5:
        print("Wrong number of arguments");
    else :
        num_clients = int(sys.argv[1]);
        num_requests_per_client = int(sys.argv[1]);
        num_preprocess_calls = int(sys.argv[2]);
        batch_size = int(sys.argv[3]);

@ray.remote
def preprocess(img):
    img = img / 255.0
    return img

@ray.remote
def preprocess2(img):
    return img

# Use an actor to keep the model in GPU memory.
@ray.remote(num_gpus=1, max_restarts=1, max_task_retries=1)
class Worker:
    def __init__(self):
        fashion_mnist = keras.datasets.fashion_mnist
        (train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
        self.model = keras.Sequential([
                        keras.layers.Flatten(input_shape=(28, 28)),
                        keras.layers.Dense(128, activation='relu'),
                        keras.layers.Dense(10)
                    ])
        self.model.compile(optimizer='adam',
              loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
              metrics=['accuracy'])

        self.model.fit(train_images, train_labels, epochs=1) # 10

        self.probability_model = tf.keras.Sequential([self.model, tf.keras.layers.Softmax()])

    def predict(self, batch):
        # The actual work.
        print("work")
        # Fail with probability based on parameter
        rand_int = randrange(100)
        if rand_int < failure_percentage:
            sys.exit()

        # Predict and return results
        results = []
        for img in batch:
            predictions = self.probability_model.predict(ray.get(img))
            for pred in predictions:
                results.append(np.argmax(pred))
        return results

@ray.remote(max_restarts=1, max_task_retries=1)
class Batcher:
    def __init__(self):
        self.buffer = []
        self.results = []

    def request(self, img_refs):
        # print(os.getpid())
        self.buffer.append(img_refs[0])

        print(len(self.buffer))
        print(self.buffer)
        if len(self.buffer) == batch_size:
            print("batch")
            worker = Worker.remote()
            predictions = ray.get(worker.predict.remote(self.buffer))
            self.results.append(predictions)
            self.buffer = []
            # print(predictions)
            

    def get_results(self):
        # print(self.results)
        return self.results


# Async actor to concurrently serve Batcher requests
fashion_mnist = keras.datasets.fashion_mnist
(train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
# print(test_images.shape)
@ray.remote(max_restarts=1, max_task_retries=1)
class Client:
    # One batcher per client
    def __init__(self):
        self.batcher = Batcher.remote()

    async def run_concurrent(self):
        # print("started")
        img_ref = [preprocess.remote(test_images[:1]) for _ in range(num_preprocess_calls)]
        ref = ray.get(self.batcher.request.remote([img_ref]))
        # print("finished")

    def get_results(self):
        results = ray.get(self.batcher.get_results.remote())
        # print(len(results[0]))
        # print(results)
        
client = Client.remote()
# [clients = Client.remote() for _ in range(numClients)]

tstart = process_time()  
ray.get([client.run_concurrent.remote() for _ in range(num_requests_per_client)])
tstop = process_time() 
print("time: ", tstop-tstart)

results = ray.get(client.get_results.remote())
print(results)

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






