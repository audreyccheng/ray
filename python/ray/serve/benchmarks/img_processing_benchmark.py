import asyncio
import ray
import sys
import time
ray.init(num_gpus=1, ignore_reinit_error=True)

numClients = 1;
numRequestsPerClient = 3;
numPreprocessCalls = 10;
numWorkersPerBatcher = 1

if len(sys.argv) > 1:
    if len(sys.argv) != 5:
        print("Wrong number of arguments");
    else :
        numClients = int(sys.argv[1]);
        numRequestsPerClient = int(sys.argv[1]);
        numWorkersPerBatcher = int(sys.argv[2]);
        numWorkersPerBatcher = int(sys.argv[3]);

@ray.remote
def preprocess(img):
    return img

@ray.remote
def preprocess2(img):
    return img

# Use an actor to keep the model in GPU memory.
@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        pass

    def foo(self, batch):
        # The actual work.
        print("work")
        time.sleep(50)
        return

@ray.remote
class Batcher:
    def __init__(self):
        self.buffer = []

    def request(self, img_refs):
        self.buffer.append(img_refs[0])

        if len(self.buffer) == numWorkersPerBatcher:
            worker = Worker.remote()
            worker.foo.remote(*self.buffer)
            self.buffer = []


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

# Async actor to concurrently serve Batcher requests
@ray.remote(max_restarts=1, max_task_retries=1)
class Client:
    # One batcher per client
    def __init__(self):
        self.batcher = Batcher.remote()

    async def run_concurrent(self):
        print("started")
        time.sleep(1)
        img_ref = [preprocess.remote(...) for _ in range(numPreprocessCalls)]
        ref = self.batcher.request.remote([img_ref])
        print("finished")

client = Client.remote()
# [clients = Client.remote() for _ in range(numClients)]

ray.get([client.run_concurrent.remote() for _ in range(numRequestsPerClient)])

# Failure experiment where task will work after one restart and will
# fail after subsequent restarts
# for _ in range(2):
#     try:
#         ray.get(client.run_concurrent.remote())
#     except ray.exceptions.RayActorError:
#         print('FAILURE')






