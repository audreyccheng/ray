import asyncio
import cv2
import os
import ray
import sys
import time
ray.init(num_gpus=1, ignore_reinit_error=True)

numClients = 1
numRequestsPerClient = 1 #3
numPreprocessCalls = 1 #10
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
    # resize image
    height = 220
    width = 220
    dim = (width, height)
    img_resized = cv2.resize(img, dim, interpolation=cv2.INTER_LINEAR)
    print("RESIZED", img_resized.shape)

    # remove noise from image
    no_noise = []
    for i in range(len(img_resized)):
        blur = cv2.GaussianBlur(img_resized[i], (5, 5), 0)
        no_noise.append(blur)
    img_no_noise = no_noise[1]

    # segment image
    # img_gray = cv2.cvtColor(img_no_noise, cv2.COLOR_RGB2GRAY)
    # ret, img_thresh = cv2.threshold(img_gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # Further noise removal
    # kernel = np.ones((3, 3), np.uint8)
    # opening = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel, iterations=2)

    # sure background area
    # sure_bg = cv2.dilate(opening, kernel, iterations=3)

    # # Finding sure foreground area
    # dist_transform = cv2.distanceTransform(opening, cv2.DIST_L2, 5)
    # ret, sure_fg = cv2.threshold(dist_transform, 0.7 * dist_transform.max(), 255, 0)

    # # Finding unknown region
    # sure_fg = np.uint8(sure_fg)
    # unknown = cv2.subtract(sure_bg, sure_fg)
    
    return img_no_noise

@ray.remote
def preprocess2(img):
    return img

# Use an actor to keep the model in GPU memory.
@ray.remote(num_gpus=1, max_restarts=1, max_task_retries=1)
class Worker:
    def __init__(self):
        pass

    def foo(self, batch):
        # The actual work.
        print("work")
        # print(os.getpid())
        time.sleep(5)
        return

@ray.remote(max_restarts=1, max_task_retries=1)
class Batcher:
    def __init__(self):
        self.buffer = []

    def request(self, img_refs):
        # print(os.getpid())
        self.buffer.append(img_refs[0])

        if len(self.buffer) == numWorkersPerBatcher:
            worker = Worker.remote()
            ray.get(worker.foo.remote(*self.buffer))
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
img = cv2.imread('/Users/audrey/ray_notebooks/img.png')
@ray.remote(max_restarts=1, max_task_retries=1)
class Client:
    # One batcher per client
    def __init__(self):
        self.batcher = Batcher.remote()

    async def run_concurrent(self):
        print("started")
        time.sleep(1)
        img_ref = [preprocess.remote(img) for _ in range(numPreprocessCalls)]
        ref = ray.get(self.batcher.request.remote([img_ref]))
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







