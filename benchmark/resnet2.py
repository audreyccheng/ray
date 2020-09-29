# import PIL 
from keras.preprocessing.image import load_img 
from keras.preprocessing.image import img_to_array 
from keras.applications.imagenet_utils import decode_predictions 
import matplotlib.pyplot as plt 
import numpy as np 
from keras.applications.resnet50 import ResNet50 
from keras.applications import resnet50
import os
import time
from PIL import Image

# from keras.applications.resnet34 import ResNet34
# from keras.applications import resnet34

filename = 'img.jpg' 
original = load_img(filename, target_size = (224, 224)) 
print('PIL image size',original.size)

numpy_image = img_to_array(original) 
image_batch = np.expand_dims(numpy_image, axis = 0) 
processed_image = resnet50.preprocess_input(image_batch.copy())
print('image batch size', image_batch.shape) 


filename2 = 'img-copy.jpg' 
original2 = load_img(filename2, target_size = (224, 224)) 
print('PIL image size',original2.size)
numpy_image2 = img_to_array(original2) 
image_batch2 = np.expand_dims(numpy_image2, axis = 0) 
processed_image2 = resnet50.preprocess_input(image_batch2.copy())

filename3 = 'img-copy.jpg' 
original3 = load_img(filename3, target_size = (224, 224)) 
print('PIL image size',original3.size)
numpy_image3 = img_to_array(original3) 
image_batch3 = np.expand_dims(numpy_image3, axis = 0) 
processed_image3 = resnet50.preprocess_input(image_batch3.copy())

# x_batch = []
# x_batch.append(processed_image)
# x_batch.append(processed_image2)
x = [processed_image,processed_image2,processed_image3]
x_batch = np.array(x).reshape(3,224, 224,3)

# img1 = Image.open("img.jpg").resize((224, 224), Image.ANTIALIAS)
# img2 = Image.open("img-copy.jpg").resize((224, 224), Image.ANTIALIAS)
# imgs = np.array([np.array(img1), np.array(img2)]).reshape(-1,224, 224,1)

# resnet_model = ResNet50(weights='imagenet')
# predictions = resnet_model.predict(imgs) 
# print(predictions)
# imgs = np.array([np.array(Image.open("~/Users/audrey/ray_notebooks/ray/benchmark/imgs" + fname)
#                           .resize((197, 197), Image.ANTIALIAS)) for fname in
#                  os.listdir("~/Users/audrey/ray_notebooks/ray/benchmark/imgs")]).reshape(-1,197,197,1)

resnet_model = resnet50.ResNet50(input_shape=(224, 224, 3),weights = 'imagenet')
tic = time.clock()
predictions = resnet_model.predict(x_batch) 
print(predictions)
toc = time.clock()
print(toc - tic)


label = decode_predictions(predictions) 
print(label)
