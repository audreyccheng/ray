import json

import torch
import torchvision.transforms as transforms
import torchvision.datasets as datasets
from PIL import Image

from alexnet_pytorch import AlexNet

# Open image
input_image = Image.open("img.jpg")
input_image2 = Image.open("img.jpg")

# Preprocess image
preprocess = transforms.Compose([
  transforms.Resize(256),
  transforms.CenterCrop(224),
  transforms.ToTensor(),
  transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])
input_tensor = preprocess(input_image)
input_batch = input_tensor.unsqueeze(0)  # create a mini-batch as expected by the model

train_dataset = datasets.ImageFolder(
    ".",
    transform=transforms.Compose([
  	transforms.Resize(256),
  	transforms.CenterCrop(224),
  	transforms.ToTensor(),
  	transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
	]))


# # Load class names
# labels_map = json.load(open("labels_map.txt"))
# labels_map = [labels_map[str(i)] for i in range(1000)]

# # Classify with AlexNet
# model = AlexNet.from_pretrained("alexnet")
# model.eval()

# # move the input and model to GPU for speed if available
# if torch.cuda.is_available():
#   input_batch = input_batch.to("cuda")
#   model.to("cuda")

# with torch.no_grad():
#   logits = model(input_batch)
# # print(torch.topk(logits, k=5))
# preds = torch.topk(logits, k=5).indices.squeeze(0).tolist()

# print("-----")
# for idx in preds:
#   label = labels_map[idx]
#   prob = torch.softmax(logits, dim=1)[0, idx].item()
#   print(idx)
#   print(f"{label:<75} ({prob * 100:.2f}%)")