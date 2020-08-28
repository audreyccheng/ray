from ray import serve

import os
import numpy as np
import requests

TRAINED_MODEL_PATH = "/tmp/mnist_model.h5"


def train_and_save_model():
    import tensorflow as tf
    # Load mnist dataset
    mnist = tf.keras.datasets.mnist
    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train, x_test = x_train / 255.0, x_test / 255.0

    # Train a simple neural net model
    model = tf.keras.models.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(128, activation="relu"),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(10)
    ])
    loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
    model.compile(optimizer="adam", loss=loss_fn, metrics=["accuracy"])
    model.fit(x_train, y_train, epochs=1)

    model.evaluate(x_test, y_test, verbose=2)
    model.summary()

    # Save the model in h5 format in local file system
    model.save(TRAINED_MODEL_PATH)


if not os.path.exists(TRAINED_MODEL_PATH):
    train_and_save_model()

class TFMnistModel:
    def __init__(self, model_path):
        import tensorflow as tf
        self.model_path = model_path
        self.model = tf.keras.models.load_model(model_path)

    def __call__(self, flask_request):
        # Step 1: transform HTTP request -> tensorflow input
        # Here we define the request schema to be a json array.
        input_array = np.array(flask_request.json["array"])
        reshaped_array = input_array.reshape((1, 28, 28))

        # Step 2: tensorflow input -> tensorflow output
        prediction = self.model(reshaped_array)

        # Step 3: tensorflow output -> web output
        return {
            "prediction": prediction.numpy().tolist(),
            "file": self.model_path
        }

serve.init()
serve.create_backend("tf:v1", TFMnistModel, "/tmp/mnist_model.h5")
serve.create_endpoint("tf_classifier", backend="tf:v1", route="/mnist")

resp = requests.get(
    "http://localhost:8000/mnist",
    json={"array": np.random.randn(28 * 28).tolist()})
print(resp.json())
