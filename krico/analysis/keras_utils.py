import os
import keras


def set_backend(backend):
    if keras.backend.backend() != backend:
        os.environ['KERAS_BACKEND'] = str(backend)
        reload(keras)
        assert keras.backend.backend() == backend
