import numpy as np
import tensorflow as tf
from tensorflow import keras

def load_data():
    X_train = np.load('data/processed/X_train.npy')
    y_train = np.load('data/processed/y_train.npy')
    X_val = np.load('data/processed/X_val.npy')
    y_val = np.load('data/processed/y_val.npy')
    return X_train, y_train, X_val, y_val

def build_model(input_shape):
    model = keras.Sequential([
        keras.layers.Dense(32, activation='relu', input_shape=(input_shape,)),
        keras.layers.Dense(16, activation='relu'),
        keras.layers.Dense(1)
    ])
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

def train_model(X_train, y_train, X_val, y_val):
    model = build_model(X_train.shape[1])
    model.fit(X_train, y_train, epochs=50, batch_size=32, validation_data=(X_val, y_val))
    model.save('models/air_quality_model.h5')

def main():
    X_train, y_train, X_val, y_val = load_data()
    train_model(X_train, y_train, X_val, y_val)

if __name__ == '__main__':
    main()
