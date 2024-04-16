import numpy as np
import tensorflow as tf

def load_model_and_data():
    model = tf.keras.models.load_model('models/air_quality_model.h5')
    X_test = np.load('data/processed/X_test.npy')
    y_test = np.load('data/processed/y_test.npy')
    return model, X_test, y_test

def evaluate_model(model, X_test, y_test):
    loss = model.evaluate(X_test, y_test)
    print("Test Loss:", loss)

def main():
    model, X_test, y_test = load_model_and_data()
    evaluate_model(model, X_test, y_test)

if __name__ == '__main__':
    main()
