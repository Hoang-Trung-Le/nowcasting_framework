import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def generate_sample_data():
    X = np.random.rand(1000, 10)  # 1000 samples, 10 features
    y = np.random.rand(1000, 1)  # 1000 samples, 1 target variable
    return X, y


def preprocess_data(X, y):
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.5, random_state=42
    )
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)
    return X_train_scaled, y_train, X_val_scaled, y_val, X_test_scaled, y_test


def save_data(X_train_scaled, y_train, X_val_scaled, y_val, X_test_scaled, y_test):
    np.save("data/processed/X_train.npy", X_train_scaled)
    np.save("data/processed/y_train.npy", y_train)
    np.save("data/processed/X_val.npy", X_val_scaled)
    np.save("data/processed/y_val.npy", y_val)
    np.save("data/processed/X_test.npy", X_test_scaled)
    np.save("data/processed/y_test.npy", y_test)


def main():
    X, y = generate_sample_data()
    X_train_scaled, y_train, X_val_scaled, y_val, X_test_scaled, y_test = (
        preprocess_data(X, y)
    )
    save_data(X_train_scaled, y_train, X_val_scaled, y_val, X_test_scaled, y_test)


if __name__ == "__main__":
    main()
