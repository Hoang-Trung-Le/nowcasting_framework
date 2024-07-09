# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import tensorflow as tf
from tensorflow import keras
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, GRU, Conv1D, MaxPooling1D, Flatten, Bidirectional, TimeDistributed
from tensorflow.keras.callbacks import EarlyStopping

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from scipy.stats import pearsonr


# %% [markdown]
# # Subfunctions

# %%
# def load_data():
#     X_train = np.load('data/processed/X_train.npy')
#     y_train = np.load('data/processed/y_train.npy')
#     X_val = np.load('data/processed/X_val.npy')
#     y_val = np.load('data/processed/y_val.npy')
#     return X_train, y_train, X_val, y_val

# def build_model(input_shape):
#     model = keras.Sequential([
#         keras.layers.Dense(32, activation='relu', input_shape=(input_shape,)),
#         keras.layers.Dense(16, activation='relu'),
#         keras.layers.Dense(1)
#     ])
#     model.compile(optimizer='adam', loss='mean_squared_error')
#     return model

# def train_model(X_train, y_train, X_val, y_val):
#     model = build_model(X_train.shape[1])
#     model.fit(X_train, y_train, epochs=50, batch_size=32, validation_data=(X_val, y_val))
#     model.save('models/air_quality_model.h5')

# def main():
#     X_train, y_train, X_val, y_val = load_data()
#     train_model(X_train, y_train, X_val, y_val)

# Function to create multivariate sequences
def create_sequences(data, data_indices, seq_length, pred_length):
    X, y, X_indices, y_indices = [], [], [], []
    for i in range(len(data) - seq_length - pred_length + 1):
        X.append(data[i:(i + seq_length), :])
        y.append(data[(i + seq_length):(i + seq_length + pred_length), -1])  # Assuming the target is the last variable
        X_indices.append(data_indices[i:(i + seq_length)])
        y_indices.append(data_indices[(i + seq_length):(i + seq_length + pred_length)])
    return np.array(X), np.array(y), X_indices, y_indices


# %% [markdown]
# ### Functions for training models

# %%
# Training and evaluation function
def train_and_evaluate(models,
                       X_train, y_train, X_test, y_test,
                       epochs,
                       batch_size,
                       patience,
                       forecast_hours,
                       key):
    history = {}
    results = {}
    early_stopping = EarlyStopping(monitor='val_loss', patience = patience, restore_best_weights=True)  # Configure early stopping

    for name, model in models.items():
        model.compile(optimizer='adam', loss='mse', metrics=['mse'])  # Add MSE as a metric

        # Add a check to ensure input and output dimensions match
        if y_train.shape[1] != model.layers[-1].output_shape[1]:
            raise ValueError(f"Output dimension mismatch for model {name}. "
                             f"Expected: {y_train.shape[1]}, Got: {model.layers[-1].output_shape[1]}")

        history[name] = model.fit(
            X_train,
            y_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(X_test, y_test),
            callbacks=[early_stopping],  # Add the early stopping callback
            verbose=1)
        results[name] = model.evaluate(X_test, y_test, verbose=0)
        # print(f'{name} Test Loss: {results[name][0]}, Test MSE: {results[name][1]}')  # Print both loss and MSE
        print(f'Test MSE: {results[name][1]}')  # Print both loss and MSE
        plot_training_history(history[name], name, forecast_hours, key)  # Plot training history for each model
    return history, results

## Ploting historical training and validating
def plot_training_history(history, name, forecast_hours, key):
    plt.figure(figsize=(12, 3))
    # plt.plot(history.history['loss'], label='Train Loss')
    # plt.plot(history.history['val_loss'], label='Val Loss')
    plt.plot(history.history['mse'], label='Train MSE')  # Plot MSE
    plt.plot(history.history['val_mse'], label='Val MSE')  # Plot MSE
    plt.title(f'Training vs. Validation Metrics for {name} at {forecast_hours}-hour forecast [{key}]')
    plt.xlabel('Epoch')
    plt.ylabel('MSE')
    plt.legend()
    plt.grid(True)
    # plt.show()

# %%
# Function to create models
def create_models(input_shape, output_length):
    models = {}

    # 1D CNN
    cnn_model = Sequential([
        Conv1D(64, kernel_size=3, activation='relu', input_shape=input_shape),
        MaxPooling1D(pool_size=2),
        Flatten(),
        Dense(64, activation='relu'),
        Dense(output_length)
    ])
    models['1D CNN'] = cnn_model

    # LSTM
    lstm_model = Sequential([
        LSTM(128, activation='tanh', return_sequences= True, input_shape=input_shape),
        LSTM(128, activation='tanh'),
        Dense(64, activation='relu'),
        Dense(output_length)
    ])
    models['LSTM'] = lstm_model

    # GRU
    gru_model = Sequential([
        GRU(128, activation='tanh', input_shape=input_shape),
        Dense(64, activation='relu'),
        Dense(output_length)
    ])
    models['GRU'] = gru_model

    # BiLSTM
    bilstm_model = Sequential([
        Bidirectional(LSTM(128, activation='tanh'), input_shape=input_shape),
        Dense(64, activation='relu'),
        Dense(output_length)
    ])
    models['BiLSTM'] = bilstm_model

    # CNN-LSTM
    cnn_lstm_model = Sequential([
        Conv1D(64, kernel_size=3, activation='relu', input_shape=input_shape),
        # MaxPooling1D(pool_size=2),
        # Reshape((input_shape)),
        LSTM(100, activation='tanh'),
        Dense(100, activation='relu'),
        Dense(output_length)
    ])
    # print(cnn_lstm_model.summary())
    models['CNN-LSTM'] = cnn_lstm_model

    # ANN
    ann_model = Sequential([
        Flatten(input_shape=input_shape),
        Dense(100, activation='relu'),
        Dense(100, activation='relu'),
        Dense(output_length)
    ])
    models['ANN'] = ann_model

    return models

# %% [markdown]
# ### Forecasting functions

# %%
# Forecasting function
def forecast(models, X, pred_length):
    forecasts = {}
    for name, model in models.items():
        forecast = model.predict(X[-1].reshape(1, seq_length, X.shape[2]), verbose = 0)
        # print(np.array(forecast).shape)
        # forecasts[name] = scaler.inverse_transform(forecast).flatten()
        # forecasts[name] = scaler_target.inverse_transform(forecast).flatten()

        forecasts[name] = forecast
    return forecasts


def plot_forecasts(data, forecasts, pred_length, title):
    print(data)
    print(forecasts)
    plt.figure(figsize=(15, 4))
    plt.plot(data.index[-(seq_length+pred_length):], data.values[-(seq_length+pred_length):], label='Real_OBS', linewidth = 3)
    for name, forecast in forecasts.items():
        print(data.index[-forecast.shape[1]:])
        forecast = forecast.flatten()
        print("forecast", forecast)
        index = data.index[-forecast.shape[0]:]
        print("index", index)
        plt.plot(index, forecast,  markersize=10,  label=f'{name} Forecast')

    # plt.plot(data.index[-len(forecasts['LSTM']):], forecasts['LSTM'], marker='o', markersize=10,  label=f'Forecast', color = 'red')
    plt.xticks(rotation=90)
    plt.title(title)
    plt.xlabel('Time')
    plt.ylabel('Magnitude')
    plt.legend()
    # plt.show()

# %% [markdown]
# ### Evaluation functions

# %%
def calculate_statistics(y_true, y_pred):
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    pearson_r, _ = pearsonr(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return rmse, mae, pearson_r, r2

def evaluate_forecasts(forecasts, y_true):
    statistics = {}
    for name, forecast in forecasts.items():
        rmse, mae, pearson_r, r2 = np.round(calculate_statistics(y_true, forecast[0]),6)
        statistics[name] = {
            'RMSE': rmse,
            'MAE': mae,
            'Pearson\'s r': pearson_r,
            'R²': r2
        }
        # print(f"{name} - RMSE: {rmse}, MAE: {mae}, Pearson's r: {pearson_r}, R²: {r2}")
    return statistics

# Convert statistics to DataFrame and display
def statistics_to_dataframe(statistics, pred_length):
    df = pd.DataFrame(statistics).T
    df.index.name = 'Model'
    df.columns = ['RMSE', 'MAE', 'Pearson\'s r', 'R²']
    df['Pred. Length'] = pred_length
    return df

# %% [markdown]
# ## Data preparation

# %% [markdown]
# #### Normalized Lidcombe station data

# %%
data = {}
data['station'] = pd.read_csv("/home/htle/Downloads/nowcasting_framework/data/processed/colocation/Lidcombe/AQMS/20210301_20231231/aqms_1141_corrected_normalized.csv",index_col="datetime_utc", parse_dates=True)
data['station'].index = pd.to_datetime(data['station'].index,format="%d/%m%Y %H:%M")
data['station'].index = data['station'].index.strftime("%Y-%m-%d %H:%M:%S")
data['station'].head()


# %% [markdown]
# #### Normalized DSI data at Lidcombe

# %%
data['dsi'] = pd.read_csv("/home/htle/Downloads/nowcasting_framework/data/processed/colocation/Lidcombe/DSI/20210301_20221231/normalized/dsi_Lidcombe_20210301-20221231_hourly.csv",index_col="datetime_utc", parse_dates=True)
data['dsi'].head()

# %% [markdown]
# #### Normalized sensor data at Lidcombe

# %%
sensor_names = ['91355','91721','92367']
for sensor in sensor_names:
    data[sensor] = pd.read_csv(f"/home/htle/Downloads/nowcasting_framework/data/processed/colocation/Lidcombe/DSI/20210301_20221231/normalized/{sensor}_corrected.csv",index_col="datetime_utc", parse_dates=True)

for sensor in sensor_names:
    data[sensor] = data[sensor].resample('1H').mean()
    print(data[sensor].head())


# %%
print(data['station'])

# %% [markdown]
# Synchronize time

# %%
# Extract the desired time range
# start_date = "2021-03-01 00:00"

# start_date = "2022-06-28 07:00:00"
# end_date = "2022-06-28 19:00:00"

start_date = "2022-11-05 01:00:00"
end_date = "2022-11-06 07:00:00"

# start_date = "2022-06-27 19:00:00"
# end_date = "2022-06-28 19:00:00"

time_range = pd.date_range(start=start_date, end=end_date, freq='H')

# Reindex and slice the data
data['station'] = data['station'].loc[start_date:end_date]
data['dsi'] = data['dsi'].loc[start_date:end_date]

for sensor in sensor_names:
    data[sensor] = data[sensor].loc[start_date:end_date]


# %%
# print(data.keys())
# for key, dt in data.items():
#   print('--------'*3 + f' Data of {key} ' + '-------'*3)
#   print(dt.describe())
#   print(f'--------------------------'*3)

# %%
training_var = 'PM2.5'
data_main = {key: dt[training_var] for key,dt in data.items()}
for key, dt in data.items():
  data_main[key] = dt[training_var]

for key, dt in data_main.items():
  print('--------'*3 + f' Data of {key} ' + '-------'*3)
  print(dt.describe())
  print(f'--------------------------'*3)

# %%
def create_sequences_df(data, data_indices, seq_length, pred_length):
    """
    Creates sequences of data for time series forecasting.

    Args:
        data: Pandas DataFrame containing time series data.
        data_indices: Pandas DateTimeIndex corresponding to the data.
        seq_length: Length of the input sequence.
        pred_length: Length of the output prediction.

    Returns:
        Tuple of lists: (X, y, X_indices, y_indices)
            X: List of input sequences.
            y: List of output targets.
            X_indices: List of indices corresponding to the input sequences.
            y_indices: List of indices corresponding to the output targets.
    """
    X, y, X_indices, y_indices = [], [], [], []
    for i in range(len(data) - seq_length - pred_length + 1):
        # Extract input sequence and target values using iloc for integer-based indexing
        X.append(data.iloc[i:(i + seq_length), :].values)  # Convert to NumPy array
        y.append(data.iloc[(i + seq_length):(i + seq_length + pred_length), -1].values)  # Assuming target is last column
        X_indices.append(data_indices[i:(i + seq_length)])
        y_indices.append(data_indices[(i + seq_length):(i + seq_length + pred_length)])
    return X, y, X_indices, y_indices

# %%
def create_sequences_ser(data, data_indices, seq_length, pred_length):
    """
    Creates sequences of data for time series forecasting.

    Args:
        data: Pandas DataFrame containing time series data.
        data_indices: Pandas DateTimeIndex corresponding to the data.
        seq_length: Length of the input sequence.
        pred_length: Length of the output prediction.

    Returns:
        Tuple of lists: (X, y, X_indices, y_indices)
            X: List of input sequences.
            y: List of output targets.
            X_indices: List of indices corresponding to the input sequences.
            y_indices: List of indices corresponding to the output targets.
    """
    X, y, X_indices, y_indices = [], [], [], []
    for i in range(len(data) - seq_length - pred_length + 1):
        # Extract input sequence and target values using iloc for integer-based indexing
        X.append(data.iloc[i:(i + seq_length)].values.reshape(-1, 1))  # Remove the extra ':' for Series slicing
        y.append(data.iloc[(i + seq_length):(i + seq_length + pred_length)].values)  # Assuming target is last column
        X_indices.append(data_indices[i:(i + seq_length)])
        y_indices.append(data_indices[(i + seq_length):(i + seq_length + pred_length)])
    return np.array(X), np.array(y), X_indices, y_indices



# %%
data_main.keys()

# %%
# @title
# Parameters for input length and output forecast lengths
seq_length = 12                 ## Number of historical input values
pred_lengths = [3, 6]            ## Number of future out values

# ## Scale the data
# scaler = StandardScaler()
# data_scaled = scaler.fit_transform(df)
# scaler_target = StandardScaler()
# target_scaled = scaler_target.fit_transform(df.values[:,-1].reshape(-1,1))

# Create sequences for different prediction lengths
X = dict()
y = dict()
X_indices = dict()
y_indices = dict()

for key, value in data_main.items():
    for i in pred_lengths:
        X[f"{i}_{key}"], y[f"{i}_{key}"], X_indices[f"{i}_{key}"], y_indices[f"{i}_{key}"] = create_sequences_ser(value, value.index, seq_length, i)


# %%
from collections import OrderedDict

### Re arrange the data following the order of forecast
X = OrderedDict(sorted(X.items()))
y = OrderedDict(sorted(y.items()))
X_indices = OrderedDict(sorted(X_indices.items()))
y_indices = OrderedDict(sorted(y_indices.items()))

print(X.keys())
print(y.keys())
print(X_indices.keys())
print(y_indices.keys())

# %%


# %% [markdown]
# ## Training models

# %%
# Split data into train and test sets
split_ratio = 0.9
# pred_lengths

# train_size = int(len(X[forecast_length]) * split_ratio)

X_train = dict()
y_train = dict()
X_test = dict()
y_test = dict()

for key in X.keys():
    train_size = int(len(X[key]) * split_ratio)
    X_train[key], X_test[key] = X[key][:train_size], X[key][train_size:]
    y_train[key], y_test[key] = y[key][:train_size], y[key][train_size:]

# key = 'dsi_6'
# print(f"Shape of X_train[{key}]: {X_train[key].shape}")
# print(f"Shape of y_train[{key}]: {y_train[key].shape}")
# print(f"Shape of X_test[{key}]: {X_test[key].shape}")
# print(f"Shape of y_test[{key}]: {y_test[key].shape}")

# # Print shapes for debugging
# for key in X_train.keys():
#     print(f"Shape of X_train[{key}]: {X_train[key].shape}")
#     print(f"Shape of y_train[{key}]: {y_train[key].shape}")
#     print(f"Shape of X_test[{key}]: {X_test[key].shape}")
#     print(f"Shape of y_test[{key}]: {y_test[key].shape}")

# X_train[forecast_length], X_test[forecast_length] = X[forecast_length][:train_size], X[forecast_length][train_size:]
# y_train[forecast_length], y_test[forecast_length] = y[forecast_length][:train_size], y[forecast_length][train_size:]

## Train and evaluate models for 6, 12, and 24 month forecasts
# input_shape = (seq_length, X_train[forecast_length].shape[2])

# print(f"------- Train {forecast_length} model -------")
epochs = 50
batch_size = 512
patience = 5

models = dict()
with tf.device('/CPU:0'):

  for forecast_length in pred_lengths:
    for i, key in enumerate(X_train.keys()):
      if((str(forecast_length)+'_') in key):    ### align length of forecast with segments of data
          input_shape = (seq_length, X_train[key].shape[-1])
          models[key] = create_models(input_shape, forecast_length)
          history, results = train_and_evaluate(models[key], X_train[key], y_train[key], X_test[key], y_test[key], epochs, batch_size, patience, forecast_length, key)



# %% [markdown]
# ### Making forecast prediction and plots

# %%
data_main['station'].iloc[-(forecast_length):]

# %%
models['3_91355']

# %%
forecast_results =  {}
for forecast_length in pred_lengths:
  for i, key in enumerate(X_train.keys()):
    if((str(forecast_length)+'_') in key):    ### align length of forecast with segments of data
      ## Make forecast and plot
      forecast_results[key] = forecast(models[key], X_test[key], forecast_length)    ## Making prediction of last segement   [OK]
      ## Create plots   [Check]
      plot_forecasts(data_main['station'], forecast_results[key], forecast_length, f'{forecast_length} Hours Forecast')
      ## Forecast stats
      statistics = evaluate_forecasts(forecast_results[key], data_main['station'].iloc[-(forecast_length):])
      ## convert to dataframes
      df_stat = statistics_to_dataframe(statistics, forecast_length)
      print(f"---- > Statistic of forecast for sensor {key}: \n", df_stat)
      print('-'*50 )
      print('\n')

plt.show()
# %%
# ---- > Statistic of forecast for sensor 6_91355:
#                RMSE       MAE  Pearson's r         R²  Pred. Length
# Model
# 1D CNN    0.005406  0.004750    -0.196939  -7.380240             6
# LSTM      0.005647  0.004760    -0.466956  -8.143579             6
# GRU       0.007504  0.006547    -0.451649 -15.146627             6
# BiLSTM    0.004160  0.003601    -0.617414  -3.963439             6
# CNN-LSTM  0.005102  0.004538     0.112328  -6.465234             6
# ANN       0.007538  0.006796     0.315925 -15.293370             6
# --------------------------------------------------


# ---- > Statistic of forecast for sensor 6_91721:
#                RMSE       MAE  Pearson's r         R²  Pred. Length
# Model
# 1D CNN    0.007761  0.007268     0.068198 -16.271254             6
# LSTM      0.005198  0.003429    -0.694689  -6.749354             6
# GRU       0.005208  0.004651     0.387225  -6.778148             6
# BiLSTM    0.008129  0.007365    -0.203245 -17.949626             6
# CNN-LSTM  0.006790  0.006027    -0.692468 -12.220591             6
# ANN       0.007025  0.006173     0.333742 -13.152934             6
# --------------------------------------------------


# ---- > Statistic of forecast for sensor 6_92367:
#                RMSE       MAE  Pearson's r         R²  Pred. Length
# Model
# 1D CNN    0.006161  0.005848     0.112145  -9.885590             6
# LSTM      0.006430  0.005885    -0.605748 -10.855625             6
# GRU       0.006839  0.006604     0.358343 -12.413937             6
# BiLSTM    0.007097  0.006537    -0.367587 -13.444266             6
# CNN-LSTM  0.007059  0.006617    -0.628257 -13.287277             6
# ANN       0.007298  0.007034     0.015756 -14.273698             6
# --------------------------------------------------


# ---- > Statistic of forecast for sensor 6_dsi:
#                RMSE       MAE  Pearson's r         R²  Pred. Length
# Model
# 1D CNN    0.003488  0.002725    -0.364914  -2.488040             6
# LSTM      0.005148  0.004829    -0.166022  -6.599865             6
# GRU       0.007120  0.005354    -0.754527 -13.535055             6
# BiLSTM    0.006841  0.005251    -0.300075 -12.418021             6
# CNN-LSTM  0.005898  0.005449    -0.156910  -8.975563             6
# ANN       0.009009  0.007910     0.289157 -22.274731             6
# --------------------------------------------------


# ---- > Statistic of forecast for sensor 6_station:
#                RMSE       MAE  Pearson's r        R²  Pred. Length
# Model
# 1D CNN    0.005193  0.004331    -0.166653 -6.734255             6
# LSTM      0.004683  0.003550    -0.329062 -5.288553             6
# GRU       0.004524  0.003989     0.231740 -4.868712             6
# BiLSTM    0.005872  0.004736    -0.442800 -8.885872             6
# CNN-LSTM  0.005247  0.004411    -0.135473 -6.895221             6
# ANN       0.004612  0.004144     0.137691 -5.100154             6
# --------------------------------------------------

# # %%
# # !git add .
# # !git commit -m "nowcasting fig"
# # !git push https://github.com/Hoang-Trung-Le/nowcasting_framework.git

# # %%
# a = np.array([[0.00364314, 0.00495138, 0.00567521]])
# a.reshape(3,1)

# # %%
# import pandas as pd

# # Example statistics data for individual sensors and DSI
# sensor_stats = {
#     "6_91355": {
#         "1D CNN": {"RMSE": 0.005406, "MAE": 0.004750, "Pearson's r": -0.196939, "R²": -7.380240, "Pred. Length": 6},
#         "LSTM": {"RMSE": 0.005647, "MAE": 0.004760, "Pearson's r": -0.466956, "R²": -8.143579, "Pred. Length": 6},
#         "GRU": {"RMSE": 0.007504, "MAE": 0.006547, "Pearson's r": -0.451649, "R²": -15.146627, "Pred. Length": 6},
#         "BiLSTM": {"RMSE": 0.004160, "MAE": 0.003601, "Pearson's r": -0.617414, "R²": -3.963439, "Pred. Length": 6},
#         "CNN-LSTM": {"RMSE": 0.005102, "MAE": 0.004538, "Pearson's r": 0.112328, "R²": -6.465234, "Pred. Length": 6},
#         "ANN": {"RMSE": 0.007538, "MAE": 0.006796, "Pearson's r": 0.315925, "R²": -15.293370, "Pred. Length": 6}
#     },
#     "6_91721": {
#         "1D CNN": {"RMSE": 0.007761, "MAE": 0.007268, "Pearson's r": 0.068198, "R²": -16.271254, "Pred. Length": 6},
#         "LSTM": {"RMSE": 0.005198, "MAE": 0.003429, "Pearson's r": -0.694689, "R²": -6.749354, "Pred. Length": 6},
#         "GRU": {"RMSE": 0.005208, "MAE": 0.004651, "Pearson's r": 0.387225, "R²": -6.778148, "Pred. Length": 6},
#         "BiLSTM": {"RMSE": 0.008129, "MAE": 0.007365, "Pearson's r": -0.203245, "R²": -17.949626, "Pred. Length": 6},
#         "CNN-LSTM": {"RMSE": 0.006790, "MAE": 0.006027, "Pearson's r": -0.692468, "R²": -12.220591, "Pred. Length": 6},
#         "ANN": {"RMSE": 0.007025, "MAE": 0.006173, "Pearson's r": 0.333742, "R²": -13.152934, "Pred. Length": 6}
#     },
#     "6_92367": {
#         "1D CNN": {"RMSE": 0.006161, "MAE": 0.005848, "Pearson's r": 0.112145, "R²": -9.885590, "Pred. Length": 6},
#         "LSTM": {"RMSE": 0.006430, "MAE": 0.005885, "Pearson's r": -0.605748, "R²": -10.855625, "Pred. Length": 6},
#         "GRU": {"RMSE": 0.006839, "MAE": 0.006604, "Pearson's r": 0.358343, "R²": -12.413937, "Pred. Length": 6},
#         "BiLSTM": {"RMSE": 0.007097, "MAE": 0.006537, "Pearson's r": -0.367587, "R²": -13.444266, "Pred. Length": 6},
#         "CNN-LSTM": {"RMSE": 0.007059, "MAE": 0.006617, "Pearson's r": -0.628257, "R²": -13.287277, "Pred. Length": 6},
#         "ANN": {"RMSE": 0.007298, "MAE": 0.007034, "Pearson's r": 0.015756, "R²": -14.273698, "Pred. Length": 6}
#     }
# }

# dsi_stats = {
#     "1D CNN": {"RMSE": 0.003488, "MAE": 0.002725, "Pearson's r": -0.364914, "R²": -2.488040, "Pred. Length": 6},
#     "LSTM": {"RMSE": 0.005148, "MAE": 0.004829, "Pearson's r": -0.166022, "R²": -6.599865, "Pred. Length": 6},
#     "GRU": {"RMSE": 0.007120, "MAE": 0.005354, "Pearson's r": -0.754527, "R²": -13.535055, "Pred. Length": 6},
#     "BiLSTM": {"RMSE": 0.006841, "MAE": 0.005251, "Pearson's r": -0.300075, "R²": -12.418021, "Pred. Length": 6},
#     "CNN-LSTM": {"RMSE": 0.005898, "MAE": 0.005449, "Pearson's r": -0.156910, "R²": -8.975563, "Pred. Length": 6},
#     "ANN": {"RMSE": 0.009009, "MAE": 0.007910, "Pearson's r": 0.289157, "R²": -22.274731, "Pred. Length": 6}
# }

# # Function to calculate the average statistics for individual sensors
# def average_sensor_stats(sensor_stats):
#     avg_stats = {}
#     for model in sensor_stats[list(sensor_stats.keys())[0]].keys():
#         avg_stats[model] = {
#             "RMSE": sum(sensor_stats[sensor][model]["RMSE"] for sensor in sensor_stats) / len(sensor_stats),
#             "MAE": sum(sensor_stats[sensor][model]["MAE"] for sensor in sensor_stats) / len(sensor_stats),
#             "Pearson's r": sum(sensor_stats[sensor][model]["Pearson's r"] for sensor in sensor_stats) / len(sensor_stats),
#             "R²": sum(sensor_stats[sensor][model]["R²"] for sensor in sensor_stats) / len(sensor_stats),
#         }
#     return avg_stats

# # Calculate the average statistics for the sensors
# avg_sensor_stats = average_sensor_stats(sensor_stats)

# # Function to calculate improvement
# def calculate_improvement(dsi_stats, avg_sensor_stats):
#     improvement = {}
#     for model in dsi_stats.keys():
#         improvement[model] = {
#             "RMSE Improvement (%)": ((avg_sensor_stats[model]["RMSE"] - dsi_stats[model]["RMSE"]) / avg_sensor_stats[model]["RMSE"]) * 100,
#             "MAE Improvement (%)": ((avg_sensor_stats[model]["MAE"] - dsi_stats[model]["MAE"]) / avg_sensor_stats[model]["MAE"]) * 100,
#             "Pearson's r Improvement": (dsi_stats[model]["Pearson's r"] - avg_sensor_stats[model]["Pearson's r"]),
#             "R² Improvement": (dsi_stats[model]["R²"] - avg_sensor_stats[model]["R²"]),
#         }
#     return improvement

# # Calculate the improvement of DSI compared to the average sensor statistics
# improvement = calculate_improvement(dsi_stats, avg_sensor_stats)

# # Display the improvement
# improvement_df = pd.DataFrame(improvement).T
# print(improvement_df)


# # %%



