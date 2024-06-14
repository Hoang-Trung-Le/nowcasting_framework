# -*- coding: utf-8 -*-
"""
This code gets hisotrical PurpleAir data from new PurpleAir API for a group of monitors and stores them in a file structure in my documents.

Data from the site are in bytes/text and NOT in JSON format.

Created on Fri Jun 10 21:34:01 2022

@author: Zuber Farooqui, Ph.D.

Modified Wed Feb 15 17:00:00 2023

@author: Aaron Lamplugh, Ph.D.
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import time
import json
import os
from os.path import join, getsize
from pathlib import Path
import glob
from io import StringIO

# from src.config.config import *

# API Keys provided by PurpleAir(c)
key_read = "D80F3AFD-DDAD-11ED-BD21-42010A800008"

# Sleep Seconds
sleep_seconds = 3  # wait sleep_seconds after each query

# Sensor List for Data Download
# Dependable group
# groupid = "1872"
# Lidcome group
# groupid = "2308"

# Average_time. The desired average in minutes, one of the following: 0 (real-time),
# 10 (default if not specified), 30, 60, 360 (6 hour), 1440 (1 day)
average_time = (
    10  # or 10  or 0 (Current script is set only for real-time, 10, or 60 minutes data)
)

# sensor_ids = [173213, 173212, 173211, 173210]
# sensor_indexes = [180263, 180261, 180259, 180257]


# def get_sensorslist(groupid, key_read):
# PurpleAir API URL
# root_url = "https://api.purpleair.com/v1/groups/"

# fields_list = ["id", "sensor_index", "created"]

# # Final API URL
# api_url = root_url + groupid + f"?api_key={key_read}"
# print(api_url)

# # Getting data
# response = requests.get(api_url)

# if response.status_code == 200:
#     # print(response.text)
#     json_data = json.loads(response.content)["members"]
#     df = pd.DataFrame.from_records(json_data)
#     df.columns = fields_list
# else:
#     raise requests.exceptions.RequestException

# # writing to csv file - Enter Directory info here
# folderpath = r"C:\Users\user\Documents\python_stuff\sensors_list"
# filename = folderpath + "\sensors_list.csv"
# df.to_csv(filename, index=False, header=True)

# # Creating a Sensors
# sensorslist = list(df.id)
# print(sensorslist)
# return sensorslist


# def convert_to_unix(date_str, format="%d-%m-%Y %H:%M:%S"):
#     """
#     Convert a datetime string to a Unix timestamp using the specified format.

#     Args:
#         date_str (str): Datetime string.
#         format (str): Format of the datetime string (default is '%d-%m-%Y %H:%M:%S').

#     Returns:
#         int: Unix timestamp.
#     """
#     try:
#         formatted_time = datetime.strptime(date_str, format)
#     except ValueError:
#         format_no_time = "%d-%m-%Y"
#         formatted_time = datetime.strptime(date_str, format_no_time)

#     unix_time = int(time.mktime(formatted_time.timetuple()))
#     return unix_time


def convert_to_unix(date_str):
    return int(time.mktime(datetime.strptime(date_str, "%Y%m%d").timetuple()))


def get_historicaldata(sensor_index, bdate, edate, average_time, field_list):
    key_read = "D80F3AFD-DDAD-11ED-BD21-42010A800008"
    # Historical API URL
    root_api_url = "https://api.purpleair.com/v1/sensors/" + sensor_index

    # Average time: The desired average in minutes, one of the following:0 (real-time),10 (default if not specified),30,60
    average_api = f"&average={average_time}"

    # SD Data Fields
    print(field_list)
    for i, f in enumerate(field_list):
        if i == 0:
            fields_api_url_sd = f"&fields={f}"
        else:
            fields_api_url_sd += f"%2C{f}"

    print(fields_api_url_sd)

    # Converting to UNIX timestamp
    begindate = convert_to_unix(bdate)
    enddate = convert_to_unix(edate)

    all_dfs = []

    # Gets Sensor Data
    # for s in sensor_index:

    # Adding sensor_index & API Key
    hist_api_url = root_api_url + f"/history/csv?api_key={key_read}"
    print(hist_api_url)
    # print(s)
    # Special URL to grab sensor registration name
    name_api_url = root_api_url + f"?fields=name&api_key={key_read}"
    print(name_api_url)
    # get sensor registration name:
    response = requests.get(name_api_url)
    response.raise_for_status()

    namedf = pd.read_csv(
        StringIO(response.text),
        sep=",|:",
        header=None,
        skiprows=8,
        index_col=None,
        engine="python",
    )

    # Response will be the registered name of the sensor
    # sensorname = str(namedf[1][0])
    # sensorname = sensorname.strip()
    # sensorname = sensorname.strip('"')

    # Wait time
    time.sleep(3)

    print(
        "Downloading for PA: %s for Dates: %s to %s."
        % (
            sensor_index,
            datetime.fromtimestamp(begindate),
            datetime.fromtimestamp(enddate),
        )
    )
    dates_api_url = f"&start_timestamp={begindate}&end_timestamp={enddate}"

    # Creates final URLs that download data
    api_url_e = hist_api_url + dates_api_url + average_api + fields_api_url_sd

    # Queries URLs for data
    response = requests.get(api_url_e)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text), sep=",", header=0)

    if df.empty:
        print("------------- No Data Available -------------")
    else:
        df["date_time_utc"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
        df = (
            df.drop_duplicates().sort_values(by="time_stamp").set_index("date_time_utc")
        )

        # cloned_df = df.copy(deep=True)
        # resampled_values = (
        #     df[field_list].resample(f"{average_time}min").bfill(limit=1).fillna(0)
        # )
        # df = pd.concat(
        #     [
        #         cloned_df.drop(field_list, axis=1)
        #         .resample(f"{average_time}min")
        #         .bfill(),
        #         resampled_values,
        #     ],
        #     axis=1,
        # )

        columns_to_round_up = ["pm2.5_alt", "pm10.0_atm"]
        df[columns_to_round_up] = np.round(df[columns_to_round_up], 3)
        df.loc[df["temperature"] != 0, "temperature"] = (
            (df.loc[df["temperature"] != 0, "temperature"] - 32) * 5 / 9
        )
        df["temperature"] = np.round(df["temperature"], 1)

        all_dfs.append(df)

    if all_dfs:
        return pd.concat(all_dfs)
    else:
        return pd.DataFrame()


# def get_historicaldata(sensor_index, bdate, edate, average_time, field_list):
#     key_read = "D80F3AFD-DDAD-11ED-BD21-42010A800008"
#     # Historical API URL
#     root_api_url = "https://api.purpleair.com/v1/sensors/" + sensor_index + "/members/"

#     # Average time: The desired average in minutes, one of the following:0 (real-time),10 (default if not specified),30,60
#     average_api = f"&average={average_time}"

#     # SD Data Fields
#     print(field_list)
#     for i, f in enumerate(field_list):
#         if i == 0:
#             fields_api_url_sd = f"&fields={f}"
#         else:
#             fields_api_url_sd += f"%2C{f}"

#     print(fields_api_url_sd)

#     # Converting to UNIX timestamp
#     begindate = convert_to_unix(bdate)
#     enddate = convert_to_unix(edate)

#     # Gets Sensor Data
#     for i, s in enumerate(sensor_index):

#         # Adding sensor_index & API Key
#         hist_api_url = root_api_url + f"{s}/history/csv?api_key={key_read}"
#         print(hist_api_url)

#         # Special URL to grab sensor registration name
#         name_api_url = root_api_url + f"{s}?fields=name&api_key={key_read}"

#         # get sensor registration name:
#         try:
#             response = requests.get(name_api_url)
#         except:
#             print(name_api_url)

#         try:
#             assert response.status_code == requests.codes.ok

#             namedf = pd.read_csv(
#                 StringIO(response.text),
#                 sep=",|:",
#                 header=None,
#                 skiprows=8,
#                 index_col=None,
#                 engine="python",
#             )

#         except AssertionError:
#             namedf = pd.DataFrame()
#             print("Bad URL!")

#         # Response will be the registered name of the sensor
#         sensorname = str(namedf[1][0])
#         sensorname = sensorname.strip()
#         sensorname = sensorname.strip('"')

#         # Creating start and end date api url
#         # Wait time
#         time.sleep(sleep_seconds)

#         # if i < len_datelist:
#         print(
#             "Downloading for PA: %s for Dates: %s to %s."
#             % (
#                 s,
#                 datetime.fromtimestamp(begindate),
#                 datetime.fromtimestamp(enddate),
#             )
#         )
#         dates_api_url = f"&start_timestamp={begindate}&end_timestamp={enddate}"

#         # Creates final URLs that download data in the format of previous PA downloads and SD card data
#         api_url_e = hist_api_url + dates_api_url + average_api + fields_api_url_sd

#         # creates list of all URLs
#         URL_List = [api_url_e]

#         # for x in URL_List:
#         # queries URLs for data
#         try:
#             response = requests.get(api_url_e)
#         except:
#             print(api_url_e)
#         #
#         try:
#             assert response.status_code == requests.codes.ok

#             # Creating a Pandas DataFrame
#             df = pd.read_csv(StringIO(response.text), sep=",", header=0)

#         except AssertionError:
#             df = pd.DataFrame()
#             print("Bad URL!")

#         if df.empty:
#             print("------------- No Data Available -------------")
#         else:
#             # Adding Sensor Index/ID
#             # df["id"] = s

#             #
#             date_time_utc = []
#             for index, row in df.iterrows():
#                 date_time_utc.append(
#                     datetime.fromtimestamp(row["time_stamp"], tz=timezone.utc)
#                 )
#             df["date_time_utc"] = date_time_utc

#             # Dropping duplicate rows
#             df = df.drop_duplicates(subset=None, keep="first", inplace=False)
#             df = df.sort_values(by=["time_stamp"], ascending=True, ignore_index=True)
#             # Convert the 'UTCDateTime' column to datetime format
#             df["date_time_utc"] = pd.to_datetime(df["date_time_utc"])

#             # Set 'UTCDateTime' as the index
#             df.set_index("date_time_utc", inplace=True)
#             # print(df)
#             cloned_df = df.copy(deep=True)
#             resampled_values = df[field_list].resample(f"{average_time}min").bfill(limit=1).fillna(0)
#             print("Resampled Values", resampled_values)
#             # print(combined_data.drop(field_list,axis=1))
#             df = pd.concat(
#                 [
#                     cloned_df.drop(field_list, axis=1).resample("2min").bfill(),
#                     resampled_values,
#                 ],
#                 axis=1,
#             )
#             # Convert the 'time_stamp' column to Unix timestamp
#             df["time_stamp"] = df.index.astype("int64") // 10**9
#             # print("Final Data", df)

#             # Columns to round up
#             columns_to_round_up = ["pm1.0_atm", "pm2.5_alt", "pm10.0_atm"]
#             # Round up the values in the specified columns
#             df[columns_to_round_up] = np.round(df[columns_to_round_up], 3)
#             # Convert temperature column from Fahrenheit to Celsius
#             df.loc[df["temperature"] != 0, "temperature"] = (
#                 (df.loc[df["temperature"] != 0, "temperature"] - 32) * 5 / 9
#             )
#             df["temperature"] = np.round(df["temperature"], 1)
#             # Writing to Postgres Table (Optional)
#             # df.to_sql('tablename', con=engine, if_exists='append', index=False)

#             # writing to csv file
#             today = datetime(2024, 4, 30).strftime("%Y%m%d")
#             # folderpathdir = rf"D:\UTS\OneDrive - UTS\HDR\Papers\Dependability\Coding\sensor-reliability\data\{today}\raw"
#             folderpathdir = rf"data\purpleair_data_csv\purpleair_data_csv"
#             if not os.path.exists(folderpathdir):
#                 os.makedirs(folderpathdir)

#             filename = folderpathdir + rf"\{s}_20240523-20240525.csv"
#             # Check if the file already exists
#             if os.path.exists(filename):
#                 # Append the DataFrame to the existing CSV file without writing the header
#                 df.to_csv(filename, mode="a", index=True, header=False)
#             else:
#                 # Write the DataFrame to a new CSV file with the header
#                 df.to_csv(filename, index=True, header=True)
#     return df

# Getting PA data
# fields = retrieve_parameters
# print(fields)
# # Data download period. Enter Start and end Dates
# bdate = "23-05-2024 00:00:00"
# edate = "25-05-2024 23:59:59"
# sensor_indexes = [91355, 91721, 92367]
# sensor_ids = [208466, 208467, 208468]
# folderlist = get_historicaldata(
#     sensor_ids, bdate, edate, average_time, fields, key_read
# )
