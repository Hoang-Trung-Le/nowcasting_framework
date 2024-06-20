import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
import json
import calendar


def reformat_aqms_data(input_filepath, output_filepath):
    """
    Reformat the AQMS data from the input CSV file and save it to the output CSV file.

    Args:
        input_filepath (str): The path to the input CSV file containing the raw AQMS data.
        output_filepath (str): The path to the output CSV file to save the processed AQMS data.
    """

    # Read the original CSV file
    df = pd.read_csv(input_filepath)

    # Initialize lists to store the values for the new columns
    datetime_utc = []
    timestamp = []
    site_id = []
    HUMID = []
    TEMP = []
    PM25 = []
    PM10 = []

    # Iterate over the rows of the DataFrame
    for _ , row in df.iterrows():
        # Construct datetime_utc
        hour = row["Hour"]
        if hour == 24:
            date_time_obj = datetime.strptime(row["Date"], "%Y-%m-%d") + timedelta(
                days=1
            )
        else:
            date_time_str = row["Date"] + " " + str(row["Hour"]) + ":00"
            date_time_obj = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")

        datetime_utc.append(date_time_obj)
        # Construct timestamp

        # Append other columns
        site_id.append(row["Site_Id"])
        if row["Parameter.ParameterCode"] == "HUMID":
            HUMID.append(row["Value"])
        elif row["Parameter.ParameterCode"] == "TEMP":
            TEMP.append(row["Value"])
        elif row["Parameter.ParameterCode"] == "PM2.5":
            if row["Value"] < 0:
                PM25.append(0)
            else:
                PM25.append(row["Value"])
        elif row["Parameter.ParameterCode"] == "PM10":
            if row["Value"] < 0:
                PM10.append(0)
            else:
                PM10.append(row["Value"])

    datetime_utc = list(sorted(set(datetime_utc)))
    for dt in datetime_utc:
        timestamp.append(int(time.mktime(dt.timetuple())))

    site_id = list(set(site_id)) * len(list(set(datetime_utc)))

    # Create a new DataFrame from the lists
    new_df = pd.DataFrame(
        {
            "datetime_utc": datetime_utc,
            "timestamp": timestamp,
            "site_id": site_id,
            "HUMID": HUMID,
            "TEMP": TEMP,
            "PM2.5": PM25,
            "PM10": PM10,
        }
    )

    # Convert timestamp to datetime and format it
    datetime_utc = pd.to_datetime(new_df["timestamp"], unit="s", utc=True)
    new_df["datetime_utc"] = datetime_utc.dt.strftime("%Y-%m-%d %H:%M:%S")

    # Set datetime_utc as the index of the DataFrame
    new_df.set_index("datetime_utc", inplace=True)

    # Fill missing values using linear interpolation
    # new_df.interpolate(method="linear", inplace=True)

    # Columns to round up
    columns_to_round_up = ["HUMID", "TEMP", "PM2.5", "PM10"]
    # Round up the values in the specified columns
    new_df[columns_to_round_up] = np.round(new_df[columns_to_round_up], 3)

    # Write the new DataFrame to a new CSV file
    new_df.to_csv(output_filepath, index=True, header=True)
    print(f"Processed data saved to {output_filepath}")


def time_average(input_filepath, output_filepath, freq):
    """
    Compute the monthly average of the AQMS data and save it to the output CSV file.

    Args:
        input_filepath (str): The path to the input CSV file containing the processed AQMS data.
        output_filepath (str): The path to the output CSV file to save the monthly averaged data.
    """

    # Read the processed CSV file
    df = pd.read_csv(
        input_filepath, parse_dates=["datetime_utc"], index_col="datetime_utc"
    )

    # Exclude zero values by replacing them with NaN
    df.replace(0, np.nan, inplace=True)

    # Group by the specified frequency and compute the mean, ignoring NaNs
    averaged_df = df.resample(f"{freq}").mean()

    # Convert the index to UTC timestamps
    averaged_df['timestamp'] = averaged_df.index.map(lambda x: calendar.timegm(x.utctimetuple()))

    # Write the time averaged DataFrame to a new CSV file
    averaged_df.to_csv(output_filepath, index=True, header=True)
    print(f"Frequency {freq} averaged data saved to {output_filepath}")


if __name__ == "__main__":

    metadata_file = "metadata/colocation/colocation.json"
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    print(metadata)

    for location, details in metadata.items():

        site_id = details["AQMS"]["site_id"]
        start_date = details["AQMS"]["data_range"][0]
        end_date = details["AQMS"]["data_range"][1]
        # Reformat start_date and end_date
        start_date_formatted = datetime.strptime(start_date, "%Y-%m-%d").strftime(
            "%Y%m%d"
        )
        end_date_formatted = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
        input_filepath = f"data/raw/colocation/{location}/AQMS/{start_date_formatted}_{end_date_formatted}/aqms_{site_id}_{start_date_formatted}_{end_date_formatted}_hourly.csv"
        output_filepath_formatted = f"data/raw/colocation/{location}/AQMS/{start_date_formatted}_{end_date_formatted}/aqms_{site_id}_{start_date_formatted}_{end_date_formatted}_hourly_formatted.csv"
        # reformat_aqms_data(input_filepath, output_filepath_formatted)

        output_filepath_monthly = f"data/raw/colocation/{location}/AQMS/{start_date_formatted}_{end_date_formatted}/aqms_{site_id}_{start_date_formatted}_{end_date_formatted}_monthly.csv"
        time_average(output_filepath_formatted,output_filepath_monthly,'ME')
        # output_filepath_daily = f"data/raw/colocation/{location}/AQMS/{start_date_formatted}_{end_date_formatted}/aqms_{site_id}_{start_date_formatted}_{end_date_formatted}_daily.csv"
        # time_average(output_filepath_formatted, output_filepath_daily, "D")
