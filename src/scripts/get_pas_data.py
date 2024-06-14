import os
import sys
import json
import time
import pandas as pd
import datetime as dt

sys.path.insert(
    0, os.path.join(os.getcwd(), "src", "utils", "data", "monitoring", "PAS")
)
from pa_get_group_data import get_historicaldata

# import src.utils.data.monitoring.PAS.pa_get_group_data


def fetch_and_store_pas_data(metadata_file, average_time):
    with open(metadata_file, "r") as f:
        metadata = json.load(f)

    for location, details in metadata.items():
        sensors = details.get("PAS", {})

        for sensor_id, sensor_details in sensors.items():
            start_date = sensor_details["data_range"][0]
            end_date = sensor_details["data_range"][1]

            # Reformat start_date and end_date
            start_date_formatted = dt.datetime.strptime(
                start_date, "%Y-%m-%d"
            ).strftime("%Y%m%d")
            end_date_formatted = dt.datetime.strptime(end_date, "%Y-%m-%d").strftime(
                "%Y%m%d"
            )

            bdate = start_date_formatted
            edate = end_date_formatted
            field_list = [
                "humidity",
                "temperature",
                "pm2.5_alt",
                "pm10.0_atm",
            ]  # Example fields, adjust as needed

            # Fetch the data using get_historicaldata function
            df = get_historicaldata(sensor_id, bdate, edate, average_time, field_list)

            if df.empty:
                print(f"No data available for sensor {sensor_id} at {location}")
                continue

            # Construct the folder path and filename
            folderpathdir = os.path.join(
                "data",
                "raw",
                "colocation",
                location,
                "PAS",
                sensor_id,
                f"{start_date_formatted}_{end_date_formatted}",
            )
            if not os.path.exists(folderpathdir):
                os.makedirs(folderpathdir)

            filename = os.path.join(
                folderpathdir,
                f"pas_{sensor_id}_{start_date_formatted}_{end_date_formatted}.csv",
            )

            # Write the DataFrame to CSV
            df.to_csv(filename, index=True, header=True)
            print(f"Data for PAS sensor {sensor_id} at {location} stored in {filename}")


if __name__ == "__main__":
    metadata_file = "metadata/colocation/colocation.json"
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    print(metadata)
    average_time = 43200  # monthly
    fetch_and_store_pas_data(metadata_file, average_time)
