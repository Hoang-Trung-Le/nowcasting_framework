import os
import sys
import json
import pandas as pd
import datetime as dt
# Print the current working directory to help debug the path issue
# print(os.getcwd())

# Adjust the path to include the directory where get_aqms_data.py is located
sys.path.insert(0, os.path.join(os.getcwd(), "src", "utils", "data", "monitoring", "AQMS"))
# from src.utils.data.monitoring.AQMS.get_aqms_data import aqms_api_class
from get_aqms_data import aqms_api_class


def fetch_and_store_aqms_data(metadata_file):
    with open(metadata_file, "r") as f:
        metadata = json.load(f)

    AQMS = aqms_api_class()

    for location, details in metadata.items():
        site_id = details["AQMS"]["site_id"]
        start_date = details["AQMS"]["data_range"][0]
        end_date = details["AQMS"]["data_range"][1]

        ObsRequest = AQMS.ObsRequest_init()
        ObsRequest["Sites"] = [site_id]
        ObsRequest["Parameters"] = ["PM2.5", "PM10", "TEMP", "HUMID"]
        ObsRequest["Categories"] = ["Averages"]
        ObsRequest["SubCategories"] = ["Hourly"]
        ObsRequest["Frequency"] = ["Hourly average"]
        ObsRequest["StartDate"] = start_date
        ObsRequest["EndDate"] = end_date

        AllObs = AQMS.get_historical_obs(ObsRequest)
        df = pd.json_normalize(AllObs.json())

        # Reformat start_date and end_date
        start_date_formatted = dt.datetime.strptime(start_date, "%Y-%m-%d").strftime(
            "%Y%m%d"
        )
        end_date_formatted = dt.datetime.strptime(end_date, "%Y-%m-%d").strftime(
            "%Y%m%d"
        )

        # Construct the folder path and filename
        folderpathdir = os.path.join(
            "data",
            "raw",
            "colocation",
            location,
            "AQMS",
            f"{start_date_formatted}_{end_date_formatted}",
        )
        if not os.path.exists(folderpathdir):
            os.makedirs(folderpathdir)

        filename = os.path.join(
            folderpathdir, f"aqms_{site_id}_{start_date_formatted}_{end_date_formatted}_hourly.csv"
        )

        # Write the DataFrame to CSV
        if os.path.exists(filename):
            df.to_csv(filename, mode="a", index=False, header=False)
        else:
            df.to_csv(filename, index=False, header=True)

        print(f"Data for {location} stored in {filename}")


if __name__ == "__main__":
    metadata_file = "metadata/colocation/colocation.json"
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    print(metadata)
    fetch_and_store_aqms_data(metadata_file)
