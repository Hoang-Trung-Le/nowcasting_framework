import os
import pandas as pd
import numpy as np
import json
from datetime import datetime


def usepa_pm25_conversion(cf, rh):
    if (0 <= cf) & (cf < 30):
        return 0.524 * cf - 0.0862 * rh + 5.75
    elif (30 <= cf) & (cf < 50):
        return (
            (0.786 * (cf / 20 - 3 / 2) + 0.524 * (1 - (cf / 20 - 3 / 2))) * cf
            - 0.0862 * rh
            + 5.75
        )
    elif (50 <= cf) & (cf < 210):
        return 0.786 * cf - 0.0862 * rh + 5.75
    elif (210 <= cf) & (cf < 260):
        return (
            (0.69 * (cf / 50 - 21 / 5) + 0.786 * (1 - (cf / 50 - 21 / 5))) * cf
            - 0.0862 * rh * (1 - (cf / 50 - 21 / 5))
            + 2.966 * (cf / 50 - 21 / 5)
            + 5.75 * (1 - (cf / 50 - 21 / 5))
            + 8.84 * (10**-4) * cf**2 * (cf / 50 - 21 / 5)
        )
    elif 260 <= cf:
        return 2.966 + 0.69 * cf + 8.84 * (10**-4) * cf**2
    else:
        return 0.0


def pas_correction(input_filepath, output_filepath):
    # Read the processed CSV file
    df = pd.read_csv(
        input_filepath, parse_dates=["datetime_utc"], index_col="datetime_utc"
    )

    df["pm2.5_corrected"] = df.apply(
        lambda row: usepa_pm25_conversion(row["pm2.5_cf_1"], row["humidity"]),
        axis=1,
    )
    df.drop("pm2.5_cf_1", axis=1, inplace=True)
    # Write the time averaged DataFrame to a new CSV file
    df.to_csv(output_filepath, index=True, header=True)
    print(f"Corrected data saved to {output_filepath}")


if __name__ == "__main__":
    metadata_file = "metadata/colocation/colocation.json"
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    print(metadata)
    frequencies = ["hourly", "daily", "monthly"]
    for freq in frequencies:
        for location, details in metadata.items():

            sensors = details.get("PAS", {})

            for sensor_id, sensor_details in sensors.items():
                start_date = sensor_details["data_range"][0]
                end_date = sensor_details["data_range"][1]
                group_id = sensor_details["group_id"]
                member_id = sensor_details["member_id"]
                # Reformat start_date and end_date
                start_date_formatted = datetime.strptime(
                    start_date, "%Y-%m-%d"
                ).strftime("%Y%m%d")
                end_date_formatted = datetime.strptime(end_date, "%Y-%m-%d").strftime(
                    "%Y%m%d"
                )
                input_filepath = f"data/raw/colocation/{location}/PAS/{sensor_id}/{start_date_formatted}_{end_date_formatted}/pas_{sensor_id}_{start_date_formatted}_{end_date_formatted}_{freq}_conversion.csv"

                # Construct the folder path and filename
                folderpathdir = os.path.join(
                    "data",
                    "processed",
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
                    f"pas_{sensor_id}_{start_date_formatted}_{end_date_formatted}_{freq}.csv",
                )

                # output_filepath_corrected = f"../data/raw/colocation/{location}/PAS/{sensor_id}/{start_date_formatted}_{end_date_formatted}/pas_{sensor_id}_{start_date_formatted}_{end_date_formatted}_{freq}.csv"
                pas_correction(input_filepath, filename)
