import os
import json

# Define the directory structure and metadata
locations = {
    "Armidale": {
        "AQMS": {
            "site_name": "ARMIDALE",
            "site_id": "1350",
            "data_range": ["2019-04-01", "2024-05-31"],
        },
        "PAS": {
            "29949": {
                "sensor_id": "29949",
                "sensor_name": "DPE Armidale",
                "data_range": ["2019-04-01", "2024-05-31"],
            }
        },
    },
    "Bathurst": {
        "AQMS": {
            "site_name": "BATHURST",
            "site_id": "795",
            "data_range": ["2021-02-01", "2024-05-31"],
        },
        "PAS": {
            "98435": {
                "sensor_id": "98435",
                "sensor_name": "DPE PA2 AQMN 34",
                "data_range": ["2021-02-01", "2024-05-31"],
            }
        },
    },
    "Lidcombe": {
        "AQMS": {
            "site_name": "LIDCOMBE",
            "site_id": "1141",
            "data_range": ["2021-03-01", "2023-12-31"],
        },
        "PAS": {
            "91721": {
                "sensor_id": "91721",
                "sensor_name": "DPE Lidcombe 1",
                "data_range": ["2021-03-01", "2023-12-31"],
            },
            "92367": {
                "sensor_id": "92367",
                "sensor_name": "DPE Lidcombe 2",
                "data_range": ["2021-03-01", "2023-12-31"],
            },
            "91355": {
                "sensor_id": "91355",
                "sensor_name": "DPE Lidcombe 3",
                "data_range": ["2021-03-01", "2023-12-31"],
            },
        },
    },
    "Millthorpe": {
        "AQMS": {
            "site_name": "MILLTHORPE",
            "site_id": "798",
            "data_range": ["2023-07-01", "2024-05-31"],
        },
        "PAS": {
            "182853": {
                "sensor_id": "182853",
                "sensor_name": "DPE PA2 AQMN 34",
                "data_range": ["2023-07-01", "2024-05-31"],
            }
        },
    },
    "Wagga_Wagga_North": {
        "AQMS": {
            "site_name": "Wagga Wagga North",
            "site_id": "1650",
            "data_range": ["2019-04-01", "2024-05-31"],
        },
        "PAS": {
            "29959": {
                "sensor_id": "29959",
                "sensor_name": "DPE Wagga 1",
                "data_range": ["2019-04-01", "2024-05-31"],
            },
            "29945": {
                "sensor_id": "29945",
                "sensor_name": "DPE Wagga 2",
                "data_range": ["2019-04-01", "2024-05-31"],
            },
            "30007": {
                "sensor_id": "30007",
                "sensor_name": "DPE Wagga 3",
                "data_range": ["2019-04-01", "2024-05-31"],
            },
        },
    },
}

# Base directory for data
base_dir = "data/raw/colocation"

# Create directories based on the structure
for location, sensors in locations.items():
    location_dir = os.path.join(base_dir, location)
    os.makedirs(location_dir, exist_ok=True)

    aqms_dir = os.path.join(location_dir, "AQMS")
    os.makedirs(aqms_dir, exist_ok=True)

    pas_dir = os.path.join(location_dir, "PAS")
    os.makedirs(pas_dir, exist_ok=True)

    for sensor, details in sensors["PAS"].items():
        sensor_dir = os.path.join(pas_dir, sensor)
        os.makedirs(sensor_dir, exist_ok=True)

# Save metadata to a JSON file
metadata_dir = "metadata/colocation"
os.makedirs(metadata_dir, exist_ok=True)
metadata_file = os.path.join(metadata_dir, "colocation.json")
with open(metadata_file, "w") as f:
    json.dump(locations, f, indent=4)

print("Directory structure created and metadata saved.")
