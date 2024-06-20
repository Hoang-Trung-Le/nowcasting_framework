column_mapping = {
    "humidity": "HUMID",
    "temperature": "TEMP",
    "pm2.5_alt": "PM2.5",
    "pm10.0_atm": "PM10",
    "pm2.5_corrected": "PM2.5_corrected",
}

HEADER_MAPPING = {
    "HUMID": "humidity",
    "TEMP": "temperature",
    "PM2.5": ["pm2.5_alt", "pm2.5_corrected"],
    "PM10": "pm10.0_atm"
}

