column_mapping = {
    "humidity": "HUMID",
    "temperature": "TEMP",
    "pm2.5_alt": "PM2.5",
    "pm10.0_atm": "PM10",
    "pm2.5_corrected": "PM2.5_corrected",
}

HEADER_MAPPING = {
    "HUMID": ["humidity", "HUMID_DSI"],
    "TEMP": ["temperature", "TEMP_DSI"],
    "PM2.5": ["pm2.5_alt", "pm2.5_corrected", "PM2.5_DSI"],
    "PM10": ["pm10.0_atm", "PM10_DSI"],
}

PAS_PARAM_DISPLAY = {
    "humidity": "Raw humidity",
    "temperature": "Raw temperature",
    "pm2.5_alt": "Raw PM2.5",
    "pm10.0_atm": "Raw PM10",
    "pm2.5_corrected": "Corrected PM2.5",
    "PM2.5_DSI": "DSI PM2.5",
}
