

# config.py
PAS_API_TIME_LIMITS = {
    "hourly": {"freq": 60, "limit_days": 179}, # the actual time limit is 180 days
    "daily": {"freq": 1440, "limit_days": 730}, # the actual time limit is 2 years
    "monthly": {"freq": 43200, "limit_days": 2000}
}
