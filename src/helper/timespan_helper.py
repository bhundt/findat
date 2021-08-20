from datetime import date, timedelta

def get_timestamp_from_date(date_to_convert):
    return int((date_to_convert - date(1970, 1, 1)) / timedelta(seconds=1))