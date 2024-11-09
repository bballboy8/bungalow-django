from datetime import datetime, timezone, timedelta
import pytz

def get_utc_time():
    return datetime.now(timezone.utc).replace(microsecond=0)


def get_x_days_ago_utc_time(x):
    return get_utc_time() - timedelta(days=x)

def convert_iso_to_datetime(iso_string):
    dt = datetime.fromisoformat(iso_string)
    if dt.tzinfo:
        return dt.astimezone(pytz.UTC)
    return dt  