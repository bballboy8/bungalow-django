from datetime import datetime, timezone
import pytz

def get_utc_time():
    return datetime.now(timezone.utc)

def convert_iso_to_datetime(iso_string):
    dt = datetime.fromisoformat(iso_string)
    if dt.tzinfo:
        return dt.astimezone(pytz.UTC)
    return dt  