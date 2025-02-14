from datetime import datetime, timezone, timedelta
import pytz
import geopandas as gpd
from shapely.geometry import Point
import os

def get_utc_time():
    return datetime.now(timezone.utc).replace(microsecond=0)


def get_x_days_ago_utc_time(x):
    return get_utc_time() - timedelta(days=x)

def convert_iso_to_datetime(iso_string):
    dt = datetime.fromisoformat(iso_string)
    if dt.tzinfo:
        return dt.astimezone(pytz.UTC)
    return dt  



def reverse_geocode_shapefile(lat, lon):
    """
    Reverse geocodes a (lat, lon) point using shapefiles.

    :param lat: Latitude of the point.
    :param lon: Longitude of the point.
    :return: (region, local) tuple.
    """
    try:
        base_dir = os.getcwd() 
        lat, lon = float(lat), float(lon)
        # Construct absolute paths dynamically
        states_shapefile = os.path.join(base_dir, "static", "shapesFiles", "state_provinces", "ne_110m_admin_1_states_provinces.shp")
        marine_shapefile = os.path.join(base_dir, "static", "shapesFiles", "marine_polys", "ne_10m_geography_marine_polys.shp")

        # check if the shapefiles exist
        if not os.path.exists(states_shapefile) or not os.path.exists(marine_shapefile):
            raise FileNotFoundError("Shapefiles not found.")
        
        states = gpd.read_file(states_shapefile)
        marine = gpd.read_file(marine_shapefile)

        point = Point(lon, lat)

        match = states[states.geometry.contains(point)]
        if not match.empty:
            # print(", ".join(map(str, match.columns)))
            # print(", ".join(map(str, match.iloc[0].values)))

            region = match.iloc[0]["admin"]
            local = match.iloc[0]["gn_name"]
            return region, local

        match = marine[marine.geometry.contains(point)]
        if not match.empty:
            region = match.iloc[0]["name_en"]
            local = f"{lat}, {lon}"
            return region, local

        return "International Waters", f"{lat}, {lon}"
    except Exception as e:
        print(f"Error in reverse_geocode_shapefile: {e}")
        return "Unknown", f"{lat}, {lon}"


#  from core.utils import reverse_geocode_shapefile
#  reverse_geocode_shapefile(34.0549, 118.2426)