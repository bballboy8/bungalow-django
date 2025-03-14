import boto3
from botocore.exceptions import NoCredentialsError
from decouple import config
from core.models import SatelliteDateRetrievalPipelineHistory
from bungalowbe.utils import reverse_geocode_shapefile
from django.contrib.gis.geos import Polygon
import hashlib
import json
import os
import geopandas as gpd
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

bucket_name = config("AWS_STORAGE_BUCKET_NAME")
AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def save_image_in_s3_and_get_url(image_bytes, id, folder="thumbnails", extension="png", expiration=3600):
    file_name = f"{id}.{extension}"
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder}/{file_name}",
            Body=image_bytes,
            ContentType=f"image/{extension}",
        )
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": f"{folder}/{file_name}"},
            ExpiresIn=expiration,  # Expiration time in seconds
        )

        return presigned_url
    except NoCredentialsError:
        return "AWS credentials not available."
    except Exception as e:
        return str(e)

from core.serializers import (
    SatelliteDateRetrievalPipelineHistorySerializer,
    CollectionCatalogSerializer,
)
from datetime import datetime
from bungalowbe.utils import convert_iso_to_datetime
from core.models import CollectionCatalog
from django.db import transaction
import time
def process_database_catalog(features, start_time, end_time, vendor_name, is_bulk= False):
    """
        Process the database catalog for the given features
    """
    print(f"Database Processing {vendor_name} catalog for {start_time} to {end_time} with {len(features)} records")
    try:
        valid_features = 0
        invalid_features = 0
        valid_records = []

        for feature in features:
            try:
                serializer = CollectionCatalogSerializer(data=feature)
                if serializer.is_valid():
                    serializer.save()
                    valid_features += 1
                    valid_records.append(serializer.data)
                else:
                    print(f"Error in serializer: {serializer.errors}")
                    invalid_features += 1
            except Exception as e:
                invalid_features += 1
        
        print(f"Total records: {len(features)}, Valid records: {(valid_features)}, Invalid records: {(invalid_features)}")

        if is_bulk:
            return "Bulk Inserted"
        
        if not valid_features:
            try:
                print(f"No records Found for {start_time} to {end_time}")
                old_record = SatelliteDateRetrievalPipelineHistory.objects.filter(vendor_name=vendor_name).order_by("-id").first()
                start_time = old_record.start_datetime if old_record else convert_iso_to_datetime(start_time)
                end_time = old_record.end_datetime if old_record else convert_iso_to_datetime(end_time)
                history_serializer = SatelliteDateRetrievalPipelineHistorySerializer(
                    data={
                        "start_datetime": start_time,
                        "end_datetime": end_time,
                        "vendor_name": vendor_name,
                        "message": {
                            "total_records": len(features),
                            "valid_records": valid_features,
                            "invalid_records": invalid_features,
                        },
                    }
                )
                if history_serializer.is_valid():
                    history_serializer.save()

                    # Send WebSocket event
                    channel_layer = get_channel_layer()
                    # Send WebSocket event
                    async_to_sync(channel_layer.group_send)(
                        f"1-SELF",
                        {
                            "type": "send_notification",
                            "message": {
                                "type": "new_records",
                                "vendor_name": vendor_name,
                                "new_updates": valid_features,
                            },
                        },
                    )
                    
                return "No records Found"
            except Exception as e:
                print(f"Error in history serializer: {e}")
                return "Error in history serializer"


        # sort the valid records based on acquisition datetime
        valid_records = sorted(valid_records, key=lambda x: x["acquisition_datetime"])

        try:
            last_acquisition_datetime = valid_records[0]["acquisition_datetime"]
            last_acquisition_datetime = datetime.strftime(
                last_acquisition_datetime, "%Y-%m-%d %H:%M:%S%z"
            )
        except Exception as e:
            last_acquisition_datetime = end_time

        history_serializer = SatelliteDateRetrievalPipelineHistorySerializer(
            data={
                "start_datetime": convert_iso_to_datetime(start_time),
                "end_datetime": convert_iso_to_datetime(last_acquisition_datetime),
                "vendor_name": vendor_name,
                "message": {
                    "total_records": len(features),
                    "valid_records": (valid_features),
                    "invalid_records": (invalid_features),
                },
            }
        )
        if history_serializer.is_valid():
            history_serializer.save()
        else:
            print(f"Error in history serializer: {history_serializer.errors}")
    except Exception as e:
        print(f"Error in process_database_catalog: {e}")

def get_holdback_seconds(acquisition_datetime, publication_datetime):
    """
        Get the holdback seconds between the acquisition and publication datetime
        Args:
            acquisition_datetime: datetime
            publication_datetime: datetime
    """
    try:
        return (publication_datetime - acquisition_datetime).total_seconds()
    except Exception as e:
        print(f"Error in get_time_difference: {e}")
        return None

def get_centroid_and_region_and_location_polygon(coordinates_record):
    try:
        data = {}
        if isinstance(coordinates_record, dict) and coordinates_record.get("type") == "Polygon":
            data["location_polygon"] =  Polygon(coordinates_record["coordinates"][0])
            centroid = data["location_polygon"].centroid
            x, y = centroid.x, centroid.y
            data["geometryCentroid_lat"] = round(y, 8)
            data["geometryCentroid_lon"] = round(x, 8)
            coordinates_record_md5 = hashlib.md5(json.dumps(coordinates_record, sort_keys=True).encode()).hexdigest()
            data["coordinates_record_md5"] = coordinates_record_md5
        return data
    except Exception as e:
        print(f"Error in get_centroid_and_region: {e}")
        return {}

def get_centroid_region_and_local(features):
    try:
        base_dir = os.getcwd() 
        states_shapefile = os.path.join(base_dir, "static", "shapesFiles", "state_provinces", "ne_10m_admin_1_states_provinces.shp")
        marine_shapefile = os.path.join(base_dir, "static", "shapesFiles", "marine_polys", "ne_10m_geography_marine_polys.shp")

        if not os.path.exists(states_shapefile) or not os.path.exists(marine_shapefile):
            raise FileNotFoundError("Shapefiles not found.")

        batch_size = 50
        for i in range(0, len(features), batch_size):
            try:
                states = gpd.read_file(states_shapefile)
                marine = gpd.read_file(marine_shapefile)

                batch = features[i:i + batch_size]
                for feature in batch:
                    feature["centroid_region"], feature["centroid_local"] = reverse_geocode_shapefile(
                        feature["geometryCentroid_lat"], feature["geometryCentroid_lon"], states, marine
                    )
            except Exception as e:
                print(f"Error in get_centroid_region_and_local: {e}")
                continue
        return features
    except Exception as e:
        return []

def remove_z_from_geometry(geometry):
    """
    Removes the Z dimension from a GeoJSON-style geometry dictionary.

    :param geometry: A dictionary with 'type' and 'coordinates' fields (GeoJSON format).
    :return: A modified geometry dictionary with only X, Y coordinates.
    """
    def strip_z(coords):
        """Recursively remove the Z dimension from nested coordinate lists."""
        if isinstance(coords[0], list):  # If the element is a list, recurse
            return [strip_z(sublist) for sublist in coords]
        return coords[:2]  # If it's a coordinate tuple, strip the Z value

    return {
        "type": geometry["type"],
        "coordinates": strip_z(geometry["coordinates"])
    }


def mark_record_as_purchased(features):
    """
    Mark the records as purchased
    """
    try:
        for feature in features:
            try:
                record = CollectionCatalog.objects.get(vendor_id=feature["vendor_id"])
                if record.is_purchased:
                    continue
                record.is_purchased = True
                record.save()
            except Exception as e:
                print(f"Error in mark_record_as_purchased: {e}")
    except Exception as e:
        print(f"Error in mark_record_as_purchased: {e}")