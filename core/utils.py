import boto3
from botocore.exceptions import NoCredentialsError

import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from boto3.s3.transfer import TransferConfig
from core.models import SatelliteDateRetrievalPipelineHistory


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

def process_database_catalog(features, start_time, end_time, vendor_name, is_bulk= False):
    """
        Process the database catalog for the given features
    """
    print(f"Database Processing {vendor_name} catalog for {start_time} to {end_time} with {len(features)} records")
    try:
        valid_features = 0
        invalid_features = 0

        for feature in features:
            try:
                serializer = CollectionCatalogSerializer(data=feature)
                if serializer.is_valid():
                    serializer.save()
                    valid_features += 1
                else:
                    print(f"Error in serializer: {serializer.errors}")
                    invalid_features += 1
            except Exception as e:
                print(f"Error in checking serialzer: {e}")
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
                return "No records Found"
            except Exception as e:
                print(f"Error in history serializer: {e}")
                return "Error in history serializer"


        try:
            last_acquisition_datetime = valid_features[0]["acquisition_datetime"]
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
