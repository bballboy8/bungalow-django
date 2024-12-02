import boto3
from botocore.exceptions import NoCredentialsError

import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from decouple import config
from boto3.s3.transfer import TransferConfig


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
