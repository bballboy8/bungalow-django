import boto3
from botocore.exceptions import NoCredentialsError

from decouple import config

bucket_name = config("AWS_STORAGE_BUCKET_NAME")
AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")



def save_image_in_s3_and_get_url(image_bytes, id, extension="png"):
    file_name = f"{id}.{extension}"
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        s3.put_object(Bucket=bucket_name, Key=f"images/{file_name}", Body=image_bytes)
        url = f"https://{bucket_name}.s3.amazonaws.com/images/{file_name}"
        return url
    except NoCredentialsError:
        return "AWS credentials not available."
    except Exception as e:
        return str(e)
