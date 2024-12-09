from typing import List
from logging_module import logger
import requests
from core.services.airbus_catalog_api import get_acces_token
from core.utils import save_image_in_s3_and_get_url
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.models import SatelliteCaptureCatalog
from core.services.maxar_catalog_api import (
    upload_to_s3 as maxar_upload_to_s3,
    AUTH_TOKEN,
)
from core.services.blacksky_catalog_api import (
    BLACKSKY_BASE_URL,
    AUTH_TOKEN as BLACKSKY_AUTH_TOKEN,
)
from core.services.planet_catalog_api import (
    upload_to_s3 as planet_upload_to_s3,
    API_KEY as PLANET_API_KEY,
)
from core.services.capella_master_collector import (
    upload_to_s3 as capella_upload_to_s3,
    get_access_token,
    USERNAME as CAPELLA_USERNAME,
    PASSWORD as CAPELLA_PASSWORD,
    API_URL as CAPELLA_API_URL,
)


def get_airbus_record_images_by_ids(ids: List[str]):
    try:
        access_token = get_acces_token()
        search_headers = {
            "Authorization": f"Bearer {access_token}",
            "Cache-Control": "no-cache",
        }
        SEARCH_API_ENDPOINT = (
            "https://search.foundation.api.oneatlas.airbus.com/api/v2/opensearch"
        )

        SEARCH_API_ENDPOINT = f"{SEARCH_API_ENDPOINT}?id={",".join(ids)}"
        all_images = []
        response = requests.get(SEARCH_API_ENDPOINT, headers=search_headers)
        if response.status_code == 200:
            response_data = response.json()
            for feature in response_data["features"]:
                all_images.append(
                    {
                        "url": feature.get("_links", {})
                        .get("thumbnail", {})
                        .get("href"),
                        "id": feature.get("properties").get("id"),
                    }
                )
        def process_image(image):
            headers = {"Authorization": "Bearer " + access_token}
            try:
                response = requests.get(
                    image.get("url"), headers=headers, stream=True, timeout=(10, 30)
                )
                response.raise_for_status()
                record_id = image.get("id")
                content = response.content
                url = save_image_in_s3_and_get_url(content, record_id, "airbus")
                SatelliteCaptureCatalog.objects.filter(vendor_id=record_id).update(
                    image_uploaded=True
                )
                return url
            except Exception as e:
                logger.error(
                    f"Error processing image with Airbus ID {image.get('id')}: {str(e)}"
                )
                return None

        uploaded_urls = []
        if all_images:
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_image = {executor.submit(process_image, img): img for img in all_images}
                for future in as_completed(future_to_image):
                    result = future.result()
                    if result:
                        uploaded_urls.append(result)

        return {
            "vendor": "airbus",
            "data": uploaded_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Airbus Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "airbus"}


def get_maxar_record_images_by_ids(ids: List[str]):
    try:
        collections = ["wv01", "wv02"]
        collections_str = ",".join(collections)
        ids = [original_id.split("-")[0] for original_id in ids]
        url = f"https://api.maxar.com/discovery/v1/search?ids={','.join(ids)}&collections={collections_str}"
        all_records = []
        headers = {"Accept": "application/json", "MAXAR-API-KEY": AUTH_TOKEN}
        all_urls = []
        try:
            response = requests.request("GET", url, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            all_records = response_data.get("features", [])
            def process_record(feature):
                try:
                    feature_id = feature.get("id") + "-" + feature.get("collection")
                    feature["vendor_id"] = feature_id
                    url = maxar_upload_to_s3(feature, "maxar")  

                    SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                        image_uploaded=True
                    )
                    return url
                except Exception as e:
                    logger.error(f"Error processing feature Maxar {feature_id}: {str(e)}")
                    return None

            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_feature = {executor.submit(process_record, feature): feature for feature in all_records}
                for future in as_completed(future_to_feature):
                    try:
                        result = future.result()
                        if result:
                            all_urls.append(result)
                    except Exception as e:
                        logger.error(f"Error in future processing Maxar: {str(e)}")

        except Exception as e:
            logger.error(f"Error in Maxar Vendor View 1: {str(e)}")

        return {
            "vendor": "maxar",
            "data": all_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Maxar Vendor View 2: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "maxar"} 


def get_blacksky_record_images_by_ids(ids: List[str]):
    try:
        final_images = []
        headers = {"Authorization": BLACKSKY_AUTH_TOKEN}
        def process_feature(feature_id):
            try:
                url = f"{BLACKSKY_BASE_URL}/v1/browse/{feature_id}"
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                s3_url = save_image_in_s3_and_get_url(response.content, feature_id, "blacksky")

                SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                    image_uploaded=True
                )
                return s3_url
            except Exception as e:
                logger.error(f"Error processing feature Blacksky {feature_id}: {str(e)}")
                return None

        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_feature = {executor.submit(process_feature, feature_id): feature_id for feature_id in ids}
            for future in as_completed(future_to_feature):
                try:
                    result = future.result()
                    if result:
                        final_images.append(result)
                except Exception as e:
                    logger.error(f"Error in future processing Blacksky: {str(e)}")

        return {
            "vendor": "blacksky",
            "data": final_images,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Blacksky Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "blacksky"}


def get_planet_record_images_by_ids(ids: List[str]):
    try:
        item_type = "SkySatCollect"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "api-key " + PLANET_API_KEY,
        }
        def process_item(item_id):
            try:
                # Fetch the item details
                search_endpoint = (
                    f"https://api.planet.com/data/v1/item-types/{item_type}/items/{item_id}"
                )
                response = requests.get(search_endpoint, headers=headers)
                response.raise_for_status()
                feature = response.json()

                # Extract feature ID and upload to S3
                feature_id = feature.get("id")
                url = planet_upload_to_s3(feature, "planet")
                
                # Update the database
                if url:
                    SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                        image_uploaded=True
                    )
                return url
            except Exception as e:
                logger.error(f"Error processing Planet item {item_id}: {str(e)}")
                return None

        # Use ThreadPoolExecutor for concurrent processing
        final_urls = []
        with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
            future_to_item = {executor.submit(process_item, item_id): item_id for item_id in ids}
            for future in as_completed(future_to_item):
                try:
                    result = future.result()
                    if result:
                        final_urls.append(result)
                except Exception as e:
                    logger.error(f"Error in future processing Planet: {str(e)}")

        return {
            "vendor": "planet",
            "data": final_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Planet Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "planet"}


def get_capella_record_images_by_ids(ids: List[str]):
    try:
        access_token = get_access_token(CAPELLA_USERNAME, CAPELLA_PASSWORD)
        access_token = access_token.get("accessToken")
        request_body = {
            "ids": ids,
            "fields": {
                "include": [
                    "id",
                    "assets:thumbnail",
                ]
            },
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        response = requests.post(CAPELLA_API_URL, json=request_body, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        all_features = []
        final_urls = []
        if response_json.get("features"):
            all_features = response_json.get("features")
            def process_feature(feature):
                try:
                    feature_id = feature.get("id")
                    thumbnail_url = feature.get("assets", {}).get("thumbnail", {}).get("href")
                    if not thumbnail_url:
                        logger.error(f"No thumbnail URL found for feature {feature_id}")
                        return None

                    # Upload to S3
                    record = {"id": feature_id, "thumbnail_url": thumbnail_url}
                    url = capella_upload_to_s3(record, "capella")

                    # Update database
                    if url:
                        SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                            image_uploaded=True
                        )
                    return url
                except Exception as e:
                    logger.error(f"Error processing feature {feature.get('id')}: {str(e)}")
                    return None

            # Use ThreadPoolExecutor for concurrent processing
            
            with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
                future_to_feature = {executor.submit(process_feature, feature): feature for feature in all_features}
                for future in as_completed(future_to_feature):
                    try:
                        result = future.result()
                        if result:
                            final_urls.append(result)
                    except Exception as e:
                        logger.error(f"Error in future processing: {str(e)}")
        return {
            "vendor": "capella",
            "data": final_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Capella Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "capella"}


def get_image_url_by_vendor_name_and_id(data: dict):
    vendor_data = {}
    for item in data:
        vendor_name = item.get("vendor")
        vendor_id = item.get("id")
        if vendor_name not in vendor_data:
            vendor_data[vendor_name] = []
        vendor_data[vendor_name].append(vendor_id)

    def fetch_images(vendor_name, vendor_ids):
        if vendor_name == "airbus":
            return {vendor_name: get_airbus_record_images_by_ids(vendor_ids)}
        elif vendor_name == "maxar":
            return {vendor_name: get_maxar_record_images_by_ids(vendor_ids)}
        elif vendor_name == "blacksky":
            return {vendor_name: get_blacksky_record_images_by_ids(vendor_ids)}
        elif vendor_name == "planet":
            return {vendor_name: get_planet_record_images_by_ids(vendor_ids)}
        elif vendor_name == "capella":
            return {vendor_name: get_capella_record_images_by_ids(vendor_ids)}
        else:
            return {vendor_name: {"data": "Vendor not supported", "status_code": 400}}

    final_data = {}
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_images, vendor_name, vendor_ids)
            for vendor_name, vendor_ids in vendor_data.items()
        ]

        for future in futures:
            try:
                result = future.result()
                final_data.update(result)
            except Exception as e:
                final_data["error"] = f"Error fetching images: {str(e)}"
    print(final_data)
    return final_data