from typing import List
from logging_module import logger
import requests
from core.services.airbus_catalog_api import get_acces_token
from core.utils import save_image_in_s3_and_get_url
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
        uploaded_urls = []
        if all_images:
            headers = {"Authorization": "Bearer " + access_token}
            for image in all_images:
                response = requests.get(
                    image.get("url"), headers=headers, stream=True, timeout=(10, 30)
                )
                response.raise_for_status()
                record_id = image.get("id")
                content = response.content
                url = save_image_in_s3_and_get_url(content, record_id, "airbus")
                try:
                    SatelliteCaptureCatalog.objects.filter(vendor_id=record_id).update(
                        image_uploaded=True
                    )
                    uploaded_urls.append(url)
                except Exception as e:
                    logger.error(
                        f"Error in updating the image_uploaded field in SatelliteCaptureCatalog: {str(e)}"
                    )

        return {
            "data": uploaded_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Airbus Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}


def get_maxar_record_images_by_ids(ids: List[str]):
    try:
        collections = ["wv01", "wv02"]
        collections_str = ",".join(collections)
        ids = [original_id.split("-")[0] for original_id in ids]
        url = f"https://api.maxar.com/discovery/v1/search?ids={','.join(ids)}&collections={collections_str}"
        all_records = []
        headers = {"Accept": "application/json", "MAXAR-API-KEY": AUTH_TOKEN}
        logger.debug(f"Maxar API URL: {url}")
        logger.debug(f"Maxar API Headers: {headers}")
        all_urls = []
        try:
            response = requests.request("GET", url, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            all_records = response_data.get("features", [])
            logger.debug(f"Maxar API Response: {all_records}")
            for feature in all_records:
                feature_id = feature.get("id") + "-" + feature.get("collection")
                feature["vendor_id"] = feature_id
                url = maxar_upload_to_s3(feature, "maxar")
                print(url)
                all_urls.append(url)
                try:
                    SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                        image_uploaded=True
                    )
                except Exception as e:
                    logger.error(
                        f"Error in updating the image_uploaded field in SatelliteCaptureCatalog: {str(e)}"
                    )

        except Exception as e:
            logger.error(f"Error in Maxar Vendor View 1: {str(e)}")

        return {
            "data": all_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Maxar Vendor View 2: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}


def get_blacksky_record_images_by_ids(ids: List[str]):
    try:
        final_images = []
        headers = {"Authorization": BLACKSKY_AUTH_TOKEN}
        for feature_id in ids:
            try:
                url = f"{BLACKSKY_BASE_URL}/v1/browse/{feature_id}"
                response = requests.get(url, headers=headers)
                url = save_image_in_s3_and_get_url(
                    response.content, feature_id, "blacksky"
                )
                final_images.append(url)
                try:
                    SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                        image_uploaded=True
                    )
                except Exception as e:
                    logger.error(
                        f"Error in updating the image_uploaded field in SatelliteCaptureCatalog: {str(e)}"
                    )
            except Exception as e:
                logger.error(f"Error in Blacksky Vendor View: {str(e)}")

        return {
            "data": final_images,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Blacksky Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}


def get_planet_record_images_by_ids(ids: List[str]):
    try:
        item_type = "SkySatCollect"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "api-key " + PLANET_API_KEY,
        }
        all_features = []
        for item_id in ids:
            search_endpoint = (
                f"https://api.planet.com/data/v1/item-types/{item_type}/items/{item_id}"
            )
            try:
                response = requests.get(search_endpoint, headers=headers)
                response.raise_for_status()
                response_json = response.json()
                all_features.append(response_json)
            except Exception as e:
                logger.error(f"Error in Planet Vendor View: {str(e)}")
        for feature in all_features:
            feature_id = feature.get("id")
            try:
                url = planet_upload_to_s3(feature, "planet")
                if url:
                    SatelliteCaptureCatalog.objects.filter(vendor_id=feature_id).update(
                        image_uploaded=True
                    )
            except Exception as e:
                logger.error(
                    f"Error in updating the image_uploaded field in SatelliteCaptureCatalog: {str(e)}"
                )

        return {
            "data": "Planet record images successfully retrieved.",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Planet Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}


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
        if response_json.get("features"):
            all_features = response_json.get("features")
            for feature in all_features:
                feature_id = feature.get("id")
                record = {
                    "id": feature_id,
                    "thumbnail_url": feature.get("assets", {})
                    .get("thumbnail", {})
                    .get("href"),
                }
                try:
                    url = capella_upload_to_s3(record, "capella")
                    if url:
                        SatelliteCaptureCatalog.objects.filter(
                            vendor_id=feature_id
                        ).update(image_uploaded=True)
                except Exception as e:
                    logger.error(
                        f"Error in updating the image_uploaded field in SatelliteCaptureCatalog: {str(e)}"
                    )
        return {
            "data": "Capella record images successfully retrieved.",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Capella Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500}
