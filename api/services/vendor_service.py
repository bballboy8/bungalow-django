from typing import List
from logging_module import logger
import requests
from core.services.airbus_catalog_api import get_acces_token
from core.utils import save_image_in_s3_and_get_url
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.models import CollectionCatalog, SatelliteDateRetrievalPipelineHistory
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
from core.services.skyfi_catalog_api import API_KEY as SKYFI_API_KEY
from django.urls import reverse
from django.db.models import Q
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import Sum



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
                url = image.get("url") + "?width=512"
                response = requests.get(
                    image.get("url"), headers=headers, stream=True, timeout=(10, 30)
                )
                response.raise_for_status()
                record_id = image.get("id")
                content = response.content
                url = save_image_in_s3_and_get_url(content, record_id, "airbus")
                CollectionCatalog.objects.filter(vendor_id=record_id).update(
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
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "airbus", "error": f"{str(e)}"}


def get_maxar_record_images_by_ids(ids: List[str]):
    try:
        collections = [ "ge01", "wv01", "wv02", "wv03-vnir", "wv04", "lg01", "lg02"]
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

                    if url:
                        CollectionCatalog.objects.filter(vendor_id=feature_id).update(
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

        print(all_urls)

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

                CollectionCatalog.objects.filter(vendor_id=feature_id).update(
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
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "blacksky", "error": f"{str(e)}"}


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
                    CollectionCatalog.objects.filter(vendor_id=feature_id).update(
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
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "planet", "error": f"{str(e)}"}

def capella_celery_processing(all_features, final_urls):
    def process_feature(feature):
                try:
                    feature_id = feature.get("id")
                    thumbnail_url = feature.get("assets", {}).get("thumbnail", {}).get("href")
                    if not thumbnail_url:
                        logger.error(f"No thumbnail URL found for feature {feature_id}")
                        pass

                    # Upload to S3
                    record = {"id": feature_id, "thumbnail_url": thumbnail_url}

                    url = capella_upload_to_s3(record, "capella")

                    # Update database
                    if url:
                        CollectionCatalog.objects.filter(vendor_id=feature_id).update(
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


def get_capella_record_thumbnails_by_ids(ids: List[str]):
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
        thumbnail_urls = []

        if response_json.get("features"):
            all_features = response_json.get("features")
            thumbnail_urls = [{"id": feature.get("id"), "thumbnail": feature.get("assets", {}).get("thumbnail", {}).get("href")} for feature in all_features]
            
        return {
            "vendor": "capella",
            "data": thumbnail_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Capella Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "capella", "error": f"{str(e)}"}


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
            capella_celery_processing(all_features, final_urls)
            
        return {
            "vendor": "capella",
            "data": final_urls,
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Capella Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "capella", "error": f"{str(e)}"}


def generate_proxy_url(request, vendor_name, vendor_id):
    return request.build_absolute_uri(
        reverse('proxy_image') + f"?vendor_name={vendor_name}&vendor_id={vendor_id}"
    )

def get_skyfi_record_thumbnails_by_ids(ids: List[str]):
    try:
        url = "https://app.skyfi.com/platform-api/archives"
        headers = {"X-Skyfi-Api-Key": SKYFI_API_KEY, "Content-Type": "application/json"}
        all_archives = []

        for archive_id in ids:
            try:
                response = requests.get(f"{url}/{archive_id}", headers=headers)
                response.raise_for_status()
                archive_data = response.json()
                image_url = list(archive_data.get("thumbnailUrls").values())[0]
                all_archives.append({
                    "id": archive_id,
                    "thumbnail": image_url,
                })
            except Exception as e:
                logger.error(f"Error fetching archive {archive_id}: {str(e)}")
                pass

        return {
            "vendor": "skyfi-umbra",
            "data": all_archives,
            "status_code": 200,
        }

    except Exception as e:
        logger.error(f"Error in Blacksky Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "skyfi-umbra", "error": f"{str(e)}"}


def get_skyfi_record_images_by_ids(ids: List[str]):
    try:
        response = get_skyfi_record_thumbnails_by_ids(ids)

        if response.get("status_code") != 200:
            return response

        all_archives = response.get("data")

        def process_archive(archive):
            try:
                response = requests.get(
                    archive.get("thumbnail"), stream=True, timeout=(10, 30)
                )
                response.raise_for_status()
                content = response.content
                filename = archive.get("id")
                response_url = save_image_in_s3_and_get_url(
                    content, filename, "skyfi-umbra"
                )
                if response_url:
                    CollectionCatalog.objects.filter(vendor_id=filename).update(
                        image_uploaded=True
                    )
                archive["image_url"] = response_url
            except Exception as e:
                logger.error(
                    f"Error fetching image for archive {archive.get('id')}: {str(e)}"
                )
            return archive

        final_urls = []

        with ThreadPoolExecutor(
            max_workers=4
        ) as executor:  # Adjust max_workers based on your requirements
            futures = [
                executor.submit(process_archive, archive) for archive in all_archives
            ]

            for future in as_completed(futures):
                try:
                    archive = (
                        future.result()
                    )
                    if archive.get("image_url"):
                        final_urls.append(archive["image_url"])
                except Exception as e:
                    logger.error(f"Exception in processing archive: {str(e)}")

        return {"data": final_urls, "status_code": 200}
    except Exception as e:
        logger.error(f"Error in Blacksky Vendor View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "vendor": "skyfi-umbra", "error": f"{str(e)}"}

from django.db.models.expressions import RawSQL


def get_collection_history(
        start_date: str,
        end_date: str,
        vendor_name: str,
        page_number: int,
        page_size: int,
):
    try:
        query_filter = Q()

        if vendor_name and "," in vendor_name:
            vendor_names = vendor_name.split(",")
            query_filter &= Q(vendor_name__in=vendor_names)
        elif vendor_name:
            query_filter &= Q(vendor_name=vendor_name)

        if start_date:
            query_filter &= Q(created_at__gte=start_date)

        if end_date:
            query_filter &= Q(created_at__lte=end_date)

        print(query_filter)

        # Extract counts from JSONField (PostgreSQL-specific)
        summary_data = SatelliteDateRetrievalPipelineHistory.objects.filter(query_filter) \
            .extra(select={'date': "DATE(created_at)"}) \
            .values('date') \
            .annotate(
                total_success=Sum(
                    RawSQL("CAST(message->>'valid_records' AS INTEGER)", [])
                ),
                total_failed=Sum(
                    RawSQL("CAST(message->>'invalid_records' AS INTEGER)", [])
                ),
                total=Sum(
                    RawSQL("CAST(message->>'total_records' AS INTEGER)", [])
                )
            ) \
            .order_by('-date')

        # Paginate by dates
        paginator = Paginator(summary_data, page_size)
        try:
            paginated_dates = paginator.page(page_number)
        except PageNotAnInteger:
            paginated_dates = paginator.page(1)
        except EmptyPage:
            paginated_dates = []

        date_list = list(paginated_dates)
        for entry in date_list:
            entry_date = entry["date"]
            entry["records"] = list(
                SatelliteDateRetrievalPipelineHistory.objects.filter(
                    query_filter, created_at__date=entry_date
                )
                .order_by('-created_at')
                .values("id", "vendor_name", "created_at", "message")
            )
            # update ke created_at to start_datetime 
            entry['records'] = list(map(lambda x: {**x, 'start_datetime': x['created_at']}, entry['records']))


        return {
            "data": {
                "records": list(paginated_dates),
                "total_dates": paginator.count,  # Total number of unique dates
                "total_pages": paginator.num_pages,
                "page_number": page_number,
                "page_size": page_size,
            },
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error in Collection History View: {str(e)}")
        return {"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}