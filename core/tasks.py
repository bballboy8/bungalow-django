# core/tasks.py
from celery import shared_task
from core.services.blacksky_catalog_api import run_blacksky_catalog_api, run_blacksky_catalog_bulk_api_for_last_35_days_from_now, fetch_and_process_products_records
from core.services.airbus_catalog_api import run_airbus_catalog_api, run_airbus_catalog_api_bulk_for_last_35_days_from_now, fetch_and_process_airbus_products_records
from core.services.planet_catalog_api import run_planet_catalog_api, run_planet_catalog_bulk_api_for_last_35_days_from_now
from core.services.capella_master_collector import run_capella_catalog_api, run_capella_catalog_bulk_api_for_last_35_days_from_now
from core.services.skyfi_catalog_api import run_skyfi_catalog_api, run_skfyfi_catalog_api_bulk_for_last_35_days_from_now
from core.services.maxar_catalog_api import run_maxar_catalog_api, run_maxar_catalog_bulk_api_for_last_35_days_from_now
from api.services.group_and_sites_service import check_updates_in_notification_enabled_groups_for_active_users



@shared_task
def run_all_catalogs():
    try:
        run_blacksky_catalog_api()
    except Exception as e:
        print(f"Error occurred while running BlackSky API: {e}")

    try:
        run_airbus_catalog_api()
    except Exception as e:
        print(f"Error occurred while running Airbus API: {e}")

    try:
        run_planet_catalog_api()
    except Exception as e:
        print(f"Error occurred while running Planet API: {e}")

    try:
        run_capella_catalog_api()
    except Exception as e:
        print(f"Error occurred while running Capella API: {e}")

    try:
        run_maxar_catalog_api()
    except Exception as e:
        print(f"Error occurred while running Maxar API: {e}")

    try:
        run_skyfi_catalog_api()
    except Exception as e:
        print(f"Error occurred while running SkyFi API: {e}")

    try:
        check_updates_in_notification_enabled_groups_for_active_users()
    except Exception as e:
        print(f"Error occurred while checking updates in notification enabled groups: {e}")


@shared_task
def run_fetch_and_process_product_orders():
    try:
        fetch_and_process_products_records()
    except Exception as e:
        print(f"Error occurred while running BlackSky API: {e}")

    try:
        fetch_and_process_airbus_products_records()
    except Exception as e:
        print(f"Error occurred while running Airbus API: {e}")
        
@shared_task
def run_skyfi_umbra_catalog():
    try:
        pass
    except Exception as e:
        print(f"Error occurred while running SkyFi API: {e}")

@shared_task
def run_all_catalogs_bulk_last_35_days():
    try:
        run_blacksky_catalog_bulk_api_for_last_35_days_from_now()
    except Exception as e:
        print(f"Error occurred while running BlackSky API: {e}")

    try:
        run_airbus_catalog_api_bulk_for_last_35_days_from_now()
    except Exception as e:
        print(f"Error occurred while running Airbus API: {e}")

    try:
        run_planet_catalog_bulk_api_for_last_35_days_from_now()
    except Exception as e:
        print(f"Error occurred while running Planet API: {e}")

    try:
        run_capella_catalog_bulk_api_for_last_35_days_from_now()
    except Exception as e:
        print(f"Error occurred while running Capella API: {e}")

    try:
        run_maxar_catalog_bulk_api_for_last_35_days_from_now()
    except Exception as e:
        print(f"Error occurred while running Maxar API: {e}")

    try:
        run_skfyfi_catalog_api_bulk_for_last_35_days_from_now()
    except Exception as e:
        print(f"Error occurred while running SkyFi API: {e}")

    try:
        check_updates_in_notification_enabled_groups_for_active_users()
    except Exception as e:
        print(f"Error occurred while checking updates in notification enabled groups: {e}")
    