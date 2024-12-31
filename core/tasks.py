# core/tasks.py
from celery import shared_task
from core.services.blacksky_catalog_api import run_blacksky_catalog_api
from core.services.airbus_catalog_api import run_airbus_catalog_api
from core.services.planet_catalog_api import run_planet_catalog_api
from core.services.capella_master_collector import run_capella_catalog_api
from core.services.skyfi_catalog_api import run_skyfi_catalog_api
from core.services.maxar_catalog_api import run_maxar_catalog_api



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

    # try:
    #     run_skyfi_catalog_api()
    # except Exception as e:
    #     print(f"Error occurred while running SkyFi API: {e}")
