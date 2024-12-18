# api/tasks.py
from celery import shared_task
from api.services.vendor_service import *
from concurrent.futures import ThreadPoolExecutor, as_completed


@shared_task
def run_image_seeder(captures):
    try:
        final_output = []
        futures = []
        vendor_map = {
            "blacksky": get_blacksky_record_images_by_ids,
            "maxar": get_maxar_record_images_by_ids,
            "airbus": get_airbus_record_images_by_ids,
            "planet": get_planet_record_images_by_ids,
            "capella": get_capella_record_images_by_ids,
            "skyfi-umbra": get_skyfi_record_images_by_ids
        }
        
        # Define a thread pool
        with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
            for vendor, func in vendor_map.items():
                if captures.get(vendor):
                    capture_ids = captures.get(vendor)
                    futures.append(
                        executor.submit(func, capture_ids)
                    )
                    final_output.append({
                        "vendor": vendor,
                        "message": f"{vendor.capitalize()} images seeding initiated",
                        "ids": capture_ids
                    })
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    final_output.append({
                        "details": result
                    })
                except Exception as e:
                    final_output.append({"error": str(e)})
        
        return final_output
    except Exception as e:
        return f"Error occurred: {str(e)}"