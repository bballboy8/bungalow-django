# api/tasks.py
from celery import shared_task
from api.services.vendor_service import *


@shared_task
def run_image_seeder(captures):
    try:
        final_output = []
        if captures.get("blacksky"):
            blacksky_captures = captures.get("blacksky")
            get_blacksky_record_images_by_ids(blacksky_captures)
            final_output.append({"message": "Blacksky images seeded successfully"})
        if captures.get("maxar"):
            maxar_captures = captures.get("maxar")
            get_maxar_record_images_by_ids(maxar_captures)
            final_output.append({"message": "Maxar images seeded successfully"})
        if captures.get("airbus"):
            airbus_captures = captures.get("airbus")
            get_airbus_record_images_by_ids(airbus_captures)
            final_output.append({"message": "Airbus images seeded successfully"})
        if captures.get("planet"):
            planet_captures = captures.get("planet")
            get_planet_record_images_by_ids(planet_captures)
            final_output.append({"message": "Planet images seeded successfully"})
        if captures.get("capella"):
            capella_captures = captures.get("capella")
            get_capella_record_images_by_ids(capella_captures)
            final_output.append({"message": "Capella images seeded successfully"})
        
        return final_output
    except Exception as e:
        return f"{str(e)}"