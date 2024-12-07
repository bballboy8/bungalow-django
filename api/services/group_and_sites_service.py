from api.models.group_and_sites_models import *
from logging_module import logger
import shapely.wkt
from pyproj import Geod
from api.services.area_service import convert_geojson_to_wkt
from core.models import SatelliteCaptureCatalog
from django.contrib.gis.geos import Polygon

def get_all_sites():
    logger.info("Fetching all sites")
    try:
        sites = Site.objects.all()
        if not sites:
            logger.warning("No sites found")
            return {"data": [], "message": "No sites found", "status_code": 404}
        logger.info("Sites fetched successfully")
        return {
            "data": sites,
            "message": "Sites fetched successfully",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error fetching sites: {str(e)}")
        return {"data": [], "message": "Error fetching sites", "status_code": 500}



def get_group_hierarchy_recursive(group_id):
    """
    Retrieve the hierarchy of a group, including all subgroups recursively.
    """
    try:
        logger.info(f"Fetching group hierarchy for group ID: {group_id}")
        group = Group.objects.filter(id=group_id).first()
        if not group:
            return {"error": "Group not found", "status_code": 404}

        def fetch_subgroups(group):
            subgroups = Group.objects.filter(parent=group)
            return [
                {
                    "id": subgroup.id,
                    "name": subgroup.name,
                    "description": subgroup.description,
                    "subgroups": fetch_subgroups(subgroup),  # Recursive call
                }
                for subgroup in subgroups
            ]

        return {
            "id": group.id,
            "name": group.name,
            "description": group.description,
            "subgroups": fetch_subgroups(group),
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error fetching group hierarchy: {str(e)}")
        return {"error": str(e), "status_code": 500}


def assign_site_to_group(group, site):
    """
    Assign a site to a group.
    """
    try:
        logger.info(f"Assigning site {site.name} to group {group.name}")
        GroupSite.objects.create(group=group, site=site, site_area=site.site_area)
        return {
            "message": "Site assigned to group",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error assigning site to group: {str(e)}")
        return {
            "data": None,
            "message": f"Error assigning site to group {e}",
            "status_code": 500,
        }


def get_sites_in_group(group_id):
    """
    Get all sites assigned to a specific group.
    """
    try:
        logger.info(f"Fetching sites for group ID: {group_id}")
        group_sites = GroupSite.objects.filter(group_id=group_id).select_related("site")
        sites = [{"id": gs.site.id, "name": gs.site.name} for gs in group_sites]
        return {
            "data": sites,
            "message": "Sites fetched successfully",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error fetching sites for group: {str(e)}")
        return {
            "data": [],
            "message": f"Error fetching sites for group: {str(e)}",
            "status_code": 500,
        }

def get_subgroups_recursive(group):
    """
    Recursively retrieve all subgroups for a given group, including indirect subgroups.
    """
    subgroups = Group.objects.filter(parent=group)
    all_subgroups = list(subgroups)
    
    # Recursively get subgroups of each subgroup
    for subgroup in subgroups:
        all_subgroups.extend(get_subgroups_recursive(subgroup))
    
    return all_subgroups

    
def total_surface_area_of_group_and_its_subgroups(group_id):
    """
    Calculate the total surface area of a group and its subgroups.
    """
    try:
        logger.info(f"Calculating total surface area for group ID: {group_id}")
        group = Group.objects.filter(id=group_id).first()
        if not group:
            return {"error": "Group not found", "status_code": 404}
        
        # Get all subgroups recursively
        all_groups = [group] + get_subgroups_recursive(group)

        # Now go through each group and calculate the total surface area
        total_surface_area = 0
        total_objects = 0
        for group in all_groups:
            sites = GroupSite.objects.filter(group=group)
            for site in sites:
                total_surface_area += site.site_area
                coordinates = site.site.coordinates_record['coordinates'][0]
                polygon = Polygon(coordinates)
                count = SatelliteCaptureCatalog.objects.filter(location_polygon__intersects=polygon).count()
                total_objects += count
        
        return {
            "data": {"total_surface_area": total_surface_area, "total_objects": total_objects},
            "message": "Total surface area calculated successfully",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error calculating total surface area: {str(e)}")
        return {
            "error": str(e),
            "message": "Error calculating total surface area",
            "status_code": 500,
        }


def get_parent_groups_with_details():
    """
    Get all parent groups with their details.
    """
    try:
        logger.info("Fetching parent groups with details")
        parent_groups = Group.objects.filter(parent=None)
        groups = []
        for group in parent_groups:
            area_response = total_surface_area_of_group_and_its_subgroups(group.id)
            groups.append(
                {
                    "id": group.id,
                    "name": group.name,
                    "created_at": group.created_at,
                    "surface_area": area_response["data"]["total_surface_area"],
                    "total_objects": area_response["data"]["total_objects"],
                }
            )
        return {
            "data": groups,
            "message": "Parent groups fetched successfully",
            "status_code": 200,
        }
    except Exception as e:
        logger.error(f"Error fetching parent groups: {str(e)}")
        return {
            "data": [],
            "message": f"Error fetching parent groups: {str(e)}",
            "status_code": 500,
        }
    
def get_area_from_geojson(geometry):
    """
    Calculate the area of a site from its GeoJSON coordinates record.
    """
    try:
        response = convert_geojson_to_wkt(geometry)
        geod = Geod(ellps="WGS84")
        polygon = shapely.wkt.loads(response["data"])
        area = round(abs(geod.geometry_area_perimeter(polygon)[0]) / 1000000.0, 2)
        return {"area": area, "status_code": 200}
    except Exception as e:
        logger.error(f"Error calculating area from GeoJSON: {str(e)}")
        return {"area": 0, "status_code": 500}