from api.models.group_and_sites_models import *
from logging_module import logger


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


def create_site(data):
    logger.info("Creating site")
    try:
        site = Site.objects.create(
            name=data.get("name"),
            location_polygon=data.get("location_polygon"),
            coordinates_record=data.get("coordinates_record"),
        )
        logger.info("Site created successfully")
        return {
            "data": site,
            "message": "Site created successfully",
            "status_code": 201,
        }
    except Exception as e:
        logger.error(f"Error creating site: {str(e)}")
        return {"data": None, "message": "Error creating site", "status_code": 500}


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
        assignment = GroupSite.objects.create(group=group, site=site)
        return {
            "data": assignment,
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
