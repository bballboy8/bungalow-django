from api.models.group_and_sites_models import *
from logging_module import logger
import shapely.wkt
from pyproj import Geod
from api.services.area_service import convert_geojson_to_wkt
from core.models import SatelliteCaptureCatalog
from django.contrib.gis.geos import Polygon
from django.db.models import Count
from datetime import datetime, timedelta
from django.db.models.functions import TruncDate
from django.db.models import Q


def get_all_sites(user_id, name=None, page_number: int = 1, per_page: int = 10, site_id=None, group_id=None):
    logger.info("Fetching all sites")
    try:
        query = Q(user__id=user_id, is_deleted=False)

        if name:
            query &= Q(name__icontains=name)
        if site_id:
            query &= Q(id=site_id)
        if group_id:
            site_ids = GroupSite.objects.filter(group_id=group_id).values_list('site_id', flat=True)
            query &= Q(id__in=site_ids)

        sites = Site.objects.filter(query)
        sites = sites.order_by("id")[(page_number - 1) * per_page : page_number * per_page]

        final_sites = []

        for site in sites:
            coordinates = site.coordinates_record["coordinates"][0]
            polygon = Polygon(coordinates)
            captures = SatelliteCaptureCatalog.objects.filter(location_polygon__intersects=polygon)

            total_records = captures.count()
            most_recent_capture = captures.order_by("-acquisition_datetime").first()
            most_recent_clear_capture = captures.filter(cloud_cover=0).order_by("-acquisition_datetime").first()

            if not most_recent_capture:
                logger.warning(f"No captures found for site {site.id}")
                records_per_acquisition = 0
                time_between_acquisitions = 0
            else:
                latest_capture_date = most_recent_capture.acquisition_datetime.date()
                latest_day_captures = captures.filter(acquisition_datetime__date=latest_capture_date)
                prior_day_captures = captures.filter(acquisition_datetime__date__lt=latest_capture_date)

                # Calculate records per acquisition
                acquisitions_today = latest_day_captures.values("acquisition_datetime").distinct().count()
                records_per_acquisition = latest_day_captures.count() / acquisitions_today if acquisitions_today else 0

                # Calculate time between acquisitions
                if prior_day_captures.exists():
                    last_prior_capture = prior_day_captures.order_by("-acquisition_datetime").first()
                    first_latest_capture = latest_day_captures.order_by("acquisition_datetime").first()
                    time_between_acquisitions = (
                        first_latest_capture.acquisition_datetime - last_prior_capture.acquisition_datetime
                    ).total_seconds() / 86400 
                else:
                    time_between_acquisitions = 0

            start_date = (datetime.now() - timedelta(days=30))
            end_date = datetime.now()
            # Generate heatmap
            heatmap = (
                captures.filter(acquisition_datetime__gte=start_date)
                .annotate(date=TruncDate("acquisition_datetime"))  # Truncate datetime to date
                .values("date")  # Group by date
                .annotate(count=Count("acquisition_datetime"))  # Count records for each date
                .order_by("date")  # Sort by date
            )


            heatmap_dict = {entry["date"]: entry["count"] for entry in heatmap}
            # Fill in missing dates with zero counts
            heatmap_data = []
            current_date = start_date.date()
            end_date = end_date.date()

            while current_date <= end_date:
                heatmap_data.append({"date": current_date.strftime("%Y-%m-%d"), "count": heatmap_dict.get(current_date, 0)})
                current_date += timedelta(days=1)

            final_sites.append(
                {
                    "id": site.id,
                    "name": site.name,
                    "area": site.site_area,
                    "acquisition_count": total_records,
                    "most_recent": most_recent_capture.acquisition_datetime if most_recent_capture else None,
                    "most_recent_clear": most_recent_clear_capture.acquisition_datetime if most_recent_clear_capture else None,
                    "heatmap": heatmap_data,
                    "frequency": records_per_acquisition,
                    "gap": time_between_acquisitions,
                    "site_type": site.site_type,
                    "notification": site.notification,
                }
            )

        total_count = Site.objects.filter(user__id=user_id).count()

        logger.info("Sites fetched successfully")
        return {
            "data": final_sites,
            "page_number": page_number,
            "per_page": per_page,
            "total_count": total_count,
            "message": "Sites fetched successfully",
            "status_code": 200,
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error fetching sites {str(e)}")
        return {
            "data": [],
            "message": f"Error fetching sites: {str(e)}",
            "status_code": 500,
            "error": f"Error fetching sites: {str(e)}",
        }

def get_group_hierarchy_recursive(group_id):
    """
    Retrieve the hierarchy of a group, including all subgroups recursively.
    """
    try:
        logger.info(f"Fetching group hierarchy for group ID: {group_id}")
        group = Group.objects.filter(id=group_id, is_deleted=False).first()
        if not group:
            return {"error": "Group not found", "status_code": 404}

        def fetch_subgroups(group):
            subgroups = Group.objects.filter(parent=group, is_deleted=False)
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


def assign_site_to_group(group, site, user_id):
    """
    Assign a site to a group.
    """
    try:
        logger.info(f"Assigning site {site.name} to group {group.name}")
        user_id = User.objects.get(id=user_id) # Get user object
        GroupSite.objects.create(group=group, site=site, site_area=site.site_area, user=user_id)
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
            "error": f"Error assigning site to group: {str(e)}",
        }


def get_sites_in_group(group_id, user_id):
    """
    Get all sites assigned to a specific group.
    """
    try:
        logger.info(f"Fetching sites for group ID: {group_id}")
        group_sites = GroupSite.objects.filter(group_id=group_id, user__id=user_id, is_deleted=False).select_related("site")
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
            "error": f"Error fetching sites for group: {str(e)}",
        }

def get_subgroups_recursive(group):
    """
    Recursively retrieve all subgroups for a given group, including indirect subgroups.
    """
    subgroups = Group.objects.filter(parent=group, is_deleted=False)
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
        group = Group.objects.filter(id=group_id, is_deleted=False).first()
        if not group:
            return {"error": "Group not found", "status_code": 404}
        
        # Get all subgroups recursively
        all_groups = [group] + get_subgroups_recursive(group)

        # Now go through each group and calculate the total surface area
        total_surface_area = 0
        total_objects = 0
        site_objects_count = {}
        for group in all_groups:
            sites = GroupSite.objects.filter(group=group, is_deleted=False)
            if not sites:
                continue
            for site in sites:
                total_surface_area += site.site_area
                coordinates = site.site.coordinates_record['coordinates'][0]
                polygon = Polygon(coordinates)
                count = SatelliteCaptureCatalog.objects.filter(location_polygon__intersects=polygon).count()
                total_objects += count
                site_objects_count[site.site.id] = count
        
        return {
            "data": {"total_surface_area": total_surface_area, "total_objects": total_objects, "site_objects_count": site_objects_count},
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


def get_parent_groups_with_details(user_id, group_name=None):
    """
    Get all parent groups with their details.
    """
    try:
        logger.info("Fetching parent groups with details")
        filters = {"user__id": user_id, "is_deleted": False, "parent": None}
        if group_name:
            filters["name__icontains"] = group_name
        
        parent_groups = Group.objects.filter(**filters)
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
            "error": f"Error fetching parent groups: {str(e)}",
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
        return {"area": 0, "status_code": 500, "error": f"Error calculating area from GeoJSON: {str(e)}"}


def get_full_hierarchy(group):
    """
    Recursive function to build the hierarchy of a group and its subgroups.
    """
    children = Group.objects.filter(parent=group, is_deleted=False)

    area_response = total_surface_area_of_group_and_its_subgroups(group.id)

    # Get sites assigned to the group
    sites = GroupSite.objects.filter(group=group, is_deleted=False)
    site_details = []
    for site in sites:
        site_details.append(
            {
                "id": site.site.id,
                "name": site.site.name,
                "area": site.site_area,
                "site_type": site.site.site_type,
                "date": site.site.created_at,
                "count": area_response["data"]["site_objects_count"].get(site.site.id, 0),
            }
        )

    return {
        "id": group.id,
        "name": group.name,
        "parent": group.parent.id if group.parent else None,
        "created_at": group.created_at,
        "sites": site_details,
        "surface_area": area_response["data"]["total_surface_area"],
        "total_objects": area_response["data"]["total_objects"],
        "subgroups": [get_full_hierarchy(child) for child in children],
    }


def prune_hierarchy(group_hierarchy, matched_ids):
    """
    Recursively prune the hierarchy to include only matched groups or their ancestors.
    """
    is_match = group_hierarchy["id"] in matched_ids

    pruned_subgroups = [
        prune_hierarchy(subgroup, matched_ids)
        for subgroup in group_hierarchy["subgroups"]
    ]

    pruned_subgroups = [subgroup for subgroup in pruned_subgroups if subgroup]

    if is_match or pruned_subgroups:
        return {
            "id": group_hierarchy["id"],
            "name": group_hierarchy["name"],
            "subgroups": pruned_subgroups,
        }
    return None

def filter_subgroups_by_name(group_hierarchy, name):
    """
    Recursively filters subgroups to retain only those matching the name.
    """
    is_match = name.lower() in group_hierarchy["name"].lower()

    filtered_subgroups = [
        filter_subgroups_by_name(subgroup, name)
        for subgroup in group_hierarchy["subgroups"]
    ]

    filtered_subgroups = [
        subgroup for subgroup in filtered_subgroups if subgroup
    ]

    if is_match or filtered_subgroups:
        return {
            "id": group_hierarchy["id"],
            "name": group_hierarchy["name"],
            "subgroups": filtered_subgroups,
        }
    return None

def get_top_level_parent(group):
    while group.parent:
        group = group.parent
    return group


def group_searching_and_hierarchy_creation(group_id=None, group_name=None, user_id=None):
    try:
        if group_id:
            group = Group.objects.filter(id=group_id, user__id=user_id, is_deleted=False).first()
            if not group:
                return {"error": "Group not found", "status_code": 404, "data":[]}

            group_hierarchy = get_full_hierarchy(group)
            if group_name:
                filtered_hierarchy = filter_subgroups_by_name(group_hierarchy, group_name)

                if not filtered_hierarchy:
                    return {"error": "No matching subgroup found.", "status_code": 404, "data":[]}
                return {"data": filtered_hierarchy, "status_code": 200}

            return {"data": group_hierarchy, "status_code": 200}

        if group_name:
            matching_groups = Group.objects.filter(name__icontains=group_name, user__id=user_id, is_deleted=False)
            if not matching_groups.exists():
                return {"error": "No groups found with the given name.", "status_code": 404, "data":[]}
            top_level_parents = {get_top_level_parent(group) for group in matching_groups}

            matched_ids = set(group.id for group in matching_groups)
            results = [
                prune_hierarchy(get_full_hierarchy(parent), matched_ids)
                for parent in top_level_parents
            ]

            results = [result for result in results if result]

            return {"data": results, "status_code": 200}
    except Exception as e:
        logger.error(f"Error fetching group hierarchy: {str(e)}")
        return {"error": str(e), "status_code": 500, "data":[]}


def get_full_hierarchy_by_group(group):
    """
    Recursive function to build the hierarchy of a group and its subgroups.
    """
    children = Group.objects.filter(parent=group, is_deleted=False)

    area_response = total_surface_area_of_group_and_its_subgroups(group.id)

    # Get sites assigned to the group
    sites = GroupSite.objects.filter(group=group, is_deleted=False)
    site_details = []
    for site in sites:
        site_details.append(
            {
                "id": site.site.id,
                "name": site.site.name,
                "area": site.site_area,
                "site_type": site.site.site_type,
                "date": site.site.created_at,
                "count": area_response["data"]["site_objects_count"].get(site.site.id, 0),
            }
        )

    return {
        "id": group.id,
        "name": group.name,
        "parent": group.parent.id if group.parent else None,
        "created_at": group.created_at,
        "sites": site_details,
        "site_objects_count": area_response["data"]["site_objects_count"],
        "surface_area": area_response["data"]["total_surface_area"],
        "total_objects": area_response["data"]["total_objects"],
        "subgroups": [get_full_hierarchy(child) for child in children],
    }