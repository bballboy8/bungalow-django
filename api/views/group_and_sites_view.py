from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from api.models import Group
from api.serializers.group_and_sites_serializer import *
from api.services.group_and_sites_service import *
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiParameter
from api.parameters.group_and_sites_parameters import *


class GetGroupsForAssignmentAndSearchingView(APIView):

    @extend_schema(
        description="Retrieve all groups or the full hierarchy of a specific group",
        parameters=group_search_for_assignment_parameters,
        responses={
            200: OpenApiResponse(
                description="Group hierarchy successfully retrieved.",
            ),
            404: OpenApiResponse(description="Group not found "),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def get(self, request):
        """
        Retrieve all groups or the full hierarchy of a specific group.
        """
        try:
            group_id = request.query_params.get("group_id")
            group_name = request.query_params.get("group_name")
            logger.info(
                f"Fetching group hierarchy for group ID: {group_id}, Name: {group_name}"
            )

            def get_full_hierarchy(group):
                """
                Recursive function to build the hierarchy of a group and its subgroups.
                """
                children = Group.objects.filter(parent=group)
                return {
                    "id": group.id,
                    "name": group.name,
                    "parent": group.parent.id if group.parent else None,
                    "subgroups": [get_full_hierarchy(child) for child in children],
                }

            if group_id:
                group = Group.objects.filter(id=group_id).first()
                if not group:
                    return Response(
                        {"error": "Group not found."},
                        status=status.HTTP_404_NOT_FOUND,
                    )

                group_hierarchy = get_full_hierarchy(group)

                if group_name:

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

                    filtered_hierarchy = filter_subgroups_by_name(
                        group_hierarchy, group_name
                    )

                    if not filtered_hierarchy:
                        return Response(
                            {"error": "No matching subgroup found."},
                            status=status.HTTP_404_NOT_FOUND,
                        )
                    return Response(filtered_hierarchy, status=status.HTTP_200_OK)

                return Response(group_hierarchy, status=status.HTTP_200_OK)

            if group_name:
                matching_groups = Group.objects.filter(name__icontains=group_name)
                if not matching_groups.exists():
                    return Response(
                        {"error": "No groups found with the given name."},
                        status=status.HTTP_404_NOT_FOUND,
                    )

                def get_top_level_parent(group):
                    while group.parent:
                        group = group.parent
                    return group

                top_level_parents = {
                    get_top_level_parent(group) for group in matching_groups
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

                    pruned_subgroups = [
                        subgroup for subgroup in pruned_subgroups if subgroup
                    ]

                    if is_match or pruned_subgroups:
                        return {
                            "id": group_hierarchy["id"],
                            "name": group_hierarchy["name"],
                            "subgroups": pruned_subgroups,
                        }
                    return None

                matched_ids = set(group.id for group in matching_groups)
                results = [
                    prune_hierarchy(get_full_hierarchy(parent), matched_ids)
                    for parent in top_level_parents
                ]

                results = [result for result in results if result]

                return Response(results, status=status.HTTP_200_OK)

            # Default behavior: Retrieve all top-level groups
            groups = Group.objects.filter(parent=None)
            serializer = GroupSerializer(groups, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error fetching group hierarchy: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AddGroupView(APIView):

    @extend_schema(
        description="Create a new group",
        request=AddGroupSerializer,
        responses={
            201: OpenApiResponse(
                description="Group created successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def post(self, request):
        try:
            serializer = GroupSerializer(data=request.data)
            if serializer.is_valid():
                group = serializer.save()
                return Response(
                    GroupSerializer(group).data, status=status.HTTP_201_CREATED
                )
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error creating group: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GetSiteView(APIView):

    @extend_schema(
        description="Retrieve all sites",
        responses={
            200: OpenApiResponse(
                description="Sites successfully retrieved.",
            ),
            404: OpenApiResponse(description="Sites not found "),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def get(self, request):
        try:
            logger.info("Fetching all sites")
            sites = get_all_sites()
            if sites["status_code"] != 200:
                return Response(sites, status=sites["status_code"])
            serializer = SiteSerializer(sites["data"], many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error fetching sites: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AddSiteView(APIView):
    @extend_schema(
        description="Create a new site",
        request=AddSiteSerializer,
        responses={
            201: OpenApiResponse(
                description="Site created successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def post(self, request):
        try:
            serializer = SiteSerializer(data=request.data)
            if serializer.is_valid():
                site = serializer.save()
                return Response(
                    SiteSerializer(site).data, status=status.HTTP_201_CREATED
                )
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error creating site: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AddGroupSiteView(APIView):

    @extend_schema(
        description="Assign a site to a group or retrieve all sites in a group",
        request=AddGroupSiteSerializer,
        responses={
            200: OpenApiResponse(
                description="Site assigned to group successfully.",
            ),
            400: OpenApiResponse(description="Invalid group ID"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def post(self, request):
        try:
            group_id = request.data.get("group_id")
            site_id = request.data.get("site_id")
            group = Group.objects.filter(id=group_id).first()
            site = Site.objects.filter(id=site_id).first()

            if not group or not site:
                return Response(
                    {"error": "Invalid group or site ID"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Check if the site is already assigned to the group
            assignment = GroupSite.objects.filter(group=group, site=site).first()
            if assignment:
                return Response(
                    {"error": "Site already assigned to the group"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            assignment = assign_site_to_group(group, site)
            if assignment["status_code"] != 200:
                return Response(assignment, status=assignment["status_code"])

            return Response(assignment, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error assigning site to group: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GetGroupSiteView(APIView):
    @extend_schema(
        description="Retrieve all sites in a group",
        parameters=[
            OpenApiParameter(
                name="group_id",
                type=int,
                description="ID of the group to retrieve the sites.",
            )
        ],
        responses={
            200: OpenApiResponse(
                description="Sites in group successfully retrieved.",
            ),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def get(self, request):
        try:
            group_id = request.query_params.get("group_id")
            sites = get_sites_in_group(group_id)
            if sites["status_code"] != 200:
                return Response(sites, status=sites["status_code"])
            serializer = SiteSerializer(sites, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class GetParentGroupsListwithDetailsView(APIView):
    @extend_schema(
        description="Retrieve all parent groups with details",
        responses={
            200: OpenApiResponse(
                description="Parent groups with details successfully retrieved.",
            ),
            404: OpenApiResponse(description="Parent groups not found "),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def get(self, request):
        try:
            logger.info("Fetching all parent groups with details")
            parent_groups = get_parent_groups_with_details()
            if parent_groups["status_code"] != 200:
                return Response(parent_groups, status=parent_groups["status_code"])
            serializer = ParentGroupSerializer(parent_groups["data"], many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error fetching parent groups with details: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class GetAreaFromGeoJsonView(APIView):
    @extend_schema(
        description="Calculate area from GeoJSON",
        request=AreaFromGeoJsonSerializer,
        responses={
            200: OpenApiResponse(
                description="Area calculated successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def post(self, request):
        try:
            serializer = AreaFromGeoJsonSerializer(data=request.data)
            if serializer.is_valid():
                geojson = serializer.validated_data["coordinates_record"]
                area = get_area_from_geojson(geojson)
                return Response({"area": area}, status=status.HTTP_200_OK)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error calculating area from GeoJSON: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )