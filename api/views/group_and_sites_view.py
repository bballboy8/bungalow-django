from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from api.models import Group
from api.serializers.group_and_sites_serializer import *
from api.services.group_and_sites_service import *
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiParameter
from api.parameters.group_and_sites_parameters import *
from rest_framework.permissions import IsAuthenticated
from api.services.utils import get_user_id_from_token 
from rest_framework.parsers import MultiPartParser, FormParser
import pandas as pd
import json

class GetGroupsForAssignmentAndSearchingView(APIView):
    permission_classes = [IsAuthenticated]

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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 
            group_id = request.query_params.get("group_id")
            group_name = request.query_params.get("group_name")
            logger.info(
                f"Fetching group hierarchy for group ID: {group_id}, Name: {group_name}"
            )

            if group_id or group_name:
                response = group_searching_and_hierarchy_creation(group_id=group_id, group_name=group_name, user_id=user_id)
                if response["status_code"] != 200:
                    return Response(response, status=response["status_code"])
                return Response(response['data'], status=status.HTTP_200_OK)
    
            # Default behavior: Retrieve all top-level groups
            groups = Group.objects.filter(parent=None, is_deleted=False)
            serializer = GroupSerializer(groups, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error fetching group hierarchy: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AddGroupView(APIView):
    permission_classes = [IsAuthenticated]

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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 
            serializer = GroupSerializer(data=request.data, context={"user_id": user_id})
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
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Retrieve all sites",
        parameters=site_search_parameters,
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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"]

            name = request.query_params.get("name")
            page_number = int(request.query_params.get("page_number",1))
            per_page = int(request.query_params.get("per_page", 10))
            site_id = request.query_params.get("site_id")
            group_id = request.query_params.get("group_id")           

            sites = get_all_sites(
                user_id=user_id, name=name, page_number=page_number, per_page=per_page, site_id=site_id, group_id=group_id
            )
            if sites["status_code"] != 200:
                return Response(sites, status=sites["status_code"])
            serializer = GetSiteSerializer(sites["data"], many=True)
            return Response(
                {
                    "data": serializer.data,
                    "total_count": sites["total_count"],
                    "page_number": page_number,
                    "per_page": per_page,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.error(f"Error fetching sites: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class AddSiteView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Create a new site, Site type: Polygon, Rectangle, Point",
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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            # check if site name already exists
            site_name = request.data.get("name")
            site = Site.objects.filter(name=site_name).first()
            if site:
                return Response(
                    {"error": "Site name already exists", "site_id": site.id},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            
            coordinates_record = request.data.get("coordinates_record")

            # area = get_area_from_geojson(coordinates_record)
            # if area["status_code"] != 200:
            #     return Response(area, status=area["status_code"])
            # area = area["area"]
            # if area > 100000:
            #     return Response(
            #         {"error": "Area of the site exceeds 100000 sq. km"},
            #         status=status.HTTP_400_BAD_REQUEST,
            #     )
                

            serializer = SiteSerializer(data=request.data, context={"user_id": user_id})
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
    permission_classes = [IsAuthenticated]

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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            group_id = request.data.get("group_id")
            site_id = request.data.get("site_id")
            group = Group.objects.filter(id=group_id, is_deleted=False).first()
            site = Site.objects.filter(id=site_id).first()

            if not group or not site:
                return Response(
                    {"error": "Invalid group or site ID"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Check if the site is already assigned to the group
            assignment = GroupSite.objects.filter(group=group, site=site, is_deleted=False).first()
            if assignment:
                return Response(
                    {"error": "Site already assigned to the group"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            assignment = assign_site_to_group(group, site, user_id)
            if assignment["status_code"] != 200:
                return Response(assignment, status=assignment["status_code"])

            return Response(assignment, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error assigning site to group: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GetGroupSiteView(APIView):
    permission_classes = [IsAuthenticated]

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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            group_id = request.query_params.get("group_id")
            sites = get_sites_in_group(group_id, user_id)
            if sites["status_code"] != 200:
                return Response(sites, status=sites["status_code"])
            serializer = SiteSerializer(sites, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class GetParentGroupsListwithDetailsView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Retrieve all parent groups with details",
        parameters=[
            OpenApiParameter(
                name="group_name",
                type=str,
                description="Search parent groups by name.",
            )
        ],
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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            group_name = request.query_params.get("group_name")

            parent_groups = get_parent_groups_with_details(user_id=user_id, group_name=group_name)
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
    permission_classes = [IsAuthenticated]

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
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

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

class UpdateSiteView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Update site details",
        request=UpdateSiteSerializer,
        responses={
            200: OpenApiResponse(
                description="Site updated successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def put(self, request):
        try:
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            site_id = request.data.get("site_id")
            site = Site.objects.filter(id=site_id).first()
            if not site:
                return Response(
                    {"error": "Invalid site ID"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            serializer = UpdateSiteSerializer(site, data=request.data, context={"user_id": user_id})
            if serializer.is_valid():
                site = serializer.save()

                # set is_deleted to true in GroupSite if site is deleted
                if site.is_deleted:
                    group_sites = GroupSite.objects.filter(site=site)
                    for group_site in group_sites:
                        group_site.is_deleted = True
                        group_site.save()

                return Response(
                    SiteSerializer(site).data, status=status.HTTP_200_OK
                )
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Error updating site: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class UpdateGroupView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Update group details",
        request=UpdateGroupSerializer,
        responses={
            200: OpenApiResponse(
                description="Group updated successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def put(self, request):
        try:
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            group_id = request.data.get("group_id")
            group = Group.objects.filter(id=group_id, is_deleted=False).first()
            if not group:
                return Response(
                    {"error": "Invalid group ID"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            serializer = UpdateGroupSerializer(group, data=request.data, context={"user_id": user_id})
            if serializer.is_valid():
                group = serializer.save()
                return Response(
                    GroupSerializer(group).data, status=status.HTTP_200_OK
                )
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error updating group: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class RemoveGroupSiteView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Remove a site from a group",
        parameters=[
            OpenApiParameter(
                name="group_site_id",
                type=int,
                description="ID of the site to be removed from group.",
            )
        ],
        responses={
            200: OpenApiResponse(
                description="Site removed from group successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def delete(self, request):
        try:
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            group_site_id = request.query_params.get("group_site_id")
            group_site = GroupSite.objects.filter(id=group_site_id, is_deleted=False).first()
            if not group_site:
                return Response(
                    {"error": "Invalid group site ID"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            group_site.is_deleted = True
            group_site.save()
            return Response(
                {"message": "Site removed from group successfully."},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.error(f"Error removing site from group: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class GetGroupSiteByGroupIdView(APIView):
    permission_classes = [IsAuthenticated]

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
            logger.info("Fetching all sites in a group")
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            group_id = request.query_params.get("group_id")
            # get that group
            group = Group.objects.filter(id=group_id).first()
            sites = get_full_hierarchy_by_group(group)

            return Response(sites, status=status.HTTP_200_OK)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class GetGroupstListWithoutNestingView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        parameters=[
            OpenApiParameter(
                name="search",
                type=str,
                description="Search groups by name.",
            )
        ],
        description="Retrieve all groups without nesting",
        responses={
            200: OpenApiResponse(
                description="Groups successfully retrieved.",
            ),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def get(self, request):
        try:
            logger.info("Fetching all groups without nesting")

            search = request.query_params.get("search")

            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            groups = get_groups_list_without_nesting(search)
            return Response(groups, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error fetching groups without nesting: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class RemoveGroupsandItsNestedGroupAndSitesView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Remove a group and its nested groups and sites",
        parameters=[
            OpenApiParameter(
                name="group_id",
                type=int,
                description="ID of the group to be removed.",
            )
        ],
        responses={
            200: OpenApiResponse(
                description="Group and its nested groups and sites removed successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def delete(self, request):
        try:
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 
            logger.info("Removing group and its nested groups and sites")
            
            group_id = request.query_params.get("group_id")
            response = remove_group_and_its_sites(group_id)
            print(response)
            return Response(
                {"message": "Group and its nested groups and sites removed successfully."},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.error(f"Error removing group and its nested groups and sites: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class SitesFileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Upload a CSV or Excel file to add sites to a group in bulk",
        parameters=[
            OpenApiParameter(
                name="group_id",
                type=int,
                description="ID of the group to which the sites will be added.",
            ),
        ],
        request=UploadFileSerializer,
        responses={
            200: OpenApiResponse(
                description="File processed successfully.",
            ),
            400: OpenApiResponse(description="Invalid input"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )

    def post(self, request, *args, **kwargs):
        file = request.FILES.get("file")
        if not file:
            return Response({"error": "No file provided"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            group_id = request.query_params.get("group_id")
            if not group_id:
                return Response({"error": "Group ID is required"}, status=status.HTTP_400_BAD_REQUEST)
            
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 
            
            # Determine file type and read
            if file.name.endswith(".csv"):
                df = pd.read_csv(file)
            elif file.name.endswith((".xls", ".xlsx")):
                df = pd.read_excel(file)
            else:
                return Response({"error": "Unsupported file format"}, status=status.HTTP_400_BAD_REQUEST)

            # Check required columns
            required_columns = {"lat", "lon", "name", "description"}
            print(df.columns)
            if not required_columns.issubset(df.columns):
                return Response({"error": "Missing required columns"}, status=status.HTTP_400_BAD_REQUEST)

            final_dics = []
            # Print each row one by one
            for index, row in df.iterrows():
                dic = {
                    "lat": row["lat"],
                    "lon": row["lon"],
                    "name": row["name"],
                    "description": row["description"],
                }
                final_dics.append(dic)

            # Process the data
            response = add_sites_to_group_in_bulk(sites_info=final_dics, group_id=group_id, user_id=user_id)
            if response["status_code"] != 200:
                return Response(response, status=response["status_code"])
            return Response(data=UploadCSVResponseSerializer(response['data'], many=True).data, status=status.HTTP_200_OK)
        
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        

class CheckUpdatesInNotificationEnabledGroupsView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Check updates in notification enabled Groups",
        responses={
            200: OpenApiResponse(
                description="Updates checked successfully.",
            ),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def get(self, request):
        try:
            logger.info("Checking updates in notification enabled Groups")
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            response = check_updates_in_notification_enabled_groups(user_id)
            return Response(response, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error checking updates in notification enabled Groups: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class ResetSiteUpdatesCountView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Reset site updates count",
        request=ResetSiteNewUpdatesCountSerializer,
        responses={
            200: OpenApiResponse(
                description="Site updates count reset successfully.",
            ),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Group and Sites"],
    )
    def put(self, request):
        try:
            logger.info("Resetting site updates count")
            auth = get_user_id_from_token(request)
            if auth["status"] != "success":
                return Response(
                    auth, status=status.HTTP_401_UNAUTHORIZED
                )
            user_id = auth["user_id"] 

            site_id = request.data.get("site_id")
            
            if not site_id:
                return Response({"error": "Site ID is required"}, status=status.HTTP_400_BAD_REQUEST)

            response = reset_site_updates_count(user_id=user_id, site_id=site_id)
            return Response(response, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error resetting site updates count: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )