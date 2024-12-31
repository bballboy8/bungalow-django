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
                return Response(response, status=status.HTTP_200_OK)
    
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

            sites = get_all_sites(
                user_id=user_id, name=name, page_number=page_number, per_page=per_page
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

            area = get_area_from_geojson(coordinates_record)
            if area > 100000:
                return Response(
                    {"error": "Area of the site exceeds 100000 sq. km"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
                

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

            parent_groups = get_parent_groups_with_details(user_id=user_id)
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
