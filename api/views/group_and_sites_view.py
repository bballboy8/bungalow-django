from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from api.models import Group
from api.serializers.group_and_sites_serializer import *
from api.services.group_and_sites_service import *
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiParameter


class GetGroupsView(APIView):

    @extend_schema(
        description="Retrieve all groups or the full hierarchy of a specific group",
        parameters=[
            OpenApiParameter(
                name="group_id",
                type=int,
                default=None,
                location=OpenApiParameter.QUERY,
                description="ID of the group to retrieve the full hierarchy or empty to retrieve all groups.",
            )
        ],
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
            logger.info(f"Fetching group hierarchy for group ID: {group_id}")
            if group_id:
                group = Group.objects.filter(id=group_id).first()
                if not group:
                    return Response(
                        {"error": "Group not found"}, status=status.HTTP_404_NOT_FOUND
                    )

                serializer = GroupSerializer(group)
                return Response(serializer.data, status=status.HTTP_200_OK)

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

            serializer = GroupSiteSerializer(assignment["data"])
            return Response(serializer.data, status=status.HTTP_201_CREATED)
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
