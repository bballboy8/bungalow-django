from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from api.models import Group
from api.serializers.group_and_sites_serializer import *
from api.services.group_and_sites_service import *


class GroupView(APIView):
    def get(self, request, group_id=None):
        """
        Retrieve all groups or the full hierarchy of a specific group.
        """
        try:
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


class SiteView(APIView):
    def get(self, request):
        try:
            logger.info("Fetching all sites")
            sites = get_all_sites()
            if sites["status_code"] != 200:
                return Response(sites, status=sites["status_code"])
            serializer = SiteSerializer(sites, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error fetching sites: {str(e)}")
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def post(self, request):
        serializer = SiteSerializer(data=request.data)
        if serializer.is_valid():
            site = serializer.save()
            return Response(SiteSerializer(site).data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class GroupSiteView(APIView):
    def post(self, request):
        group_id = request.data.get("group_id")
        site_id = request.data.get("site_id")
        group = Group.objects.filter(id=group_id).first()
        site = Site.objects.filter(id=site_id).first()

        if not group or not site:
            return Response(
                {"error": "Invalid group or site ID"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        assignment = assign_site_to_group(group, site)
        if assignment["status_code"] != 200:
            return Response(assignment, status=assignment["status_code"])
        serializer = GroupSiteSerializer(assignment)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def get(self, request, group_id):
        try:
            sites = get_sites_in_group(group_id)
            if sites["status_code"] != 200:
                return Response(sites, status=sites["status_code"])
            serializer = SiteSerializer(sites, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
