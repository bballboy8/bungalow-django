from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from api.services.area_service import *
from api.serializers.area_serializer import *
from rest_framework.permissions import IsAuthenticated
from rest_framework import status


@extend_schema(
    description="Filter satellite captures by a location polygon in WKT format. Returns records whose polygons intersect with the input polygon.",
    request=SatelliteCatalogFilterSerializer,
    responses={
        200: OpenApiResponse(
            description="A list of filtered satellite captures.",
            response=SatelliteCatalogFilterSerializer(many=True),
        ),
        400: OpenApiResponse(description="Bad Request - Invalid WKT Polygon."),
        500: OpenApiResponse(description="Internal server error"),
    },
    tags=["Satellite Capture"],
)
class SatelliteCaptureCatalogFilterView(APIView):

    def post(self, request, *args, **kwargs):
        wkt_polygon = request.data.get("wkt_polygon", None)

        if not wkt_polygon:
            return Response(
                {"data": "Polygon (WKT format) is required", "status_code": 400},
                status=400,
            )

        service_response = filter_by_polygon(wkt_polygon)

        if service_response["status_code"] != 200:
            return Response(service_response, status=service_response["status_code"])

        serializer = SatelliteCatalogFilterSerializer(
            service_response["data"], many=True
        )
        return Response({"data": serializer.data, "status_code": 200})
