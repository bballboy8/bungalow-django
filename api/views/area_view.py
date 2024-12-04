from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from api.services.area_service import *
from api.serializers.area_serializer import *
from rest_framework.permissions import IsAuthenticated
from logging_module import logger


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
        logger.info("Inside Post method of Satellite Capture Catalog Filter View")
        try:
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
            logger.info("Satellite Capture Catalog Filter View response")
            return Response({"data": serializer.data, "status_code": 200})
        except Exception as e:
            logger.error(f"Error in Satellite Capture Catalog Filter View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


class GeoJSONToWKTView(APIView):
    @extend_schema(
        description="Convert a GeoJSON polygon to a WKT polygon.",
        request=GeoJSONToWKTSerializer,
        responses={
            200: OpenApiResponse(
                description="WKT polygon successfully converted.",
            ),
            400: OpenApiResponse(description="Bad Request - Invalid GeoJSON."),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )
    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of GeoJSON to WKT View")
        try:
            geometry = request.data.get("geometry", None)
            if not geometry:
                return Response(
                    {"data": "Geometry (GeoJSON format) is required", "status_code": 400},
                    status=400,
                )

            service_response = convert_geojson_to_wkt(geometry)

            if service_response["status_code"] != 200:
                return Response(service_response, status=service_response["status_code"])

            logger.info("GeoJSON to WKT View response")
            return Response(
                {"data": {"wkt_polygon": service_response["data"]}, "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in GeoJSON to WKT View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)
