from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiResponse
from api.services.area_service import *
from api.serializers.area_serializer import *
from api.parameters.area_parameters import *
from rest_framework.permissions import IsAuthenticated
from logging_module import logger
import time


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
                    {
                        "data": "Geometry (GeoJSON format) is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            service_response = convert_geojson_to_wkt(geometry)

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("GeoJSON to WKT View response")
            return Response(
                {"data": {"wkt_polygon": service_response["data"]}, "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in GeoJSON to WKT View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


class SatelliteCaptureCatalogView(APIView):
    # permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get satellite captures with optional filters for page number, page size, start date, and end date.",
        request=SatelliteCatalogFilterSerializer,
        parameters= satellite_capture_catalog_params,
        responses={
            200: OpenApiResponse(
                description="A list of satellite captures.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )
    def post(self, request, *args, **kwargs):
        logger.info("Inside Get method of Satellite Capture Catalog View")
        try:
            # Query parameters
            page_number = int(request.query_params.get("page_number", 1))
            page_size = int(request.query_params.get("page_size", 10))
            start_date = request.query_params.get("start_date", None)
            end_date = request.query_params.get("end_date", None)
            latitude = int(request.query_params.get("latitude", 0))
            longitude = int(request.query_params.get("longitude", 0))
            distance = int(request.query_params.get("distance", 0))
            
            # Request body
            wkt_polygon = request.data.get("wkt_polygon", None)

            logger.info(
                f"Page Number: {page_number}, Page Size: {page_size}, Start Date: {start_date}, End Date: {end_date} Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}"
            )

            service_response = get_satellite_records(
                page_number=page_number,
                page_size=page_size,
                start_date=start_date,
                end_date=end_date,
                latitude=latitude,
                longitude=longitude,
                distance=distance,
                wkt_polygon=wkt_polygon,
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            serializer = SatelliteCaptureCatalogSerializer(
                service_response["data"], many=True
            )
            logger.info("Satellite Capture Catalog View response")
            return Response(
                {
                    "data": serializer.data,
                    "page_number": service_response["page_number"],
                    "page_size": service_response["page_size"],
                    "total_records": service_response["total_records"],
                    "status_code": 200,
                }
            )
        except Exception as e:
            logger.error(f"Error in Satellite Capture Catalog View")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)
