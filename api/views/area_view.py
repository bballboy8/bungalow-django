from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiResponse
from api.services.area_service import *
from api.serializers.area_serializer import *
from api.parameters.area_parameters import *
from rest_framework.permissions import IsAuthenticated
from logging_module import logger
from api.tasks import run_image_seeder
import time


class GeoJSONToWKTView(APIView):
    permission_classes = [IsAuthenticated]

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
                {"data": {"wkt_polygon": service_response["data"], "area": service_response["area"]}, "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in GeoJSON to WKT View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


class SatelliteCaptureCatalogView(APIView):
    permission_classes = [IsAuthenticated]

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
            source = request.query_params.get("source", "home")

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
                source=source,
                request=request
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            serializer = SatelliteCaptureCatalogSerializer(
                service_response["data"], many=True
            )
            data = serializer.data
            # Non-blocking function call using Celery
            if source != "home":
                grouped_data = group_by_vendor(data)
                run_image_seeder.delay(grouped_data)
            
            logger.info("Satellite Capture Catalog View response")
            return Response(
                {
                    "data": data,
                    "page_number": service_response["page_number"],
                    "page_size": service_response["page_size"],
                    "total_records": service_response["total_records"],
                    "time_taken": service_response["time_taken"],
                    "status_code": 200,
                }
            )
        except Exception as e:
            logger.error(f"Error in Satellite Capture Catalog View")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)


class GetSatelliteCapturedImageByIdAndVendorView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="""Get satellite capture images by their IDs and vendor Example.{
                "record": [
                    {
                    "id": "c45c156d-a24a-4c45-90f0-99bf79baa745",
                    "vendor": "airbus"
                    }
                ]
                } """,
        request=SatelliteCaptureImageByIdAndVendorSerializer,
        responses={
            200: OpenApiResponse(
                description="Satellite capture images successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )
    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of Satellite Capture Image By ID and Vendor View")
        try:
            record = request.data.get("record", [])

            if not record:
                return Response(
                    {
                        "data": "Record is required",
                        "status_code": 400,
                    },
                    status=400,
                )
            response = get_presigned_url_by_vendor_name_and_id(record)
            if response["status_code"] != 200:
                return Response(
                    response, status=response["status_code"]
                )

            logger.info("Satellite Capture Image By ID and Vendor View response")
            return Response(
                {"data": response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Satellite Capture Image By ID and Vendor View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)



class GetPinSelectionAnalyticsAndLocation(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get pin selection analytics and location.",
        request=PinSelectionAnalyticsAndLocationSerializer,
        responses={
            200: OpenApiResponse(
                description="Pin selection analytics and location successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )

    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of Pin Selection Analytics and Location View")
        try:
            latitude = request.data.get("latitude", None)
            longitude = request.data.get("longitude", None)
            distance = request.data.get("distance", None)

            if not latitude or not longitude or not distance:
                return Response(
                    {
                        "data": "Latitude, Longitude, and Distance are required",
                        "status_code": 400,
                    },
                    status=400,
                )

            logger.info(
                f"Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}"
            )
            service_response = get_pin_selection_analytics_and_location(
                latitude=latitude, longitude=longitude, distance=distance
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Pin Selection Analytics and Location View response")
            return Response(
                {"data": service_response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Pin Selection Analytics and Location View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)
        

class GetPolygonSelectionAnalyticsAndLocation(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get polygon selection analytics and location.",
        request=PolygonSelectionAnalyticsAndLocationSerializer,
        responses={
            200: OpenApiResponse(
                description="Polygon selection analytics and location successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )

    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of Polygon Selection Analytics and Location View")
        try:
            polygon_wkt = request.data.get("polygon_wkt", None)
            if not polygon_wkt:
                return Response(
                    {
                        "data": "WKT Polygon is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            logger.info(f"WKT Polygon: {polygon_wkt}")
            service_response = get_polygon_selection_analytics_and_location_wkt(
                polygon_wkt=polygon_wkt
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Polygon Selection Analytics and Location View response")
            return Response(
                {"data": service_response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Polygon Selection Analytics and Location View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)
        
class GetPolygonSelectionAcquisitionCalenderDaysFrequencyView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get polygon selection calender days frequency.",
        request=PolygonSelectionAnalyticsAndLocationSerializer,
        responses={
            200: OpenApiResponse(
                description="Polygon selection calender days frequency successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )

    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of Polygon Selection Calender Days Frequency View")
        try:
            polygon_wkt = request.data.get("polygon_wkt", None)
            if not polygon_wkt:
                return Response(
                    {
                        "data": "WKT Polygon is required",
                        "status_code": 400,
                    },
                    status=400,
                )

            logger.info(f"WKT Polygon: {polygon_wkt}")
            service_response = get_polygon_selection_acquisition_calender_days_frequency(
                polygon_wkt=polygon_wkt
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Polygon Selection Calender Days Frequency View response")
            return Response(
                {"data": service_response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Polygon Selection Calender Days Frequency View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)
        
class GetPinSelectionAcquisitionCalenderDaysFrequencyView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get pin selection calender days frequency.",
        request=PinSelectionAnalyticsAndLocationSerializer,
        responses={
            200: OpenApiResponse(
                description="Pin selection calender days frequency successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )

    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of Pin Selection Calender Days Frequency View")
        try:
            latitude = request.data.get("latitude", None)
            longitude = request.data.get("longitude", None)
            distance = request.data.get("distance", None)

            if not latitude or not longitude or not distance:
                return Response(
                    {
                        "data": "Latitude, Longitude, and Distance are required",
                        "status_code": 400,
                    },
                    status=400,
                )

            logger.info(
                f"Latitude: {latitude}, Longitude: {longitude}, Distance: {distance}"
            )
            service_response = get_pin_selection_acquisition_calender_days_frequency(
                latitude=latitude, longitude=longitude, distance=distance
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Pin Selection Calender Days Frequency View response")
            return Response(
                {"data": service_response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Pin Selection Calender Days Frequency View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500}, status=500)