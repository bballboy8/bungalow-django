from rest_framework.views import APIView
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiResponse
from api.services.area_service import *
from api.serializers.area_serializer import *
from api.parameters.area_parameters import *
from rest_framework.permissions import IsAuthenticated
from logging_module import logger
from api.tasks import run_image_seeder
from core.models import time_ranges

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
                        "data": [],
                        "status_code": 400,
                        "error": "Geometry (GeoJSON format) is required",
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
            return Response({"data": [], "status_code": 500, "error": f"{str(e)}"}, status=500)


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
            page_number = (request.query_params.get("page_number", 1))
            page_size = (request.query_params.get("page_size", 10))
            start_date = request.query_params.get("start_date")
            end_date = request.query_params.get("end_date")
            latitude = (request.query_params.get("latitude"))
            longitude = (request.query_params.get("longitude"))
            distance = (request.query_params.get("distance"))
            source = request.query_params.get("source", "home")
            vendor_id = request.query_params.get("vendor_id")
            sort_by = request.query_params.get("sort_by", "acquisition_datetime")
            sort_order = request.query_params.get("sort_order", "desc")
            zoomed_wkt = request.query_params.get("zoomed_wkt")
            vendor_name = request.query_params.get("vendor_name")
            min_cloud_cover = (request.query_params.get("min_cloud_cover"))
            max_cloud_cover = (request.query_params.get("max_cloud_cover"))
            min_off_nadir_angle = (request.query_params.get("min_off_nadir_angle"))
            max_off_nadir_angle = (request.query_params.get("max_off_nadir_angle"))
            min_gsd = (request.query_params.get("min_gsd"))
            max_gsd = (request.query_params.get("max_gsd"))
            focused_records_ids = request.query_params.get("focused_records_ids", "")
            user_timezone = request.query_params.get("user_timezone")
            user_duration_type = request.query_params.get("user_duration_type")

            # New Filters, azimuth_angle, illumination_azimuth_angle, illumination_elevation_angle, publication_datetime, holdback_seconds

            min_azimuth_angle = (request.query_params.get("min_azimuth_angle"))
            max_azimuth_angle = (request.query_params.get("max_azimuth_angle"))
            min_illumination_azimuth_angle = (request.query_params.get("min_illumination_azimuth_angle"))
            max_illumination_azimuth_angle = (request.query_params.get("max_illumination_azimuth_angle"))
            min_illumination_elevation_angle = (request.query_params.get("min_illumination_elevation_angle"))
            max_illumination_elevation_angle = (request.query_params.get("max_illumination_elevation_angle"))
            min_holdback_seconds = (request.query_params.get("min_holdback_seconds"))
            max_holdback_seconds = (request.query_params.get("max_holdback_seconds"))


            if user_duration_type:
                for duration in str(user_duration_type).split(","):
                    if duration not in time_ranges:
                        return Response({"data": f"Duration not valid", "status_code": 400, "error": f"Duration not valid"}, status=400)


            # Request body
            wkt_polygon = request.data.get("wkt_polygon", None)

            logger.info(
                f"Page Number: {page_number}, Page Size: {page_size}, Start Date: {start_date}, End Date: {end_date} Latitude: {latitude}, Longitude: {longitude}, Distance: {distance} Source: {source}, Vendor ID: {vendor_id} "
            )
            logger.info(f"Sort By: {sort_by}, Sort Order: {sort_order}, Zoomed WKT: {zoomed_wkt} WKT Polygon: {wkt_polygon} Vendor Name: {vendor_name}")

            logger.info(f"Min Cloud Cover: {min_cloud_cover}, Max Cloud Cover: {max_cloud_cover}")
            logger.info(f"Min Off Nadir Angle: {min_off_nadir_angle}, Max Off Nadir Angle: {max_off_nadir_angle}")
            logger.info(f"Min GSD: {min_gsd}, Max GSD: {max_gsd}")

            logger.info(f"Focused Records IDs: {focused_records_ids}")

            logger.info(f"User Timezone: {user_timezone}, User Duration Type: {user_duration_type}")

            logger.info(f"Min Azimuth Angle: {min_azimuth_angle}, Max Azimuth Angle: {max_azimuth_angle}")

            logger.info(f"Min Illumination Azimuth Angle: {min_illumination_azimuth_angle}, Max Illumination Azimuth Angle: {max_illumination_azimuth_angle}")

            logger.info(f"Min Illumination Elevation Angle: {min_illumination_elevation_angle}, Max Illumination Elevation Angle: {max_illumination_elevation_angle}")

            logger.info(f"Min Holdback Seconds: {min_holdback_seconds}, Max Holdback Seconds: {max_holdback_seconds}")

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
                vendor_id=vendor_id,
                request=request,
                sort_by=sort_by,
                sort_order=sort_order,
                zoomed_wkt=zoomed_wkt,
                vendor_name=vendor_name,
                min_cloud_cover=min_cloud_cover,
                max_cloud_cover=max_cloud_cover,
                min_off_nadir_angle=min_off_nadir_angle,
                max_off_nadir_angle=max_off_nadir_angle,
                min_gsd=min_gsd,
                max_gsd=max_gsd,
                focused_records_ids=focused_records_ids,
                user_timezone=user_timezone,
                user_duration_type=user_duration_type,
                min_azimuth_angle=min_azimuth_angle,
                max_azimuth_angle=max_azimuth_angle,
                min_illumination_azimuth_angle=min_illumination_azimuth_angle,
                max_illumination_azimuth_angle=max_illumination_azimuth_angle,
                min_illumination_elevation_angle=min_illumination_elevation_angle,
                max_illumination_elevation_angle=max_illumination_elevation_angle,
                min_holdback_seconds=min_holdback_seconds,
                max_holdback_seconds=max_holdback_seconds
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            serializer = SatelliteCaptureCatalogListSerializer(
                service_response["data"], many=True,  context={'timezone': user_timezone}
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
                    "zoomed_captures_count": service_response["zoomed_captures_count"],
                    "polygon_area_km2": service_response["polygon_area_km2"],
                    "page_number": service_response["page_number"],
                    "page_size": service_response["page_size"],
                    "total_records": service_response["total_records"],
                    "regular_captures_count": service_response["regular_captures_count"],
                    "focused_captures_count": service_response["focused_captures_count"],
                    "time_taken": service_response["time_taken"],
                    "status_code": 200,
                }
            )
        except Exception as e:
            logger.error(f"Error in Satellite Capture Catalog View")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)


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
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)



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
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)
        

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
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)
        
class GetPolygonSelectionAcquisitionCalenderDaysFrequencyView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get polygon selection calender days frequency.",
        parameters= calendar_params,
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
            start_date = request.data.get("start_date", None)
            end_date = request.data.get("end_date", None)
            vendor_id = request.query_params.get("vendor_id", None)
            vendor_name = request.query_params.get("vendor_name", None)
            min_cloud_cover = float(request.query_params.get("min_cloud_cover", -1))
            max_cloud_cover = float(request.query_params.get("max_cloud_cover", 100))
            min_off_nadir_angle = float(request.query_params.get("min_off_nadir_angle", 0))
            max_off_nadir_angle = float(request.query_params.get("max_off_nadir_angle", 360))
            min_gsd = float(request.query_params.get("min_gsd", 0))
            max_gsd = float(request.query_params.get("max_gsd", 100))
            user_timezone = request.query_params.get("user_timezone")
            user_duration_type = request.query_params.get("user_duration_type")

            if user_duration_type:
                for duration in str(user_duration_type).split(","):
                    if duration not in time_ranges:
                        return Response({"data": f"Duration not valid", "status_code": 400, "error": f"Duration not valid"}, status=400)

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
                polygon_wkt=polygon_wkt,
                start_date=start_date,
                end_date=end_date,
                vendor_id=vendor_id,
                vendor_name=vendor_name,
                min_cloud_cover=min_cloud_cover,
                max_cloud_cover=max_cloud_cover,
                min_off_nadir_angle=min_off_nadir_angle,
                max_off_nadir_angle=max_off_nadir_angle,
                min_gsd=min_gsd,
                max_gsd=max_gsd,
                user_timezone=user_timezone,
                user_duration_type=user_duration_type
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Polygon Selection Calender Days Frequency View response")
            return Response(
                {"data": service_response["data"], "status_code": 200, "time_taken": service_response["time_taken"]}
            )
        except Exception as e:
            logger.error(f"Error in Polygon Selection Calender Days Frequency View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)
        
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
            start_date = request.data.get("start_date", None)
            end_date = request.data.get("end_date", None)

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
                latitude=latitude, longitude=longitude, distance=distance, start_date=start_date, end_date=end_date
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
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)
        

class GetAreaFromPolygonWkt(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get area from WKT polygon.",
        request=AreaFromPolygonWktSerializer,
        responses={
            200: OpenApiResponse(
                description="Area successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )

    def post(self, request, *args, **kwargs):
        logger.info("Inside Post method of Area from Polygon WKT View")
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
            service_response = get_area_from_polygon_wkt(
                polygon_wkt=polygon_wkt
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Area from Polygon WKT View response")
            return Response(
                {"data": service_response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Area from Polygon WKT View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)
        

class GenerateCirclePolygonAPIView(APIView):
    """
    API to generate a GeoJSON Polygon for a circle based on center latitude, longitude, and radius.
    """
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Generate GeoJSON Polygon for a circle.",
        request=GenerateCirclePolygonSerializer,
        responses={
            200: OpenApiResponse(
                description="GeoJSON Polygon successfully generated.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Circle Operations"],
    )
    def post(self, request, *args, **kwargs):
        try:
            latitude = request.data.get("latitude")
            longitude = request.data.get("longitude")
            distance_km = request.data.get("distance_km")

            if latitude is None or longitude is None or distance_km is None:
                return Response(
                    {"data": "latitude, longitude, and distance_km are required", "status_code": 400},
                    status=400,
                )

            geojson_polygon = generate_circle_polygon_geojson(latitude, longitude, distance_km)
            return Response({"data": geojson_polygon, "status_code": 200}, status=200)

        except Exception as e:
            logger.error(f"Error generating GeoJSON Polygon: {str(e)}")
            return Response({"data": str(e), "status_code": 500, "error": f"{str(e)}"}, status=500)


class ExtractCircleParametersAPIView(APIView):
    """
    API to extract center latitude, longitude, and radius from a GeoJSON Polygon.
    """
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Extract original parameters from a GeoJSON Polygon.",
        request=ExtractCircleParametersSerializer,
        responses={
            200: OpenApiResponse(
                description="Parameters successfully extracted.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Circle Operations"],
    )
    def post(self, request, *args, **kwargs):
        try:
            geojson_polygon = request.data.get("geojson_polygon")

            if not geojson_polygon:
                return Response(
                    {"data": "geojson_polygon is required", "status_code": 400},
                    status=400,
                )

            latitude, longitude, distance_km = get_circle_parameters_from_geojson(geojson_polygon)
            return Response(
                {
                    "data": {
                        "latitude": latitude,
                        "longitude": longitude,
                        "distance_km": distance_km
                    },
                    "status_code": 200,
                },
                status=200
            )

        except Exception as e:
            logger.error(f"Error extracting parameters from GeoJSON: {str(e)}")
            return Response({"data": str(e), "status_code": 500, "error": f"{str(e)}"}, status=500)
        

class GetCollectionHistoryView(APIView):
    """
    API to get the daily collection status of satellite images.
    """
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get daily collection status of satellite images.",
        parameters=collection_history_params,
        responses={
            200: OpenApiResponse(
                description="Daily collection status successfully retrieved.",
            ),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Satellite Capture"],
    )
    def get(self, request, *args, **kwargs):
        try:
            logger.info("Inside Get method of Daily Collection Status View")

            start_date = request.query_params.get("start_date", None)
            end_date = request.query_params.get("end_date", None)
            vendor_name = request.query_params.get("vendor_name", None)
            page_number = int(request.query_params.get("page_number", 1))
            page_size = int(request.query_params.get("page_size", 10))

            logger.info(f"Start Date: {start_date}, End Date: {end_date}, Vendors Names: {vendor_name}")

            service_response = get_collection_history(
                start_date=start_date,
                end_date=end_date,
                vendor_name=vendor_name,
                page_number=page_number,
                page_size=page_size
            )
            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )
            data = service_response["data"]
            return Response({"data": data, "status_code": 200}, status=200)

        except Exception as e:
            logger.error(f"Error getting daily collection status: {str(e)}")
            return Response({"data": str(e), "status_code": 500, "error": f"{str(e)}"}, status=500)
        

class GetWeatherDetailsFromTommorrowThirdParty(APIView):
    """
    API to get weather details from Tomorrow.io third party service.
    """
    permission_classes = [IsAuthenticated]

    @extend_schema(
        description="Get weather details from Tomorrow.io third party service.",
        request=WeatherDetailsFromTommorrowThirdPartySerializer,
        responses={
            200: OpenApiResponse(
                description="Weather details successfully retrieved.",
            ),
            400: OpenApiResponse(description="Bad Request"),
            500: OpenApiResponse(description="Internal server error"),
        },
        tags=["Weather Capture"],
    )


    def post(self, request, *args, **kwargs):
        try:
            logger.info("Inside Post method of Get Weather Details From Tommorrow Third Party View")
            latitude = request.data.get("latitude", None)
            longitude = request.data.get("longitude", None)
            if not latitude or not longitude:
                return Response(
                    {
                        "data": "Latitude and Longitude are required",
                        "status_code": 400,
                    },
                    status=400,
                )

            logger.info(f"Latitude: {latitude}, Longitude: {longitude}")
            service_response = get_weather_details_from_tommorrow_third_party(
            )

            if service_response["status_code"] != 200:
                return Response(
                    service_response, status=service_response["status_code"]
                )

            logger.info("Get Weather Details From Tommorrow Third Party View response")
            return Response(
                {"data": service_response["data"], "status_code": 200}
            )
        except Exception as e:
            logger.error(f"Error in Get Weather Details From Tommorrow Third Party View: {str(e)}")
            return Response({"data": f"{str(e)}", "status_code": 500, "error": f"{str(e)}"}, status=500)