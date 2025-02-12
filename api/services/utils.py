from rest_framework_simplejwt.tokens import UntypedToken
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError
from logging_module import logger
from pyproj import Geod

def get_user_id_from_token(request):
    try:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                decoded_token = UntypedToken(token)  # Automatically validates the token
            except (InvalidToken, TokenError) as e:
                return {"error": "Invalid token", "status": "error_401"}
            return {"user_id": decoded_token.payload["user_id"], "status": "success"}
        logger.error("No token provided")
        return {"error": "No token provided", "status": "error_401"}
    except Exception as e:
        logger.error(f"Error in get_user_id_from_token: {str(e)}")
        return {"error": str(e), "status": "error_500"}
    
def generate_hexagon_geojson(lat, lon, radius_km=1):
    """
    Generate a hexagonal polygon (6 points) around a latitude and longitude with a given radius.
    
    Parameters:
    - lat (float): Latitude of the center.
    - lon (float): Longitude of the center.
    - radius_km (float): Radius in kilometers.
    
    Returns:
    - dict: A GeoJSON-like dictionary representing the hexagonal polygon.
    """
    try:
        geod = Geod(ellps="WGS84")
        points = []

        for angle in range(0, 360, 60):  # 6 sides of the hexagon
            lon_new, lat_new, _ = geod.fwd(lon, lat, angle, radius_km * 1000)
            points.append([lon_new, lat_new])

        points.append(points[0])

        geojson = {
            "type": "Polygon",
            "coordinates": [points]
        }

        return {"polygon": geojson, "status_code": 200}
    
    except Exception as e:
        return {"error": str(e), "status_code": 500}