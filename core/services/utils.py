import os
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from PIL import Image, ImageChops
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from pyproj import Geod

MAX_THREADS = 10


def check_csv_and_rename_output_dir(
    OUTPUT_DIR, START_DATE, END_DATE, output_base_dir, vendor_name
):
    # Full path of the output CSV file
    OUTPUT_CSV_FILE = os.path.join(OUTPUT_DIR, f"output_{vendor_name}.csv")
    
    if not os.path.exists(OUTPUT_CSV_FILE):
        print(f"CSV file not found: {OUTPUT_CSV_FILE}")
        return
    
    try:
        with open(OUTPUT_CSV_FILE, "r") as f:
            reader = csv.reader(f)
            row_count = sum(1 for _ in reader)

        if row_count < 2:
            print("No data found for the given parameters")
            new_output_dir = os.path.join(
                output_base_dir, f"{vendor_name}/0_{START_DATE}_{END_DATE}"
            )

            if os.path.exists(new_output_dir):
                counter = 1
                while os.path.exists(f"{new_output_dir}_{counter}"):
                    counter += 1
                new_output_dir = f"{new_output_dir}_{counter}"

            # Try renaming the directory
            try:
                os.rename(OUTPUT_DIR, new_output_dir)
                print(f"Directory renamed to: {new_output_dir}")
            except PermissionError:
                print(f"Permission denied: could not rename {OUTPUT_DIR}")
    
    except Exception as e:
        print(f"Error processing file: {e}")


def check_folder_content_and_rename_output_dir(
    OUTPUT_THUMBNAIL_FOLDER,
    OUTPUT_DIR,
    START_DATE,
    END_DATE,
    output_base_dir,
    vendor_name,
):
    if len(os.listdir(OUTPUT_THUMBNAIL_FOLDER)) == 0:
        print("No data found for the given parameters")
        new_output_dir = os.path.join(
            output_base_dir, f"{vendor_name}/0_{START_DATE}_{END_DATE}"
        )

        if os.path.exists(new_output_dir):
            counter = 1
            while os.path.exists(f"{new_output_dir}_{counter}"):
                counter += 1
            new_output_dir = f"{new_output_dir}_{counter}"

        os.rename(OUTPUT_DIR, new_output_dir)


def process_geojson(features, OUTPUT_GEOJSON_FOLDER):
    """Saves each feature as a separate GeoJSON file."""
    for feature in features:
        feature_id = feature.get("id", "unknown")
        geojson_data = {
            "type": "FeatureCollection",
            "features": [feature],  # Save each feature individually
        }

        geojson_filename = f"{feature_id}.geojson"
        geojson_path = os.path.join(OUTPUT_GEOJSON_FOLDER, geojson_filename)

        with open(geojson_path, "w") as geojson_file:
            json.dump(geojson_data, geojson_file, indent=4)

def calculate_bbox(geometry):
    """Calculate the bounding box from the GeoJSON polygon coordinates."""
    coordinates = geometry['coordinates'][0]  # Assuming the first polygon
    longitudes = [coord[0] for coord in coordinates]
    latitudes = [coord[1] for coord in coordinates]
    
    min_long = min(longitudes)
    max_long = max(longitudes)
    min_lat = min(latitudes)
    max_lat = max(latitudes)
    
    return min_long, min_lat, max_long, max_lat


def save_image(feature, OUTPUT_THUMBNAILS_FOLDER, OUTPUT_GEOTIFF_FOLDER, AUTH_TOKEN):
    """Downloads an image from the provided URL and saves it to the specified path."""
    try:
        url = feature.get("url")
        bbox = calculate_bbox(feature.get("geometry"))
        save_path = os.path.join(OUTPUT_THUMBNAILS_FOLDER, f"{feature.get('id')}.png")
        headers = {"Authorization": "Bearer " + AUTH_TOKEN}
        response = requests.get(url, headers=headers, stream=True)

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            georectify_image(save_path, bbox, OUTPUT_GEOTIFF_FOLDER, feature.get("id"))
        else:
            print(f"Error during download: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Exception occurred while downloading image: {
            feature.get('id')}: {e}"
        )
        return False


def download_thumbnails(
    features, OUTPUT_THUMBNAILS_FOLDER, OUTPUT_GEOTIFF_FOLDER, AUTH_TOKEN
):
    """Download and save thumbnail images for the given features."""

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(
                save_image,
                feature,
                OUTPUT_THUMBNAILS_FOLDER,
                OUTPUT_GEOTIFF_FOLDER,
                AUTH_TOKEN,
            ): feature
            for feature in features
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    # print(f"Successfully downloaded thumbnail for feature {feature.get('id')}")
                    pass
                else:
                    # print(f"Failed to download thumbnail for feature {feature.get('id')}")
                    pass
            except Exception as e:
                # print(f"Exception occurred while downloading thumbnail for feature {feature.get('id')}: {e}")
                pass


def remove_black_borders(img):
    """Remove black borders from the image."""
    bg = Image.new(img.mode, img.size, img.getpixel((0, 0)))
    diff = ImageChops.difference(img, bg)
    bbox = diff.getbbox()
    if bbox:
        return img.crop(bbox)
    return img


def georectify_image(
    png_path, bbox, geotiffs_folder, image_id, target_resolution=(1500, 1500)
):
    try:
        with Image.open(png_path) as img:
            img = remove_black_borders(img)
            img = img.resize(target_resolution, Image.Resampling.LANCZOS)
            img_array = np.array(img)

        width, height = target_resolution

        left, bottom, right, top = bbox

        transform = from_bounds(left, bottom, right, top, width, height)

        geotiff_name = f"{image_id}.tif"
        geotiff_path = os.path.join(geotiffs_folder, geotiff_name)

        if len(img_array.shape) == 2:
            img_array = np.expand_dims(img_array, axis=-1)
            count = 1
        else:
            count = img_array.shape[2]

        # Write the GeoTIFF file using rasterio
        with rasterio.open(
            geotiff_path,
            "w",
            driver="GTiff",
            height=img_array.shape[0],
            width=img_array.shape[1],
            count=count,
            dtype=img_array.dtype,
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            for i in range(1, count + 1):
                dst.write(img_array[:, :, i - 1], i)

    except Exception as e:
        pass


def process_geojson(features, OUTPUT_GEOJSON_FOLDER):
    """Saves each feature as a separate GeoJSON file."""
    for feature in features:
        feature_id = feature.get("id", "unknown")
        geojson_data = feature
        geojson_filename = f"{feature_id}.geojson"
        geojson_path = os.path.join(OUTPUT_GEOJSON_FOLDER, geojson_filename)

        with open(geojson_path, "w") as geojson_file:
            json.dump(geojson_data, geojson_file, indent=4)


def latlon_to_geojson(lat, lon, range_meters):
    """Generate a GeoJSON Polygon from a lat, lon, and range in meters."""
    geod = Geod(ellps="WGS84")
    
    # Get bounding box coordinates
    north_lon, north_lat, _ = geod.fwd(lon, lat, 0, range_meters)
    south_lon, south_lat, _ = geod.fwd(lon, lat, 180, range_meters)
    east_lon, east_lat, _ = geod.fwd(lon, lat, 90, range_meters) 
    west_lon, west_lat, _ = geod.fwd(lon, lat, 270, range_meters)
    
    # Format as GeoJSON Polygon
    geojson_geometry = {
        "type": "Polygon",
        "coordinates": [[
            [west_lon, south_lat],
            [east_lon, south_lat],
            [east_lon, north_lat],
            [west_lon, north_lat],
            [west_lon, south_lat] 
        ]]
    }
    
    return geojson_geometry

def latlon_to_wkt(lat, lon, range_meters):
    """Generate a WKT POLYGON from a lat, lon, and range in meters."""
    geod = Geod(ellps="WGS84")
    
    # Get bounding box coordinates
    north_lon, north_lat, _ = geod.fwd(lon, lat, 0, range_meters)
    south_lon, south_lat, _ = geod.fwd(lon, lat, 180, range_meters)
    east_lon, east_lat, _ = geod.fwd(lon, lat, 90, range_meters)
    west_lon, west_lat, _ = geod.fwd(lon, lat, 270, range_meters)
    
    # Format as WKT POLYGON
    wkt_polygon = f"POLYGON(({west_lon} {south_lat}, {east_lon} {south_lat}, {east_lon} {north_lat}, {west_lon} {north_lat}, {west_lon} {south_lat}))"
    
    return wkt_polygon

def calculate_bbox_npolygons(geometry):
    """Calculate the bounding box for GeoJSON Polygon or MultiPolygon coordinates."""
    
    def extract_coords(geometry):
        if geometry['type'] == 'Polygon':
            return geometry['coordinates']
        elif geometry['type'] == 'MultiPolygon':
            return [ring for polygon in geometry['coordinates'] for ring in polygon]
        else:
            raise ValueError("Unsupported geometry type")

    coordinates = extract_coords(geometry)
    
    longitudes = []
    latitudes = []
    
    for ring in coordinates:  # Loop through all rings
        longitudes.extend([coord[0] for coord in ring])
        latitudes.extend([coord[1] for coord in ring])
    
    min_long = min(longitudes)
    max_long = max(longitudes)
    min_lat = min(latitudes)
    max_lat = max(latitudes)
    
    return min_long, min_lat, max_long, max_lat



def generate_land_grids(shapefile_path):
    import geopandas as gpd
    from shapely.geometry import Polygon
    import os
    # Define fixed bounds
    lat_min, lat_max = -90, 90
    lon_min, lon_max = -180, 180
    
    # Check if the shapefile exists
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"Shapefile not found at {shapefile_path}")
    
    # Load the world shapefile
    world = gpd.read_file(shapefile_path)
    
    land_grids = []
    
    # Iterate over the latitudes and longitudes with a step of 6 degrees
    for lat in range(int(lat_min), int(lat_max), 6):
        for lon in range(int(lon_min), int(lon_max), 6):
            # Create the coordinates for the bounding box
            lat1 = lat
            lat2 = min(lat + 6, lat_max)
            lon1 = lon
            lon2 = min(lon + 6, lon_max)
            
            # Generate the polygon for the bounding box
            grid = Polygon([(lon1, lat1), (lon2, lat1), (lon2, lat2), (lon1, lat2), (lon1, lat1)])
            
            # Check if the grid intersects with any land (world polygons)
            if world.intersects(grid).any():
                land_grids.append(grid)
    
    return land_grids

from shapely.geometry import Polygon, shape
from pyproj import Transformer, CRS
def calculate_area_from_geojson(geojson, id):
    """
    Calculates the area of a polygon given in GeoJSON format.

    Parameters:
        geojson (dict): GeoJSON dictionary with a Polygon type geometry.
        id (str or int): Identifier for logging/debugging.

    Returns:
        float: Area in square meters.
    """
    try:
        polygon = shape(geojson)  

        # Determine UTM zone dynamically based on the polygon centroid
        lon, lat = polygon.centroid.x, polygon.centroid.y
        utm_zone = int((lon + 180) / 6) + 1
        is_southern = lat < 0
        utm_crs = CRS.from_proj4(f"+proj=utm +zone={utm_zone} +{'south' if is_southern else 'north'} +datum=WGS84 +units=m +no_defs")

        # Transformer to reproject to the appropriate UTM zone
        transformer = Transformer.from_crs("epsg:4326", utm_crs, always_xy=True)

        # Transform coordinates to UTM
        utm_coords = [transformer.transform(x, y) for x, y in polygon.exterior.coords]
        utm_polygon = Polygon(utm_coords)

        # Calculate and return the area in square meters
        area_sq_m = utm_polygon.area
        return area_sq_m

    except Exception as e:
        print(f"Error calculating area for id {id}: {e}")
        return 0