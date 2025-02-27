import psycopg2
from django.db import connections
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json
from shapely.geometry import shape

BATCH_SIZE = 100  # Number of records per batch
THREAD_COUNT = os.cpu_count()  # Number of threads to use (adjust based on your system's resources)

def calculate_centroid(geojson):
    """Calculate the centroid of a GeoJSON polygon."""
    try:
        geom = shape(geojson)
        centroid = geom.centroid
        return centroid.y, centroid.x  # Latitude, Longitude
    except Exception as e:
        print(f"Error calculating centroid: {e}")
        return None, None

def process_batch(offset, batch_number, total_batches):
    """Process a batch of records to calculate and update centroids."""
    try:
        print(f"Processing batch {batch_number}/{total_batches} with offset {offset}...")

        db_conn = connections["default"]

        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, coordinates_record
                FROM core_collectioncatalog 
                WHERE coordinates_record IS NOT NULL 
                AND (
                    (POSITION('.' IN CAST("geometryCentroid_lat" AS TEXT)) > 0 
                        AND CHAR_LENGTH(CAST("geometryCentroid_lat" AS TEXT)) - POSITION('.' IN CAST("geometryCentroid_lat" AS TEXT)) < 3)
                    OR 
                    (POSITION('.' IN CAST("geometryCentroid_lon" AS TEXT)) > 0 
                        AND CHAR_LENGTH(CAST("geometryCentroid_lon" AS TEXT)) - POSITION('.' IN CAST("geometryCentroid_lon" AS TEXT)) < 3)
                )
                LIMIT %s OFFSET %s;
                """,
                [BATCH_SIZE, offset]
            )
            records = cursor.fetchall()

            if not records:
                return 0  # No records left to process

            update_data = []
            for record in records:
                record_id, geojson_data = record
                geojson = json.loads(geojson_data) if isinstance(geojson_data, str) else geojson_data
                lat, lon = calculate_centroid(geojson)
                if lat is not None and lon is not None:
                    update_data.append((round(lat,5), round(lon,5), record_id))

            if update_data:
                query = """
                    UPDATE core_collectioncatalog 
                    SET "geometryCentroid_lat" = %s, "geometryCentroid_lon" = %s 
                    WHERE id = %s;
                """
                cursor.executemany(query, update_data)
                db_conn.commit()

            print(f"Batch {batch_number} completed. Updated {len(update_data)} records.")
            return len(update_data)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error while processing batch {batch_number} with offset {offset}: {e}")
        return 0

def update_centroids_parallel():
    """Calculate and update centroids in parallel."""
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM core_collectioncatalog 
                WHERE coordinates_record IS NOT NULL 
                AND (
                    (POSITION('.' IN CAST("geometryCentroid_lat" AS TEXT)) > 0 
                        AND CHAR_LENGTH(CAST("geometryCentroid_lat" AS TEXT)) - POSITION('.' IN CAST("geometryCentroid_lat" AS TEXT)) < 3)
                    OR 
                    (POSITION('.' IN CAST("geometryCentroid_lon" AS TEXT)) > 0 
                        AND CHAR_LENGTH(CAST("geometryCentroid_lon" AS TEXT)) - POSITION('.' IN CAST("geometryCentroid_lon" AS TEXT)) < 3)
                )
                """
            )
            total_records = cursor.fetchone()[0]

        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"Total records: {total_records}, Total batches: {total_batches}")

        processed_records = 0

        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            futures = {
                executor.submit(process_batch, offset, batch_number + 1, total_batches): batch_number + 1
                for batch_number, offset in enumerate(range(0, total_records, BATCH_SIZE))
            }

            for future in as_completed(futures):
                batch_number = futures[future]
                try:
                    updated_count = future.result()
                    processed_records += updated_count
                    print(f"Progress: {processed_records}/{total_records} records processed.")
                except Exception as e:
                    print(f"Error in batch {batch_number}: {e}")

        print(f"All batches processed successfully. Total records updated: {processed_records}/{total_records}")

    except Exception as e:
        print(f"Error while updating centroids in parallel: {e}")

if __name__ == "__main__":
    update_centroids_parallel()

# from core.services.database_bulk_lat_lon_decimal_update import update_centroids_parallel
# update_centroids_parallel()
