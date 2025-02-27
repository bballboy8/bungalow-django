import psycopg2
from django.db import connections
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from shapely.geometry import Polygon
import json
import geopandas as gpd
from shapely.geometry import Point

BATCH_SIZE = 100  # Number of records per batch
THREAD_COUNT = os.cpu_count()  # Number of threads to use (adjust based on your system's resources)


def process_batch(offset, batch_number, total_batches):
    """Process a batch of records to update the centroid fields."""
    try:
        print(f"Processing batch {batch_number}/{total_batches} with offset {offset}...")

        # Get database connection for the thread
        db_conn = connections["default"]

        with db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, coordinates_record FROM core_collectioncatalog "
                "WHERE centroid_local IS NULL AND centroid_region IS NULL "
                "LIMIT %s OFFSET %s;",
                [BATCH_SIZE, offset]
            )
            records = cursor.fetchall()

            if not records:
                return 0  # No records left to process
            
            base_dir = os.getcwd() # Construct absolute paths dynamically
            states_shapefile = os.path.join(base_dir, "static", "shapesFiles", "state_provinces", "ne_10m_admin_1_states_provinces.shp")
            marine_shapefile = os.path.join(base_dir, "static", "shapesFiles", "marine_polys", "ne_10m_geography_marine_polys.shp")

            # check if the shapefiles exist
            if not os.path.exists(states_shapefile) or not os.path.exists(marine_shapefile):
                raise FileNotFoundError("Shapefiles not found.")

            states = gpd.read_file(states_shapefile)
            marine = gpd.read_file(marine_shapefile)
            

            update_data = []

            for record in records:
                record_id = record[0]
                coordinate_record = json.loads(record[1])

                try:
                    polygon = Polygon(coordinate_record["coordinates"][0])  # Ensure GeoJSON format
                    lat, lon = polygon.centroid.y, polygon.centroid.x
                    point = Point(lon, lat)

                    match = states[states.geometry.intersects(point)]
                    if not match.empty:
                        region = match.iloc[0]["admin"]
                        local = match.iloc[0]["gn_name"]
                        update_data.append((region, local, record_id))
                        continue 

                    match = marine[marine.geometry.intersects(point)]
                    if not match.empty:
                        region = match.iloc[0]["name_en"]
                        local = f"{lat}, {lon}"
                        update_data.append((region, local, record_id))
                        continue
                    update_data.append(("International Waters", f"{lat}, {lon}", record_id))
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    print(f"Error processing record ID {record_id}: {e}")

            if update_data:
                query = "UPDATE core_collectioncatalog SET centroid_region=%s, centroid_local=%s WHERE id=%s"
                cursor.executemany(query, update_data)
                db_conn.commit()  # Commit after batch update

            print(f"Batch {batch_number} completed. Updated {len(update_data)} records.")
            return len(update_data)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error while processing batch {batch_number} with offset {offset}: {e}")
        return 0


def update_gsd_column_parallel():
    """Update the centroid fields in the database using multi-threading."""
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM core_collectioncatalog WHERE centroid_local IS NULL AND centroid_region IS NULL;"
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
        print(f"Error while updating local and region in parallel: {e}")


if __name__ == "__main__":
    update_gsd_column_parallel()


# from core.services.database_bulk_update import update_gsd_column_parallel