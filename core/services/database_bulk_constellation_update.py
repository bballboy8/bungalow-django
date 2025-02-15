import psycopg2
from django.db import connections
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

BATCH_SIZE = 100  # Number of records per batch
THREAD_COUNT = os.cpu_count()  # Number of threads to use (adjust based on your system's resources)


def process_batch(offset, batch_number, total_batches):
    """Process a batch of records to copy platform column value to constellation."""
    try:
        print(f"Processing batch {batch_number}/{total_batches} with offset {offset}...")

        # Get database connection for the thread
        db_conn = connections["default"]

        with db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, platform FROM core_collectioncatalog "
                "WHERE vendor_name IN ('maxar', 'planet', 'airbus', 'capella') "
                "AND platform IS NOT NULL AND platform <> constellation"
                "LIMIT %s OFFSET %s;",
                [BATCH_SIZE, offset]
            )
            records = cursor.fetchall()

            if not records:
                return 0  # No records left to process

            update_data = [(record[1], record[0]) for record in records]  # (platform, id)

            if update_data:
                query = "UPDATE core_collectioncatalog SET constellation=%s WHERE id=%s"
                cursor.executemany(query, update_data)
                db_conn.commit()  # Commit after batch update

            print(f"Batch {batch_number} completed. Updated {len(update_data)} records.")
            return len(update_data)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error while processing batch {batch_number} with offset {offset}: {e}")
        return 0


def update_constellation_parallel():
    """Copy platform column value to constellation in the database using multi-threading."""
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM core_collectioncatalog "
                "WHERE vendor_name IN ('maxar', 'planet', 'airbus', 'capella') "
                "AND platform IS NOT NULL AND platform <> constellation"
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
        print(f"Error while updating constellation in parallel: {e}")


if __name__ == "__main__":
    update_constellation_parallel()


# from core.services.database_bulk_constellation_update import update_constellation_parallel
# update_constellation_parallel()