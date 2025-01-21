import psycopg2
from django.db import connection
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

BATCH_SIZE = 10000  # Number of records per batch
THREAD_COUNT = os.cpu_count()  # Number of threads to use (adjust based on your system's resources)

# Function to convert resolution to gsd
def convert_resolution_to_gsd(resolution, vendor_name):
    """Convert the resolution to gsd."""
    try:
        if resolution and 'm' in resolution:
            # Extract the numeric part of the resolution
            final_gsd = float(resolution.replace('m', '').strip())
            if vendor_name == "skyfi-umbra":
                final_gsd = final_gsd / 100  # Adjust for specific vendor if needed
            return final_gsd
        return 0
    except Exception as e:
        print(f"Error while converting resolution to gsd: {e}")
        return 0

# Worker function to process a batch of records
def process_batch(offset, batch_number, total_batches):
    """Process a batch of records to update the gsd column."""
    try:
        print(f"Processing batch {batch_number}/{total_batches} with offset {offset}...")
        with connection.cursor() as cursor:
            # Fetching a batch of records
            cursor.execute(
                "SELECT id, resolution, vendor_name FROM core_satellitecapturecatalog WHERE resolution IS NOT NULL AND gsd=0 LIMIT %s OFFSET %s;",
                [BATCH_SIZE, offset]
            )
            records = cursor.fetchall()

            if not records:
                return 0  # No records left to process

            for record in records:
                record_id = record[0]
                resolution = record[1]
                vendor_name = record[2]

                # Calculate the gsd value
                gsd_value = convert_resolution_to_gsd(resolution, vendor_name)

                if gsd_value is not None:
                    # Update the record
                    cursor.execute(
                        "UPDATE core_satellitecapturecatalog SET gsd = %s WHERE id = %s;",
                        [gsd_value, record_id]
                    )
            connection.commit()
            print(f"Batch {batch_number} completed. Updated {len(records)} records.")
            return len(records)

    except Exception as e:
        print(f"Error while processing batch {batch_number} with offset {offset}: {e}")
        return 0

# Function to update gsd column in parallel
def update_gsd_column_parallel():
    """Update the gsd column in the database using multi-threading."""
    try:
        with connection.cursor() as cursor:
            # Get the total number of records
            cursor.execute(
                "SELECT COUNT(*) FROM core_satellitecapturecatalog WHERE resolution IS NOT NULL AND gsd=0 AND id >= %s;",
                [0]
            )
            total_records = cursor.fetchone()[0]

        # Calculate total batches
        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"Total records: {total_records}, Total batches: {total_batches}")

        processed_records = 0

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            # Submit tasks for each batch
            futures = {
                executor.submit(process_batch, offset, batch_number + 1, total_batches): batch_number + 1
                for batch_number, offset in enumerate(range(0, total_records, BATCH_SIZE))
            }

            # Process results as they complete
            for future in as_completed(futures):
                batch_number = futures[future]
                try:
                    updated_count = future.result()
                    processed_records += updated_count
                    print(f"Progress: {processed_records}/{total_records} records processed.")
                except Exception as e:
                    print(f"Error in batch {batch_number}: {e}")

        print("All batches processed successfully.")
        print(f"Total records updated: {processed_records}/{total_records}")

    except Exception as e:
        print(f"Error while updating gsd column in parallel: {e}")

if __name__ == "__main__":
    update_gsd_column_parallel()


# from core.services.database_bulk_update import update_gsd_column_batch