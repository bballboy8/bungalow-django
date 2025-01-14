import psycopg2
from django.db import connection
from django.conf import settings

BATCH_SIZE = 10000  # You can adjust the batch size based on your performance considerations

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

# Function to update gsd in batches
def update_gsd_column_batch():
    """Update the gsd column in the database based on the resolution column in batches."""
    try:
        # Initialize a connection and cursor
        with connection.cursor() as cursor:
            offset = 0
            while True:
                # Fetching a batch of records to update
                cursor.execute(
                    "SELECT id, resolution, vendor_name FROM core_satellitecapturecatalog WHERE resolution IS NOT NULL LIMIT %s OFFSET %s;",
                    [BATCH_SIZE, offset]
                )
                records = cursor.fetchall()
                
                if not records:
                    break  # Exit loop if no more records are left to process
                
                # Processing each record in the batch
                for record in records:
                    record_id = record[0]
                    resolution = record[1]
                    vendor_name = record[2]

                    # Calculate the gsd value based on resolution
                    gsd_value = convert_resolution_to_gsd(resolution, vendor_name)

                    if gsd_value is not None:
                        # Update the record with the new gsd value
                        cursor.execute(
                            "UPDATE core_satellitecapturecatalog SET gsd = %s WHERE id = %s;",
                            [gsd_value, record_id]
                        )
                
                # Commit the batch updates
                connection.commit()
                
                # Move the offset to the next batch
                offset += BATCH_SIZE

            print(f"Successfully updated gsd for all relevant records.")

    except Exception as e:
        print(f"Error while updating gsd column: {e}")

if __name__ == "__main__":
    update_gsd_column_batch()

# from core.services.database_bulk_update import update_gsd_column_batch