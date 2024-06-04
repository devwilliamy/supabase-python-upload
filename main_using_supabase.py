import csv
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Define your Supabase connection details from environment variables
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

# Path to your CSV file
csv_file_path = './Seat Cover Review DB - 5.29.2024 (Original).csv'
table_name = 'seat_cover_reviews_20240530_2'

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"upload_log_{timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Log the start of the process
logging.info("Starting CSV upload process")
print("Starting CSV upload process")

# Function to convert blank strings to None (NULL)
def convert_to_null(value):
    return None if value == "" else value

# Create a Supabase client
supabase: Client = create_client(supabase_url, supabase_key)

# Function to upload CSV to Supabase in batches
def upload_csv_to_supabase(csv_file_path, table_name, batch_size=500, start_row=190000):
    try:
        with open(csv_file_path, 'r', encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip the header row

            rows = []
            total_rows = start_row
            total_time = 0

            start_skip_time = time.time()
            for _ in range(start_row):
                next(reader, None)
            end_skip_time = time.time()
            skip_duration = end_skip_time - start_skip_time
            logging.info(f"Skipped {start_row} rows in {skip_duration:.2f} seconds")
            print(f"Skipped {start_row} rows in {skip_duration:.2f} seconds")

            for i, row in enumerate(reader, start=start_row + 1):
                processed_row = {header[j]: convert_to_null(value) for j, value in enumerate(row)}
                rows.append(processed_row)
                if (i - start_row) % batch_size == 0:
                    start_time = time.time()
                    retry_count = 0
                    while retry_count <= 3:
                        try:
                            response = supabase.table(table_name).insert(rows).execute()
                            # if response.status_code != 201:
                            #     raise Exception(f"Failed to insert batch: {response.json()}")
                            break
                        except Exception as e:
                            logging.error(f"Error inserting batch at row {total_rows}: {e}")
                            print(f"Error inserting batch at row {total_rows}: {e}")
                            retry_count += 1
                            if retry_count > 3:
                                raise
                            logging.info(f"Retrying batch at row {total_rows}, attempt {retry_count}")
                            print(f"Retrying batch at row {total_rows}, attempt {retry_count}")
                            time.sleep(5)  # Wait before retrying
                    end_time = time.time()

                    batch_time = end_time - start_time
                    total_time += batch_time
                    total_rows += len(rows)

                    logging.info(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
                    print(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
                    rows = []

                    # Add a delay between batch inserts
                    time.sleep(2)

            # Insert any remaining rows
            if rows:
                start_time = time.time()
                retry_count = 0
                while retry_count <= 3:
                    try:
                        response = supabase.table(table_name).insert(rows).execute()
                        # if response.status_code != 201:
                        #     raise Exception(f"Failed to insert final batch: {response.json()}")
                        break
                    except Exception as e:
                        logging.error(f"Error inserting final batch at row {total_rows}: {e}")
                        print(f"Error inserting final batch at row {total_rows}: {e}")
                        retry_count += 1
                        if retry_count > 3:
                            raise
                        logging.info(f"Retrying final batch at row {total_rows}, attempt {retry_count}")
                        print(f"Retrying final batch at row {total_rows}, attempt {retry_count}")
                        time.sleep(5)  # Wait before retrying
                end_time = time.time()

                batch_time = end_time - start_time
                total_time += batch_time
                total_rows += len(rows)

                logging.info(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
                print(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Failed to upload CSV to Supabase: {e}")
        print(f"Failed to upload CSV to Supabase: {e}")

# Upload CSV to Supabase in batches
upload_csv_to_supabase(csv_file_path, table_name)

logging.info("CSV upload complete.")
print("CSV upload complete.")
