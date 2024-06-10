import csv
import os
import time
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Define your Supabase connection details from environment variables
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

# Path to your CSV file
# csv_file_path = './Seat Cover Review DB - 5.29.2024 (Original).csv'
# table_name = 'seat_cover_reviews_20240530_2'

default_csv_file_path = './Car Cover Review DB - 6.04.2024 (V2).csv'
default_table_name = 'car_cover_reviews_20240604'

# Set up argument parser
parser = argparse.ArgumentParser(description='Upload CSV to PostgreSQL')
parser.add_argument('--csv', type=str, help='Path to the CSV file')
parser.add_argument('--table', type=str, help='Name of the database table')

args = parser.parse_args()

# Get values from arguments or use defaults
csv_file_path = args.csv if args.csv else default_csv_file_path
table_name = args.table if args.table else default_table_name

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{table_name}_supabase_upload_log_{timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Log the start of the process
logging.info("Starting CSV upload process")
print("Starting CSV upload process")

# Log and print the CSV file path and table name
logging.info(f"CSV file path: {csv_file_path}")
logging.info(f"Table name: {table_name}")
print(f"CSV file path: {csv_file_path}")
print(f"Table name: {table_name}")

# Function to convert blank strings to None (NULL)
def convert_to_null(value):
    return None if value == "" else value

# Create a Supabase client
supabase: Client = create_client(supabase_url, supabase_key)

# Function to upload CSV to Supabase in batches
def upload_csv_to_supabase(csv_file_path, table_name, batch_size=500, start_row=0):
    try:
        # with open(csv_file_path, 'r', encoding='ISO-8859-1') as f:
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
