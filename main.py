import psycopg2
import csv
from dotenv import load_dotenv
import os
import time
import logging
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Define your Supabase connection details from environment variables
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Path to your CSV file
# csv_file_path = './Seat Cover Review DB - 5.29.2024 (Truncate-1).csv'
csv_file_path = './Seat Cover Review DB - 5.29.2024 (Original).csv'
table_name='seat_cover_reviews_20240530_2'
# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"upload_log_{timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Log the start of the process
logging.info("Starting CSV upload process")

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    dbname=db_config['dbname'],
    user=db_config['user'],
    password=db_config['password']
)

def convert_to_null(value):
    return None if value == "" else value

# Create a cursor object
cur = conn.cursor()


# Function to upload CSV to PostgreSQL in batches
def upload_csv_to_postgres(csv_file_path, table_name, batch_size=500):
    try:

    # with open(csv_file_path, 'r') as f:
        with open(csv_file_path, 'r', encoding='ISO-8859-1') as f:

            reader = csv.reader(f)
            header = next(reader)  # Skip the header row

            escaped_header = [f'"{col}"' for col in header]

            rows = []
            total_rows = 0
            total_time = 0
            
            # Create an insert query template
            query = f"INSERT INTO {table_name} ({', '.join(escaped_header)}) VALUES ({', '.join(['%s'] * len(header))})"

            for i, row in enumerate(reader, start=1):
                processed_row = [convert_to_null(value) for value in row]
                rows.append(tuple(processed_row))
                if i % batch_size == 0:
                    start_time = time.time()
                    try:
                        cur.executemany(query, rows)
                        conn.commit()
                    except Exception as e:
                        logging.error(f"Error inserting batch at row {total_rows}: {e}")
                        print(f"Error inserting batch at row {total_rows}: {e}")
                        raise
                    end_time = time.time()
                    
                    batch_time = end_time - start_time
                    total_time += batch_time
                    total_rows += len(rows)
                    
                    logging.info(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
                    print(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
                    rows = []
            
            # Insert any remaining rows
            if rows:
                start_time = time.time()
                try:
                    cur.executemany(query, rows)
                    conn.commit()
                except Exception as e:
                    logging.error(f"Error inserting final batch at row {total_rows}: {e}")
                    print(f"Error inserting final batch at row {total_rows}: {e}")
                    raise
                end_time = time.time()
                
                batch_time = end_time - start_time
                total_time += batch_time
                total_rows += len(rows)
                
                logging.info(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
                print(f"Inserted {total_rows} rows in {batch_time:.2f} seconds. Total time: {total_time:.2f} seconds")
    except Exception as e:
            logging.error(f"Failed to upload CSV to PostgreSQL: {e}")
            print(f"Failed to upload CSV to PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()
# Upload CSV to PostgreSQL in batches
upload_csv_to_postgres(csv_file_path, table_name)

# Close the cursor and connection
cur.close()
conn.close()

logging.info("CSV upload complete.")
print("CSV upload complete.")
