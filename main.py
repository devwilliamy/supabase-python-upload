import psycopg2
import csv
from dotenv import load_dotenv
import os
import time
import logging
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Define your PostgreSQL connection details from environment variables
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

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

# Function to establish a new connection to the database
def create_connection():
    return psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )

# Function to upload CSV to PostgreSQL in batches, starting from a specific row
def upload_csv_to_postgres(csv_file_path, table_name, batch_size=500, start_row=108500, max_retries=3):
    conn = create_connection()
    cur = conn.cursor()

    try:
        with open(csv_file_path, 'r', encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip the header row

            escaped_header = [f'"{col}"' for col in header]

            rows = []
            total_rows = start_row
            total_time = 0
            
            # Create an insert query template
            query = f"INSERT INTO {table_name} ({', '.join(escaped_header)}) VALUES ({', '.join(['%s'] * len(header))})"
            
            start_skip_time = time.time()
            for _ in range(start_row):
                next(reader, None)
            end_skip_time = time.time()
            skip_duration = end_skip_time - start_skip_time
            logging.info(f"Skipped {start_row} rows in {skip_duration:.2f} seconds")
            print(f"Skipped {start_row} rows in {skip_duration:.2f} seconds")
            
            for i, row in enumerate(reader, start=start_row + 1):
                processed_row = [convert_to_null(value) for value in row]
                rows.append(tuple(processed_row))
                if (i - start_row) % batch_size == 0:
                    start_time = time.time()
                    retry_count = 0
                    while retry_count <= max_retries:
                        try:
                            cur.executemany(query, rows)
                            conn.commit()
                            break
                        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                            logging.error(f"Error inserting batch at row {total_rows}: {e}")
                            print(f"Error inserting batch at row {total_rows}: {e}")
                            conn.rollback()
                            retry_count += 1
                            if retry_count > max_retries:
                                raise
                            logging.info(f"Retrying batch at row {total_rows}, attempt {retry_count}")
                            print(f"Retrying batch at row {total_rows}, attempt {retry_count}")
                            time.sleep(50)  # Wait before retrying
                            conn = create_connection()
                            cur = conn.cursor()
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
                retry_count = 0
                while retry_count <= max_retries:
                    try:
                        cur.executemany(query, rows)
                        conn.commit()
                        break
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        logging.error(f"Error inserting final batch at row {total_rows}: {e}")
                        print(f"Error inserting final batch at row {total_rows}: {e}")
                        conn.rollback()
                        retry_count += 1
                        if retry_count > max_retries:
                            raise
                        logging.info(f"Retrying final batch at row {total_rows}, attempt {retry_count}")
                        print(f"Retrying final batch at row {total_rows}, attempt {retry_count}")
                        time.sleep(5)  # Wait before retrying
                        conn = create_connection()
                        cur = conn.cursor()
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

logging.info("CSV upload complete.")
print("CSV upload complete.")
