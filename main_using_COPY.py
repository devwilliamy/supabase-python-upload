import psycopg2
import csv
from dotenv import load_dotenv
import os
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
table_name = 'seat_cover_reviews_20240530_4'

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"upload_log_{timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Log the start of the process
logging.info("Starting CSV upload process")
print("Starting CSV upload process")

# Connect to the PostgreSQL database
def create_connection():
    return psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )

# Function to upload CSV to PostgreSQL using COPY
def upload_csv_using_copy(csv_file_path, table_name):
    conn = create_connection()
    cur = conn.cursor()

    try:
        # Set statement timeout to 0 (no timeout)
        cur.execute("SET statement_timeout TO 0;")

        with open(csv_file_path, 'r', encoding='ISO-8859-1') as f:
            copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER"
            start_time = datetime.now()
            cur.copy_expert(copy_sql, f)
            conn.commit()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"CSV upload complete in {duration:.2f} seconds")
            print(f"CSV upload complete in {duration:.2f} seconds")
    except Exception as e:
        logging.error(f"Failed to upload CSV to PostgreSQL: {e}")
        print(f"Failed to upload CSV to PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()

# Upload CSV to PostgreSQL
upload_csv_using_copy(csv_file_path, table_name)

logging.info("CSV upload process completed.")
print("CSV upload process completed.")
