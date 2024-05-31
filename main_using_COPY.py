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
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    dbname=db_config['dbname'],
    user=db_config['user'],
    password=db_config['password']
)
cur = conn.cursor()

try:
    with open(csv_file_path, 'r', encoding='ISO-8859-1') as f:
        # Use COPY command for bulk insert
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
    conn.commit()
    logging.info("CSV upload complete.")
    print("CSV upload complete.")
except Exception as e:
    logging.error(f"Failed to upload CSV to PostgreSQL: {e}")
    print(f"Failed to upload CSV to PostgreSQL: {e}")
finally:
    cur.close()
    conn.close()
