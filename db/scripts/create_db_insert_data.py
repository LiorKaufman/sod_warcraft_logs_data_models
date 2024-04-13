import psycopg2
import pandas as pd
import logging
import os
import json
from pathlib import Path
from datetime import datetime
import re
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


def get_conn():
    logger.info("Creating Database Connection...")
    conn = psycopg2.connect(
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
    )

    cur = conn.cursor()
    logger.info("Connection to Database established")

    return conn, cur


def extract_date_from_filename(filename):
    # Regex to find a date pattern YYYYMMDD in the filename
    match = re.search(r'\d{8}', filename)
    if match:
        date_str = match.group(0)
        try:
            # Convert string to date object
            return datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            print("Error: Date format is incorrect.")
            return None
    else:
        print("Error: No date found in the filename.")
        return None


if __name__ == "__main__":
    conn, cur = get_conn()

    logger.info("Creating tables...")

    # Ensure the table is created, this SQL needs to be executed prior in your DB management tools or here if necessary
    table_creation_query = """
    CREATE TABLE IF NOT EXISTS wcl_reports (
        id SERIAL PRIMARY KEY,
        json_col JSONB,
        file_created_at DATE,
        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cur.execute(table_creation_query)
    conn.commit()

    logger.info("Inserting data to DB...")

    data_folder = Path(__file__).resolve().parent.parent / "data"
    json_files = data_folder.glob('*.json')

    for json_file in json_files:
        file_date = extract_date_from_filename(json_file.name)
        if not file_date:
            continue

        with open(json_file, 'r') as file:
            json_data = json.load(file)

        try:
            logger.info(
                "Uploading data from {} to the table wcl_reports...".format(json_file.name))
            cur.execute(
                "INSERT INTO wcl_reports (json_col, file_created_at) VALUES (%s, %s)",
                (json.dumps(json_data), file_date)
            )
            conn.commit()
            logger.info(
                "Data from {} uploaded to the Database".format(json_file.name))
        except Exception as e:
            logger.error(
                "Error uploading data from {}: {}".format(json_file.name, e))

    cur.close()
    conn.close()
