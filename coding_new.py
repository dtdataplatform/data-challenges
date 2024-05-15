import os
import xml.etree.ElementTree as ET
import pandas as pd
from typing import List, Dict
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='ro_processing.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def read_files_from_dir(dir_path: str) -> List[str]:
    try:
        xml_files = []
        for file in os.listdir(dir_path):
            if file.endswith(".xml"):
                with open(os.path.join(dir_path, file), "r") as f:
                    xml_files.append(f.read())
        return xml_files
    except Exception as e:
        logging.error("An error occurred - reading files: %s", e)
        return []

def parse_xml(files: List[str]) -> pd.DataFrame:
    try:
        data = []
        for file_content in files:
            event = ET.fromstring(file_content)
            order_id = event.find("order_id").text
            date_time = event.find("date_time").text 
            status = event.find("status").text
            cost = float(event.find("cost").text)
            technician = event.find("repair_details/technician").text
            
            parts = []
            for part in event.findall("repair_details/repair_parts/part"):
                parts.append({
                    "name": part.get("name"),
                    "quantity": int(part.get("quantity"))
                })
            data.append({
                "order_id": order_id,
                "date_time": date_time,
                "status": status,
                "cost": cost,
                "technician": technician,
                "parts": parts
            })
        return pd.DataFrame(data)
    except Exception as e:
        logging.error("An error occurred - parsing XML: %s", e)
        return pd.DataFrame()

def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    try:
        data["date_time"] = pd.to_datetime(data["date_time"])
        grouped = data.groupby(pd.Grouper(key="date_time", freq=window))
        window_data = {str(window): group.tail(1) for window, group in grouped}
        return window_data
    except Exception as e:
        logging.error("An error occurred - grouping data: %s", e)
        return {}

class RO:
    def __init__(self, order_id, date_time, status, cost, technician, parts):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.parts = parts

def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    try:
        ro_list = []
        for window, df in data.items():
            for index, row in df.iterrows():
                ro_list.append(RO(
                    order_id=row["order_id"],
                    date_time=row["date_time"],
                    status=row["status"],
                    cost=row["cost"],
                    technician=row["technician"],
                    parts=row["parts"]
                ))
        return ro_list
    except Exception as e:
        logging.error("An error occurred - processing data to RO objects: %s", e)
        return []

def main(dir_path: str, window: str, db_file: str = 'ro_database.db'):
    """
    Main function to process XML files, insert data into an SQLite database, and create a table.

    Args:
        dir_path (str): Directory path containing XML files.
        window (str): Time window for grouping data.
        db_file (str): SQLite database file name.
    """
    try:
        logging.info("Starting the processing")
        xml_files = read_files_from_dir(dir_path)
        data = parse_xml(xml_files)
        windowed_data = window_by_datetime(data, window)
        ro_list = process_to_RO(windowed_data)

        # Delete existing database file if it exists
        if os.path.exists(db_file):
            os.remove(db_file)

        # Create a new SQLite database connection
        with sqlite3.connect(db_file) as conn:
            c = conn.cursor()

            # Create the table schema
            c.execute('''CREATE TABLE IF NOT EXISTS RO (
                order_id TEXT,
                date_time TEXT,
                status TEXT,
                cost REAL,
                technician TEXT,
                parts TEXT
            )''')

            # Insert data into the table
            for ro in ro_list:
                c.execute("INSERT INTO RO VALUES (?, ?, ?, ?, ?, ?)", (
                    ro.order_id,
                    f'{ro.date_time:%Y-%m-%d %H:%M:%S}',
                    ro.status,
                    ro.cost,
                    ro.technician,
                    str(ro.parts)
                ))

            # Commit changes
            conn.commit()

        logging.info("Processing completed successfully")
    except Exception as e:
        logging.error("An error occurred: %s", e)

main("C:\\Users\\LENOVO\\Downloads\\", "1D")
