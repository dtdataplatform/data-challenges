import os
import xml.etree.ElementTree as ET
import pandas as pd
from typing import List, Dict
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RO:
    def __init__(self, order_id, date_time, status, cost, technician, parts):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.parts = parts

def read_files_from_dir(dir_path: str) -> List[str]:
    logging.info(f"Reading XML files from directory: {dir_path}")
    xml_files = []
    try:
        for file in os.listdir(dir_path):
            if file.endswith(".xml"):
                with open(os.path.join(dir_path, file), "r") as f:
                    xml_files.append(f.read())
        logging.info(f"Found {len(xml_files)} XML files in directory")
    except Exception as e:
        logging.error(f"Error occurred while reading files: {e}")
    return xml_files

def parse_xml(files: List[str]) -> pd.DataFrame:
    logging.info("============Parsing XML files============")
    data = []
    try:
        for file_content in files:
            event = ET.fromstring(file_content)
            #for event in root.findall("event"):
            order_id = event.find("order_id").text
            date_time = event.find("date_time").text  # Ensure 'date_time' exists
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
        logging.info("============XML files parsed successfully==============")
    except Exception as e:
        logging.error(f"================Error occurred while parsing XML files: {e}===========")
    return pd.DataFrame(data)

def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    logging.info(f"Windowing data by {window}")
    try:
        # Ensure 'date_time' column exists
        if 'date_time' not in data.columns:
            logging.error("=============DataFrame does not contain 'date_time' column============")
            return {}

        data["date_time"] = pd.to_datetime(data["date_time"])
        grouped = data.groupby(pd.Grouper(key="date_time", freq=window))
        window_data = {str(window): group.tail(1) for window, group in grouped}
        logging.info("=========Data window successfully=========")
        return window_data
    except Exception as e:
        logging.error(f"Error occurred while windowing data: {e}")
        return {}

def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    logging.info("Processing data into RO format")
    ro_list = []
    try:
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
        logging.info("==========Data processed into RO format completed successfully===============")
    except Exception as e:
        logging.error(f"Error occurred while processing data into RO format: {e}")
    return ro_list

def main(dir_path: str, window: str):
    try:
        # Read XML files
        xml_files = read_files_from_dir(dir_path)
        print(xml_files)
        # Parse XML files
        data = parse_xml(xml_files)

        
        # Window data
        windowed_data = window_by_datetime(data, window)
        
        # Process to RO format
        ro_list = process_to_RO(windowed_data)
        
        # Write to SQLite database
        conn = sqlite3.connect('ro_database.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS RO
                     (order_id TEXT, date_time TEXT, status TEXT, cost REAL, technician TEXT, parts TEXT)''')
        for ro in ro_list:
            c.execute("INSERT INTO RO VALUES (?, ?, ?, ?, ?, ?)",
              (ro.order_id, ro.date_time.strftime('%Y-%m-%d %H:%M:%S'), ro.status, ro.cost, ro.technician, str(ro.parts)))

        conn.commit()
        conn.close()
        
        logging.info("Pipeline execution completed successfully")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")

# Example usage:
main("C:/Users/user/test/data-challenges/data-engineer/data/","1D") # Your path
