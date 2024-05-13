import os
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List, Dict
import sqlite3
import logging

# create a log file in the desired log directory
logging.basicConfig(filename='/data-engineer/data/pipeline.log', level=logging.INFO)

# class to represent Repair Order
class RO:
    def __init__(self, order_id, date_time, status, cost, technician, parts):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.parts = parts

# function to read files from directory
def read_files_from_dir(dir: str) -> List[str]:
    logging.info(f'Reading files from directory: {dir}')
    files = []
    if os.path.exists(dir):  # Check if the directory exists
        for filename in os.listdir(dir):
            logging.info(f'Reading file: {filename}')
            if filename.endswith(".xml"):
                file_path = os.path.join(dir, filename)
                if os.path.exists(file_path):  # Check if the file exists
                     files.append(file_path)
                else:
                    logging.error(f'File {file_path} does not exist.')
    else:
        logging.error(f'Directory {dir} does not exist.')
    logging.info(f'Finished reading {len(files)} files from directory: {dir}')
    return files

# function to parse xml files
def parse_xml(files: List[str]) -> pd.DataFrame:
    logging.info('<------ Started parsing XML files ------->')
    data = []
    for file in files:
        try:
            logging.info(f'Parsing file: {file}')
            tree = ET.parse(file)
            for event in tree.iter('event'):
                order_id = event.find('order_id')
                if order_id is not None:
                    order_id = order_id.text
                date_time = event.find('date_time')
                if date_time is not None:
                    date_time = date_time.text
                status = event.find('status')
                if status is not None:
                    status = status.text
                cost = event.find('cost')
                if cost is not None:
                    cost = cost.text
                technician = event.find('repair_details/technician')
                if technician is not None:
                    technician = technician.text
                parts = [(part.attrib['name'], part.attrib['quantity']) for part in event.findall('repair_details/repair_parts/part')]
                data.append([order_id, date_time, status, cost, technician, parts])
            logging.info(f'Completed parsing file: {file}')
        except ET.ParseError:
            logging.error(f'File {file} is not well-formed XML and will be skipped.')
    logging.info('<------Finished parsing all XML files ------->')
    df = pd.DataFrame(data, columns=['order_id', 'date_time', 'status', 'cost', 'technician', 'parts']) 
    logging.info(f'DataFrame is : {df}')
    return df

# function to window data by datetime
def window_by_datetime(df_RO: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    logging.info('Windowing data by datetime')
    df_RO['date_time'] = pd.to_datetime(df_RO['date_time'])
    df_RO = df_RO.sort_values('date_time', ascending=False)
    df_RO.set_index('date_time', inplace=True)
    windows = {}
    for name, group in df_RO.groupby(pd.Grouper(freq=window)):
        windows[str(name)] = group
    logging.info('Finished windowing data by datetime')
    logging.info(f'Windows are : {windows}')
    return windows

# function to process data into RO format
def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    logging.info('Processing data into RO format')
    ro_list = []
    for window, df in data.items():
        for index, row in df.iterrows():
            ro = RO(row['order_id'], str(index), row['status'], row['cost'], row['technician'], row['parts'])
            ro_list.append(ro)
    logging.info('Finished processing data into RO format')
    logging.info(f'RO List is : {ro_list}')
    return ro_list

# function to connect to sqlite database
def connect_db():
    conn = sqlite3.connect('ro.db')
    logging.info("connected to sqlite database")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ro (order_id INTEGER,date_time TEXT,status TEXT,cost REAL,technician TEXT)''')
    return conn

# function to write data to sqlite database table RO
def write_to_sqlite(ro_list: List[RO], db_name: str,conn):
    logging.info(f'Writing data to SQLite database: {db_name}')
    conn = sqlite3.connect(db_name)
    for ro in ro_list:
        conn.execute("INSERT INTO RO (order_id, date_time, status, cost, technician, parts) VALUES (?, ?, ?, ?, ?, ?)",
                     (ro.order_id, ro.date_time, ro.status, ro.cost, ro.technician, str(ro.parts)))
    conn.commit()
    conn.close()
    logging.info(f'Finished writing data to SQLite database: {db_name}')

# function to pipeline the data
def pipeline(dir: str, window: str, db_name: str):
    logging.basicConfig(filename='pipeline.log', level=logging.INFO)
    logging.info('Starting pipeline')
    try:
        files = read_files_from_dir(dir)
        df = parse_xml(files)
        windows = window_by_datetime(df, window)
        ro_list = process_to_RO(windows)
        conn = connect_db()
        write_to_sqlite(ro_list, db_name,conn)
        logging.info('Finished pipeline')
    except Exception as e:
        logging.error(f'Error occurred: {e}')

pipeline('data-engineer/data', '1D', 'ro_output.db')