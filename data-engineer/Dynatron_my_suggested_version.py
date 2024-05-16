
#!/usr/bin/python
#########################################################################
### PYTHON PROGRAMMING SKILLS ASSESMENT TEST WITH Dynatron for Data Engineer.
### https://github.com/dtdataplatform/data-challenges/blob/main/data-engineer/README.md
###  The script design version of aper tips suggested in the assigmnet
#########################################################################
### REQUIREMENT UNKNOWS AND QUESTIONS :
###    1) There is no grouping criteria grain specified in the assignemt.
###    2) The assigment does not describe the relational structe of the SQL Lite (TABLES)
###      where the hierarchical (XML) data is to be loaded to.
###    3) In TIPS section, Whats is the purpose of keeping the same version of the in different shape and forms?
#########################################################################
###  REVISIONS:
###     2024-05-11 sergey doubov  initial
#########################################################################
import os
import glob
import xml.etree.ElementTree as ET
import pandas as pd
from typing import List, Dict
import datetime
import sqlite3
import sys

def initialize_logging(filelogpath):
	class Logger(object):
		def __init__(self):
			self.terminal = sys.stdout
			self.log = open(filelogpath, "a")  #overwrites, not appends
		def write(self, message):
			self.terminal.write(message)
			self.log.write(message)
		def flush(self):
			self.log.flush()
	logger1 = Logger()
	sys.stdout = logger1
	sys.stderr = logger1
	return logger1

def get_file_names(directory: str) -> List[str]:
    if os.path.exists(directory):
        return glob.glob(os.path.join(directory, '*.xml'))
    else:
        print(f"ERROR: The directory {directory} not found.")
        sys.exit()
        return None


def parse_xml(filenames: str) -> pd.DataFrame:
    data = {
        'order_id': [],
        'date_time': [],
        'status': [],
        'cost': [],
        'technician': [],
        'repair_parts': []
    }
    for myfile in filenames:

        with open(myfile, 'r') as file:
            file_content = file.read()
        try:
            root = ET.fromstring(file_content)
            for event in root.findall('.'):
                data['order_id'].append(event.find('order_id').text)
                data['date_time'].append(pd.to_datetime(event.find('date_time').text))
                data['status'].append(event.find('status').text)
                data['cost'].append(float(event.find('cost').text))
                data['technician'].append(event.find('repair_details/technician').text)
                ##data['repair_parts'].append([(part.attrib['name'], int(part.attrib['quantity'])) for part in event.findall('repair_details/repair_parts/part')])

                my_repair_parts = ''
                for part in event.findall('repair_details/repair_parts/part'):
                     my_repair_parts = my_repair_parts + ',' + part.attrib['name']+ '=>' + part.attrib['quantity']

                data['repair_parts'].append(my_repair_parts)
        except Exception as e:
            print("Error parsing XML files: {}".format(myfile))

    df = pd.DataFrame(data)
    return df

def load_into_db_table(conn1 , df1: pd.DataFrame):
    cursor = conn1.cursor()

    cursor.execute('DROP TABLE IF EXISTS repair_orders')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS repair_orders (
        order_id VARCHAR,
        date_time VARCHAR,
        status VARCHAR,
        cost VARCHAR,
        technician VARCHAR,
        repair_parts VARCHAR
    );
    ''')
    df1.to_sql('repair_orders', conn1, if_exists='append',index=False)
    conn1.commit()
    cursor.close()

# Main pipeline
def main(directory: str, window: str):

    ct = datetime.datetime.now()
    logfilepath = r'{}\_logs\{}_{}.log'.format(directory,datetime.datetime.now().strftime("%Y%m%d"),os.path.basename(__file__))
    initialize_logging(logfilepath)

    print("START EXECUTION AT: ".format(ct))

    # Step 1: Read file names
    filenames = get_file_names(directory)
    #print (filenames)

    # Step 2: Parse XML
    data = parse_xml(filenames)
    print(data)

    conn = sqlite3.connect('example.db')
    cursor = conn.cursor()

### Load into flat table
    load_into_db_table(conn , data)

    cursor.execute('''SELECT
                             order_id  
                            ,strftime('%Y-%m-%d',date_time) as DATE_AGG
                            ,date_time  
                            ,status     
                            ,cost       
                            ,technician
                            ,repair_parts
                        FROM repair_orders
                        ''')

    for row in cursor.fetchall():
        print(row)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    directory = r'C:\Users\flant\Downloads\test2drop'
    window = "D"
    main(directory, window)



