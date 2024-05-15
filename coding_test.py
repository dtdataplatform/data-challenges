import unittest
import os
import sqlite3
import pandas as pd
from datetime import datetime
from coding_new import read_files_from_dir, parse_xml, window_by_datetime, process_to_RO, main

class TestPipeline(unittest.TestCase):
    # Test reading files from directory
    def test_read_files_from_dir(self):
        dir_path = "data"
        xml_files = read_files_from_dir(dir_path)
        self.assertEqual(len(xml_files), 3)  # Assuming there are 3 XML files in the directory

    # Test parsing XML files
    def test_parse_xml(self):
        files = [
            """<event>
                <order_id>123</order_id>
                <date_time>2023-08-10T12:34:56</date_time>
                <status>Completed</status>
                <cost>100.50</cost>
                <repair_details>
                    <technician>John Doe</technician>
                    <repair_parts>
                        <part name="Brake Pad" quantity="2"/>
                        <part name="Oil Filter" quantity="1"/>
                    </repair_parts>
                </repair_details>
            </event>""",
            # Add more sample XML files here if needed
        ]
        df = parse_xml(files)
        self.assertEqual(len(df), 1)  # Assuming only one event is parsed

    # Test windowing by date_time
    def test_window_by_datetime(self):
        data = pd.DataFrame({
            "order_id": [123, 456, 789],
            "date_time": pd.to_datetime(["2023-08-10T12:34:56", "2023-08-10T15:00:00", "2023-08-11T10:00:00"]),
            "status": ["Completed", "InProgress", "Completed"],
            "cost": [100.50, 200.75, 150.25],
            "technician": ["John Doe", "Jane Smith", "John Doe"],
            "parts": [[{"name": "Brake Pad", "quantity": 2}, {"name": "Oil Filter", "quantity": 1}],
                      [{"name": "Tire", "quantity": 4}],
                      [{"name": "Brake Pad", "quantity": 2}, {"name": "Oil Filter", "quantity": 1}]]
        })
        windowed_data = window_by_datetime(data, '1D')
        self.assertEqual(len(windowed_data), 2)  # Assuming 2 days of data, hence 2 windows

    # Test processing into structured RO format
    def test_process_to_RO(self):
        data = {
            "2023-08-10": pd.DataFrame({
                "order_id": [123],
                "date_time": [datetime(2023, 8, 10, 12, 34, 56)],
                "status": ["Completed"],
                "cost": [100.50],
                "technician": ["John Doe"],
                "parts": [[{"name": "Brake Pad", "quantity": 2}, {"name": "Oil Filter", "quantity": 1}]]
            }),
            "2023-08-11": pd.DataFrame({
                "order_id": [789],
                "date_time": [datetime(2023, 8, 11, 10, 0, 0)],
                "status": ["Completed"],
                "cost": [150.25],
                "technician": ["John Doe"],
                "parts": [[{"name": "Brake Pad", "quantity": 2}, {"name": "Oil Filter", "quantity": 1}]]
            })
        }
        ro_list = process_to_RO(data)
        self.assertEqual(len(ro_list), 2)  # Assuming 2 ROs are processed

    # Test main pipeline
    def test_main(self):
        dir_path = "data"
        window = '1D'
        db_file = 'test_ro_database.db'
        main(dir_path, window, db_file)
        self.assertTrue(os.path.exists(db_file))  # Check if database file is created
        # Check if data is inserted into the database
        with sqlite3.connect(db_file) as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM RO")
            result = c.fetchone()[0]
            self.assertEqual(result, 2)  # Assuming 2 ROs are inserted into the database

if __name__ == '__main__':
    unittest.main()
