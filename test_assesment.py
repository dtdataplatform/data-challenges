import unittest
import os
import pandas as pd
from unittest.mock import patch, MagicMock,ANY
from assesment import pipeline, read_files_from_dir, parse_xml, window_by_datetime, process_to_RO, connect_db, write_to_sqlite

class TestPipeline(unittest.TestCase):

    @patch('assesment.read_files_from_dir')
    @patch('assesment.parse_xml')
    @patch('assesment.window_by_datetime')
    @patch('assesment.process_to_RO')
    @patch('assesment.connect_db')
    @patch('assesment.write_to_sqlite')

    def test_pipeline(self, mock_write_to_sqlite, mock_connect_db, mock_process_to_RO, mock_window_by_datetime, mock_parse_xml, mock_read_files_from_dir):
        # Arrange
        mock_read_files_from_dir.return_value = ['data-engineer/data/shard1.xml', 'data-engineer/data/shard2.xml']
        mock_parse_xml.return_value = pd.DataFrame()
        mock_window_by_datetime.return_value = {}
        mock_process_to_RO.return_value = []
        mock_connect_db.return_value = MagicMock()

        # Act
        pipeline('/home/reddym00/data-engineer/data', '1D', 'ro_output.db')

        # Assert
        mock_read_files_from_dir.assert_called_once_with('data-engineer/data')
        mock_parse_xml.assert_called_once_with(['data-engineer/data/shard1.xml', 'data-engineer/data/shard2.xml'])
        mock_window_by_datetime.assert_called_once_with(ANY, '1D')
        mock_process_to_RO.assert_called_once_with({})
        mock_connect_db.assert_called_once()
        mock_write_to_sqlite.assert_called_once_with([], 'ro_output.db', mock_connect_db.return_value)

if __name__ == '__main__':
    unittest.main()