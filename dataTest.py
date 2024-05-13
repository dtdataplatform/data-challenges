import unittest

class TestParseXML(unittest.TestCase):
    def test_valid_xml_content(self):
        # Test with valid XML content
        xml_content = """
        <event>
            <order_id>101</order_id>
            <date_time>2023-08-10T12:30:00</date_time>
            <status>Completed</status>
            <cost>60.00</cost>
            <repair_details>
                <technician>Jane Smith</technician>
                <repair_parts>
                    <part name="Air Filter" quantity="1"/>
                    <part name="Oil Filter" quantity="1"/>
                </repair_parts>
            </repair_details>
        </event>
        """
        df = parse_xml([xml_content])
        self.assertEqual(len(df), 1)
        # Add more assertions to validate the DataFrame contents

    def test_missing_element(self):
        # Test with XML content missing a required element
        xml_content = """
        <event>
            <order_id>102</order_id>
            <status>Completed</status>
            <cost>60.00</cost>
            <repair_details>
                <technician>Jane Smith</technician>
                <repair_parts>
                    <part name="Air Filter" quantity="1"/>
                    <part name="Oil Filter" quantity="1"/>
                </repair_parts>
            </repair_details>
        </event>
        """
        with self.assertLogs(level='ERROR') as cm:
            df = parse_xml([xml_content])
        self.assertIn("Error occurred while parsing XML files", cm.output[0])

    # Add more test cases for other scenarios such as invalid XML syntax, empty XML content, etc.

if __name__ == '__main__':
    unittest.main()
  
