from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
from boto3.dynamodb.types import Decimal


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


table_hcp = dynamodb.Table(table_name_hcp)
table_clc = dynamodb.Table(table_name_clc)

# Default values for each column
default_values_hcp = {
    'Calls_Traditionalist': 1, 'Calls_Digital_savvy': 1, 'Calls_Hybrid': 1, 'Calls_Status': True,
    'RTE_Traditionalist': 1, 'RTE_Digital_savvy': 1, 'RTE_Hybrid': 1, 'RTE_Status': True,
    'HOE_Traditionalist': 1, 'HOE_Digital_savvy': 1, 'HOE_Hybrid': 1, 'HOE_Status': True,
    '3P_Media_Traditionalist': 1, '3P_Media_Digital_savvy': 1, '3P_Media_Hybrid': 1, '3P_Status': True
}


default_values_clc = {
    'Calls': 1, 'RTE': 1, 'End_date': '2023-12-31', 'Start_date': '2023-01-01', '3P_Media': 1, 'Status': 'Active', 'HOE': 1
}

class RequestHandler(BaseHTTPRequestHandler):

    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message).encode())

    def _insert_items(self, table, items):
        # Use batch write for inserting multiple items
        with table.batch_writer() as batch:
            for item in items:
                # Get the next 'Id' value
                next_id = self._get_next_id(table)
                # Insert data into DynamoDB table
                item['Id'] = int(next_id)
                # Insert item into DynamoDB table
                batch.put_item(Item=item)

    def _get_next_id(self, table):
        # Scan the table to find the maximum value of the 'Id' attribute
        response = table.scan(ProjectionExpression='Id', Limit=1)
        items = response.get('Items', [])
        previous_id = items[0]['Id'] if items and 'Id' in items[0] else 0
        return previous_id + 1

    def _convert_decimal_to_int(self, data):
        if isinstance(data, Decimal):
            return int(data)
        elif isinstance(data, list):
            return [self._convert_decimal_to_int(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._convert_decimal_to_int(value) for key, value in data.items()}
        else:
            return data

    def _get_last_added_data(self, table):
        response = table.scan()
        items = response.get('Items', [])

        largest_id_data = 0
        largest_id = 0

        for item in items:
            current_id = item.get('Id', 0)
            if current_id > largest_id:
                largest_id = current_id
                largest_id_data = item

        if largest_id_data and 'Id' in largest_id_data:
            largest_id_data['Id'] = int(largest_id_data['Id'])

        return largest_id_data

    def _validate_conditions_hcp(self, data):
        if (data['Calls_Traditionalist'] == data['RTE_Traditionalist'] or
            data['Calls_Traditionalist'] == data['HOE_Traditionalist'] or
            data['Calls_Traditionalist'] == data['3P_Media_Traditionalist'] or
            data['RTE_Traditionalist'] == data['HOE_Traditionalist'] or
            data['RTE_Traditionalist'] == data['3P_Media_Traditionalist'] or
            data['HOE_Traditionalist'] == data['3P_Media_Traditionalist']):
            return False

        if (data['Calls_Digital_savvy'] == data['RTE_Digital_savvy'] or
            data['Calls_Digital_savvy'] == data['HOE_Digital_savvy'] or
            data['Calls_Digital_savvy'] == data['3P_Media_Digital_savvy'] or
            data['RTE_Digital_savvy'] == data['HOE_Digital_savvy'] or
            data['RTE_Digital_savvy'] == data['3P_Media_Digital_savvy'] or
            data['HOE_Digital_savvy'] == data['3P_Media_Digital_savvy']):
            return False

        if (data['Calls_Hybrid'] == data['RTE_Hybrid'] or
            data['Calls_Hybrid'] == data['HOE_Hybrid'] or
            data['Calls_Hybrid'] == data['3P_Media_Hybrid'] or
            data['RTE_Hybrid'] == data['HOE_Hybrid'] or
            data['RTE_Hybrid'] == data['3P_Media_Hybrid'] or
            data['HOE_Hybrid'] == data['3P_Media_Hybrid']):
            return False

        if (data['Calls_Status'] is None or
            data['HOE_Status'] is None or 
            data['RTE_Status'] is None or 
            data['3P_Status'] is None or 
            not isinstance(data['Calls_Status'], bool) or 
            not isinstance(data['RTE_Status'], bool) or 
            not isinstance(data['HOE_Status'], bool) or 
            not isinstance(data['3P_Status'], bool)):
            return False

        return True


    def do_GET(self):
        if self.path == '/HCP':
            try:
                # Get the row with the largest ID for HCP table
                largest_id_data_hcp = self._get_last_added_data(table_hcp)
                data = self._convert_decimal_to_int(largest_id_data_hcp)
                # Return the data of the row with the largest ID for HCP table
                self._send_response(200, {'data': data})
            except Exception as e:
                print(f"Error processing GET request: {e}")
                self._send_response(500, {'error': 'Internal server error'})

        elif self.path == '/CLC':
            try:
                # Get the row with the largest ID for CLC table
                largest_id_data_clc = self._get_last_added_data(table_clc)
                data = self._convert_decimal_to_int(largest_id_data_clc)
                # Return the data of the row with the largest ID for HCP table
                self._send_response(200, {'data': data})
            except Exception as e:
                print(f"Error processing GET request: {e}")
                self._send_response(500, {'error': f'Internal server error: {e}'})

        else:
            self._send_response(404, {'error': 'Not Found'})


    def do_POST(self):
        if self.path == '/HCP':
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))

                # Validate payload keys
                required_keys = ['Calls_Traditionalist', 'Calls_Digital_savvy', 'Calls_Hybrid',
                             'RTE_Traditionalist', 'RTE_Digital_savvy', 'RTE_Hybrid',
                             'HOE_Traditionalist', 'HOE_Digital_savvy', 'HOE_Hybrid',
                             '3P_Media_Traditionalist', '3P_Media_Digital_savvy', '3P_Media_Hybrid']

                for key in required_keys:
                    if key not in data:
                        self._send_response(400, {'error': f'Missing required key: {key}'})
                        return

                # Type validation and conversion
                for key, value in data.items():
                    if key in required_keys:
                        if not isinstance(value, int):
                            self._send_response(400, {'error': f'Type validation failed for column {key}'})
                            return

                # Validate conditions
                if not self._validate_conditions_hcp(data):
                    self._send_response(400, {'error': 'Conditions not met'})
                    return

                # Insert data into DynamoDB table using batch write
                self._insert_items(table_hcp, [data])
                self._send_response(201, {'message': 'Data inserted successfully', 'data': data})

            except Exception as e:
                print(f"Error processing POST request for HCP: {e}")
                self._send_response(500, {'error': 'Internal server error'})

        elif self.path == '/CLC':
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))

                # Validate payload keys
                required_keys = ['Calls', 'RTE', 'End_date', 'Start_date', '3P_Media', 'Status', 'HOE']

                for key in required_keys:
                    if key not in data:
                        self._send_response(400, {'error': f'Missing required key: {key}'})
                        return

                # Type validation and conversion
                for key, value in data.items():
                    if key in required_keys:
                        if value is None:
                            self._send_response(400, {'error': f'Not null validation failed for column {key}'})
                            return

                        if key in ['Calls', 'RTE', '3P_Media', 'HOE'] and not isinstance(value, int):
                            self._send_response(400, {'error': f'Type validation failed for column {key}'})
                            return

                        if key in ['End_date', 'Start_date', 'Status'] and not isinstance(value, str):
                            self._send_response(400, {'error': f'Type validation failed for column {key}'})
                            return

                # Insert data into DynamoDB table using batch write
                self._insert_items(table_clc, [data])
                self._send_response(201, {'message': 'Data inserted successfully', 'data': data})

            except Exception as e:
                print(f"Error processing POST request for CLC: {e}")
                self._send_response(500, {'error': f'Internal server error: {e}'})

        else:
            self._send_response(404, {'error': 'Not Found'})


if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on http://localhost:8080')
    httpd.serve_forever()
