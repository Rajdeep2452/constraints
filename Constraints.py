from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import boto3
from boto3.dynamodb.types import Decimal
from config import *


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)



# Get a reference to your table
table_hcp = dynamodb.Table(table_name_hcp)
table_clc = dynamodb.Table(table_name_clc)

# Default values for each column
default_values_hcp = {
    'Calls_Traditionalist': 1, 'Calls_Digital_savvy': 1, 'Calls_Hybrid': 1, 'Calls_SegD': 1,
    'RTE_Traditionalist': 1, 'RTE_Digital_savvy': 1, 'RTE_Hybrid': 1, 'RTE_SegD': 1,
    'HOE_Traditionalist': 1, 'HOE_Digital_savvy': 1, 'HOE_Hybrid': 1, 'HOE_SegD': 1,
    '3P_Media_Traditionalist': 1, '3P_Media_Digital_savvy': 1, '3P_Media_Hybrid': 1, '3P_Media_SegD': 1
}

hidden_column_hcp = ['SegD']

default_values_clc = {
    'Calls': 1, 'RTE': 1, 'End_date': '2023-12-31', 'Start_date': '2023-01-01', '3P_Media': 1, 'Status': 'Active', 'HOE': 1
}

class RequestHandler(BaseHTTPRequestHandler):

    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message).encode())

    def _hide_columns(self, item):
        for col in hidden_column_hcp:
            item.pop(col, None)
    
    def _get_next_id(self, table):
        # Scan the table to find the maximum value of the 'Id' attribute
        response = table.scan(ProjectionExpression='Id', Limit=1)
        items = response.get('Items', [])
        previous_id = items[0]['Id'] if items and 'Id' in items[0] else 0
        return previous_id + 1

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

        if (data['Calls_SegD'] == data['RTE_SegD'] or
            data['Calls_SegD'] == data['HOE_SegD'] or
            data['Calls_SegD'] == data['3P_Media_SegD'] or
            data['RTE_SegD'] == data['HOE_SegD'] or
            data['RTE_SegD'] == data['3P_Media_SegD'] or
            data['HOE_SegD'] == data['3P_Media_SegD']):
            return False

        return True

    def _validate_conditions_clc(self, data):
        # Validate not null and data type for each column
        for key, value in data.items():
            if key == 'Calls':
                if value is None or not isinstance(value, int):
                    return False
            elif key == 'RTE':
                if value is None or not isinstance(value, int):
                    return False
            elif key == 'End_date':
                if value is None or not isinstance(value, str):
                    return False
            elif key == 'Start_date':
                if value is None or not isinstance(value, str):
                    return False
            elif key == '3P_Media':
                if value is None or not isinstance(value, int):
                    return False
            elif key == 'Status':
                if value is None or not isinstance(value, str):
                    return False
            elif key == 'HOE':
                if value is None or not isinstance(value, int):
                    return False
        return True

    def do_GET(self):
        if self.path == '/HCP':
            try:
                response = table_hcp.scan()
                items = response.get('Items', [])

                result = []
                for item in items:
                    validated_item = {}
                    for col in default_values_hcp.keys():
                        if col not in hidden_column_hcp:
                            col_value = item.get(col, default_values_hcp[col])
                            col_value = int(col_value) if isinstance(col_value, Decimal) else col_value

                            if not isinstance(col_value, int):
                                self._send_response(500, {'error': f'Type validation failed for column {col}'})
                                return
                            if col_value is None:
                                self._send_response(500, {'error': f'Not null validation failed for column {col}'})
                                return
                            validated_item[col] = col_value
                    result.append(validated_item)

                self._send_response(200, result)
            except Exception as e:
                print(f"Error processing GET request: {e}")
                self._send_response(500, {'error': 'Internal server error'})

        elif self.path == '/CLC':
            try:
                response = table_clc.scan()
                items = response.get('Items', [])

                result = []
                for item in items:
                    validated_item = {}
                    for col in default_values_clc.keys():
                        col_value = item.get(col, default_values_clc[col])
                        col_value = str(col_value) if isinstance(col_value, Decimal) else col_value

                        validated_item[col] = col_value
                    result.append(validated_item)

                self._send_response(200, result)

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
                required_keys = ['Calls_Traditionalist', 'Calls_Digital_savvy', 'Calls_Hybrid', 'Calls_SegD',
                                 'RTE_Traditionalist', 'RTE_Digital_savvy', 'RTE_Hybrid', 'RTE_SegD',
                                 'HOE_Traditionalist', 'HOE_Digital_savvy', 'HOE_Hybrid', 'HOE_SegD',
                                 '3P_Media_Traditionalist', '3P_Media_Digital_savvy', '3P_Media_Hybrid', '3P_Media_SegD']

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

                # Hide columns
                self._hide_columns(data)

                # Validate conditions
                if not self._validate_conditions_hcp(data):
                    self._send_response(400, {'error': 'Conditions not met'})
                    return

                # Get the next 'Id' value
                next_id = self._get_next_id(table_hcp)
                # Insert data into DynamoDB table
                data['Id'] = next_id
                # Insert data into DynamoDB table
                table_hcp.put_item(Item=data)

                self._send_response(201, {'message': 'Data inserted successfully'})
            except Exception as e:
                print(f"Error processing POST request: {e}")
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

                # Validate conditions
                if not self._validate_conditions_clc(data):
                    self._send_response(400, {'error': 'Conditions not met'})
                    return

                # Insert data into DynamoDB table
                table_clc.put_item(Item=data)

                self._send_response(201, {'message': 'Data inserted successfully'})
            except Exception as e:
                print(f"Error processing POST request: {e}")
                self._send_response(500, {'error': f'Internal server error: {e}'})

        else:
            self._send_response(404, {'error': 'Not Found'})


if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on http://localhost:8080')
    httpd.serve_forever()