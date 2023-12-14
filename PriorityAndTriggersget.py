from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
from boto3.dynamodb.types import Decimal


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)



# Get a reference to your table
table_pt = dynamodb.Table(table_name_pt)

# Default values for each column
default_values_pt = {
    'Rule': "", 'Status': True, 'Priority_Order': 1, 'Trigger_Value': True,
    'Trigger_Urgency': "Normal", 'RTE_Digital_savvy': 1, 'Only_For_Targets': True, 'Default_Channel': "",
    'Segment': ""
}

class RequestHandler(BaseHTTPRequestHandler):

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')
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

    def _convert_decimal_to_int(self, data):
        if isinstance(data, Decimal):
            return int(data)
        elif isinstance(data, list):
            return [self._convert_decimal_to_int(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._convert_decimal_to_int(value) for key, value in data.items()}
        else:
            return data

    def do_GET(self):
        if self.path == '/PT':
            try:
                response = table_pt.scan()
                items = response.get('Items', [])
                result = []
                for item in items:
                    values = {}
                    for col in default_values_pt.keys():
                        col_value = item.get(col, default_values_pt[col])
                        col_value = str(col_value) if isinstance(col_value, Decimal) else col_value
                        values[col] = col_value
                    result.append(values)

                self._send_response(200, {"data": result})
            except Exception as e:
                print(f"Error processing GET request: {e}")
                self._send_response(500, {'error': 'Internal server error'})
        else:
            self._send_response(404, {'error': 'Not Found'})




if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on http://localhost:8080')
    httpd.serve_forever()
