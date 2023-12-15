from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
from boto3.dynamodb.types import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

table_suppression = dynamodb.Table(table_name_suppression)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

class RequestHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()


    def _count_dynamic_fields(self, post_data):
        # Count the number of 3pe_m* fields dynamically
        count = 0
        while f'3pe_m{count + 1}' in post_data or f'3pe_m{count + 1}_value' in post_data:
            count += 1
        return count

    def _validate_post_data_suppression(self, post_data):
        # Validate common fields
        common_fields_valid = (
            1 <= post_data.get('vs_last_visit_completed', 0) <= 90 and
            1 <= post_data.get('vs_next_visit_planned', 0) <= 90 and
            1 <= post_data.get('rtes_last_rte_sent', 0) <= 90 and
            1 <= post_data.get('hoes_last_hoe_sent', 0) <= 90
        )

        # Validate 3pe_m* fields
        dynamic_fields_valid = all(
            isinstance(post_data.get(f'3pe_m{i}_value', 0), int) and
            1 <= post_data.get(f'3pe_m{i}_value', 0) <= 90 and
            isinstance(post_data.get(f'3pe_m{i}', ''), str)
            for i in range(1,  self._count_dynamic_fields(post_data) + 1)
        )

        return common_fields_valid and dynamic_fields_valid 

    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self._send_cors_headers()
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message, cls=DecimalEncoder).encode())

    def do_GET(self):
        if self.path == '/Suppression':
            response = table_suppression.scan()
            items = response.get('Items', [])
            self._send_response(200, items)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

    def do_POST(self):
        if self.path == '/Suppression':
            content_length = int(self.headers['Content-Length'])
            post_data = json.loads(self.rfile.read(content_length).decode('utf-8'))

            # Validate data
            if not self._validate_post_data_suppression(post_data):
                self._send_response(400, {'message': 'Data Validation failed'})
                return

            # Check if there are zero rows
            response = table_suppression.scan()
            existing_data = response.get('Items', [])
            
            if existing_data:
                # Update data
                existing_item = table_suppression.get_item(Key={'id': 1}).get('Item')
                if existing_item:
                    # Merge the existing item with the new data
                    updated_item = {**{'id': 1}, **existing_item, **post_data}

                    # Update the item in the table
                    table_suppression.put_item(Item=updated_item)
                else:
                    # Handle the case where the item with the given primary key doesn't exist
                    # You may choose to insert a new item or handle it differently based on your requirements
                    self._send_response(404, {'message': 'Item not found'})
            else:
                # Insert data
                table_suppression.put_item(Item={**{'id': 1},**post_data})

            self._send_response(201, ['Data updated successfully', post_data])

        else:
            self._send_response(404, 'Not Found')

if __name__ == '__main__':
    server_address = ('localhost', 4200)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on localhost:4200')
    httpd.serve_forever()
