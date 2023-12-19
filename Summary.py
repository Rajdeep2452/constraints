from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
from boto3.dynamodb.types import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Get a reference to your table
table_summary = dynamodb.Table(table_name_summary)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)  # Convert Decimal to string for JSON serialization
        return super(DecimalEncoder, self).default(o)

class RequestHandler(BaseHTTPRequestHandler):
 
    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()
       
    def _set_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')
 
    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message, cls=DecimalEncoder).encode())

    def do_GET(self):
        if self.path == '/Summary':
            try:
                # Retrieve data from the DynamoDB table
                response = table_summary.get_item(Key={'id': 1})

                # Check if data exists
                if 'Item' in response:
                    data = response['Item']

                    # Send a response back to the client
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self._set_cors_headers()
                    self.end_headers()
                    self.wfile.write(json.dumps(data, cls=DecimalEncoder).encode())
                else:
                    # Send a response back to the client if data doesn't exist
                    self.send_response(404)
                    self.send_header('Content-type', 'application/json')
                    self._set_cors_headers()
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': 'Data not found'}).encode('utf-8'))

            except Exception as e:
                # Send an error response back to the client
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self._set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))

    def do_POST(self):
        if self.path == '/Summary':
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length)
            data = json.loads(body.decode('utf-8'))

            try:
                # Extract values from the request
                num_rep = data['Num_Rep']
                calls_recomm = data['Calls_Recomm']
                rte_recomm = data['RTE_Recomm']
                insights = data['Insights']

                # Check if data for the given id exists
                existing_data = table_summary.get_item(Key={'id': 1}).get('Item')

                if existing_data:
                    # Update existing data
                    response = table_summary.update_item(
                        Key={'id': 1},
                        UpdateExpression='SET Num_HCP = :nh, Num_Rep = :nr, Recomm_Cycle = :rc, Recomm_Date = :rd, '
                                        'Calls_Recomm = :cr, RTE_Recomm = :rte, Insights = :ins, '
                                        'Avg_Calls = :ac, Avg_RTE = :art, Avg_Insights = :ai',
                        ExpressionAttributeValues={
                            ':nh': data['Num_HCP'],
                            ':nr': num_rep,
                            ':rc': data['Recomm_Cycle'],
                            ':rd': data['Recomm_Date'],
                            ':cr': calls_recomm,
                            ':rte': rte_recomm,
                            ':ins': insights,
                            ':ac': str(calls_recomm / num_rep),
                            ':art': str(rte_recomm / num_rep),
                            ':ai': str(insights / num_rep)
                        },
                        ReturnValues='ALL_NEW'
                    )
                else:
                    # Insert new data
                    response = table_summary.put_item(
                        Item={
                            'id': 1,
                            'Num_HCP': data['Num_HCP'],
                            'Num_Rep': num_rep,
                            'Recomm_Cycle': data['Recomm_Cycle'],
                            'Recomm_Date': data['Recomm_Date'],
                            'Calls_Recomm': calls_recomm,
                            'RTE_Recomm': rte_recomm,
                            'Insights': insights,
                            'Avg_Calls': str(calls_recomm / num_rep),
                            'Avg_RTE': str(rte_recomm / num_rep),
                            'Avg_Insights': str(insights / num_rep)
                        }
                    )

                # Send a response back to the client
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self._set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps(["Data Inserted Successfully", data], cls=DecimalEncoder).encode())

            except Exception as e:
                # Send an error response back to the client
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self._set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))

if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print(f'Starting server on {server_address[0]}:{server_address[1]}...')
    httpd.serve_forever()
