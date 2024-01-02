from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
from validations import Validation
from helper import Helper
import json
import datetime
import urllib.parse
from boto3.dynamodb.types import Decimal


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
helper = Helper()
validation = Validation()

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

class RequestHandler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._set_cors_headers()
        self.end_headers()

    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(message, cls=DecimalEncoder).encode())
    
    def _set_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')

    def do_GET(self):
        if self.path == '/HCP':
            try:
                # Get the row with the largest ID for HCP table
                data = helper._get_last_added_data(table_hcp)
                # Return the data of the row with the largest ID for HCP table
                self._send_response(200, {'data': data})
                
            except Exception as e:
                print(f"Error processing GET request: {e}")
                self._send_response(500, {'error': 'Internal server error'})

        elif self.path == '/CLC':
            try:
                # Get the row with the largest ID for CLC table
                largest_id_data_clc = helper._get_last_added_data(table_clc)
                data = helper._convert_decimal_to_int(largest_id_data_clc)
                # Return the data of the row with the largest ID for HCP table
                self._send_response(200, {'data': data})
            except Exception as e:
                print(f"Error processing GET request: {e}")
                self._send_response(500, {'error': f'Internal server error: {e}'})

        elif self.path == '/PT':
            response_data = {}
            for rule in Validation.valid_rules:
                item = table_pt.get_item(Key={'Rule': rule}).get('Item', helper.default_values_pt)
                response_data[rule] = item
            self.send_response(200)
            self._set_cors_headers()
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, cls=DecimalEncoder).encode())

        elif self.path == '/Suppression':
            response = table_suppression.scan()
            items = response.get('Items', [])
            self._send_response(200, items)

        elif self.path == '/Summary':
            Helper.show_details(Helper)
            Helper.compute_summary(Helper)
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

        elif self.path.startswith('/SummaryDetail'):
            try:
                # Extract parameters from the URL
                parsed_url = urllib.parse.urlparse(self.path)
                query_params = urllib.parse.parse_qs(parsed_url.query)

                # Get the value of the 'action' parameter
                action_param = query_params.get('action', "empty")

                largest_id_data_clc = helper._get_last_added_data(table_clc)
                call_limit = helper._convert_decimal_to_int(largest_id_data_clc['Calls'])


                # Filter rows based on the 'action' parameter
                if 'calls' in action_param:
                    table = calls_table
                elif 'emails' in action_param:
                    table = email_table
                elif 'insights' in action_param:
                    table = web_table
                else:
                    raise ValueError("Invalid action parameter")

                if table == calls_table:
                    dynamo_response = table.scan(Limit=call_limit)
                else:
                    dynamo_response = table.scan()
                dynamo_data = dynamo_response.get('Items', [])
                num_hcp = len(suggestions_data)
                num_rep = max([hcp['rep_id'] for hcp in hcp_data])
                recomm_cycle = 2
                recomm_date = datetime.datetime.now().strftime('%dth %b (%a @ %I.%M %p)')

                # Create the response structure
                response_data = {
                    "Num_HCP": num_hcp,
                    "Num_Rep": num_rep,
                    "NBA_Recomm_Cycle": recomm_cycle,
                    "Recomm_date": recomm_date,
                    "details": dynamo_data
                }

                # Convert response data to JSON
                response_json = json.dumps([response_data], cls=DecimalEncoder)

                # Send a response back to the client
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self._set_cors_headers()
                self.end_headers()
                self.wfile.write(response_json.encode())

            except Exception as e:
                # Print the error details
                print(f"Error processing GET request: {str(e)}")

                # Send an error response back to the client
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self._set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))

        elif self.path.startswith('/DownloadCSV'):
            try:
                # Get the value of the 'action' parameter for CSV download
                parsed_url = urllib.parse.urlparse(self.path)
                query_params = urllib.parse.parse_qs(parsed_url.query)

                # Get the value of the 'action' parameter
                action_param = query_params.get('action', "empty")

                if 'calls' in action_param:
                    table = calls_table
                elif 'emails' in action_param:
                    table = email_table
                elif 'insights' in action_param:
                    table = web_table
                else:
                    raise ValueError("Invalid action parameter")
                    
                # Scan the DynamoDB table to get all items
                response = table.scan()

            # Check if 'Items' key exists and the list is not empty
                if 'Items' in response and response['Items']:
                    items = response['Items']

                    # Convert data to a CSV string
                    csv_data = Helper.convert_to_csv(items)

                    # Send the CSV as a response
                    self.send_response(200)
                    self.send_header('Content-type', 'text/csv')
                    self._set_cors_headers()
                    self.end_headers()
                    self.wfile.write(csv_data.encode())
                else:
                    # Send a response indicating no data
                    self.send_response(404)
                    self.send_header('Content-type', 'application/json')
                    self._set_cors_headers()
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': 'No data available'}).encode('utf-8'))

            except Exception as e:
                # Handle exceptions for CSV download
                print(f"Error processing Download: {str(e)}")

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
                        try:
                            # Round the float value to 2 decimal places
                            rounded_value = round(Decimal(value), 2)
                            data[key] = rounded_value
                        except ValueError:
                            self._send_response(400, {'error': f'Invalid value for column {key}. Must be a float.'})
                            return

                # Validate conditions
                if not Validation._validate_conditions_hcp(data):
                    print(data)
                    self._send_response(400, {'error': 'Conditions not met'})
                    return

                # Insert data into DynamoDB table using batch write
                Helper._insert_items(Helper, table_hcp, [data])
                self._send_response(201, ['Data inserted successfully', data])

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
                Helper._insert_items(Helper, table_clc, [data])
                self._send_response(201, ['Data inserted successfully', data])

            except Exception as e:
                print(f"Error processing POST request for CLC: {e}")
                self._send_response(500, {'error': f'Internal server error: {e}'})


        elif self.path == '/PT':
            content_length = int(self.headers['Content-Length'])
            post_data = json.loads(self.rfile.read(content_length))

            if 'rules' in post_data:
                rules_data = post_data['rules']
                inserted_data = {}

                for rule, rule_data in rules_data.items():
                    if rule in Validation.valid_rules:
                        # Validate data
                        is_valid_data, error_message = Validation.validate_data_pt(Validation, rule, rule_data)
                        if not is_valid_data:
                            self._send_response(400, f'Bad Request: {error_message}')
                            Helper.existing_priority_orders = set()
                            return

                        # Update or insert into DynamoDB
                        # Use 'Rule' as the key for put_item
                        table_pt.put_item(Item={**{'Rule': rule}, **helper.default_values_pt, **rule_data})

                        # Fetch the inserted data using the correct key
                        item = table_pt.get_item(Key={'Rule': rule}).get('Item', helper.default_values_pt)
                        inserted_data[rule] = item
                    else:
                        self._send_response(400, f'Bad Request: Invalid rule - {rule}')
                        Helper.existing_priority_orders = set()
                        return

                self._send_response(200, {'message': 'Data updated successfully', 'data': inserted_data})
            else:
                self._send_response(400, 'Bad Request: Missing "rules" key in the payload')

        elif self.path == '/Suppression':
            content_length = int(self.headers['Content-Length'])
            post_data = json.loads(self.rfile.read(content_length).decode('utf-8'))

            # Validate data
            if not Validation._validate_post_data_suppression(post_data):
                self._send_response(400, {'message': 'Data Validation failed'})
                return

            # Check if there are zero rows
            response = table_suppression.scan()
            existing_data = response.get('Items', [])
            
            if existing_data:
                Helper.delete_all_rows_from_table("id", response)

            table_suppression.put_item(Item={**{'id': 1},**post_data})

            self._send_response(201, ['Data updated successfully', post_data])

        elif self.path == '/Summary':
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

        else:
            self._send_response(404, {'error': 'Not Found'})
        Helper.existing_priority_orders = set()


if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on http://localhost:8080')
    httpd.serve_forever()
