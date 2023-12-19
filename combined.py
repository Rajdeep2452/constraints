from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
from boto3.dynamodb.types import Decimal


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


# Get a reference to your table
table_hcp = dynamodb.Table(table_name_hcp)
table_clc = dynamodb.Table(table_name_clc)
table_pt = dynamodb.Table(table_name_pt)
table_suppression = dynamodb.Table(table_name_suppression)
table_summary = dynamodb.Table(table_name_summary)

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

default_values_pt = {
    'Status': False,
    'Priority_Order': 1,
    'Trigger_Value': None,
    'Trigger_Urgency': "Normal",
    'Only_For_Targets': False,
    'Default_Channel': "",
    'Segment': ""
}

# Global variable to track existing priority orders
existing_priority_orders = set()

# Validate rule names
valid_rules = set([
    "new_patients_expected_in_the_next_3_months",
    "new_patient_starts_in_a_particular_lot",
    "decline_in_rx_share_in_the_last_one_month",
    "switch_to_competitor_drug",
    "high_value_website_visits_in_the_last_15_days",
    "clicked_rep_triggered_email",
    "clicked_home_office_email",
    "clicked_3rd_party_email",
    "low_call_plan_attainment",
    "no_explicit_consent"
])

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

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
        self.wfile.write(json.dumps(message, cls=DecimalEncoder).encode())

    
    def _set_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')

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

    def validate_priority_order(self, priority_order, existing_priority_orders):
        if not isinstance(priority_order, int):
            return False, 'Priority_Order should be an integer.'

        if priority_order < 1 or priority_order > len(valid_rules):
            return False, f'Priority_Order should be between 1 and {len(valid_rules)}.'

        if priority_order in existing_priority_orders:
            return False, f'Each rule should have a unique Priority_Order. Duplicate - {priority_order}'

        
        return True, None

    def validate_data_pt(self, rule, data):
        priority_order = data.get('Priority_Order', 1)  # Default value is 1 if not provided
        status = data.get('Status', False)  # Default value is False if not provided
        trigger_value = data.get('Trigger_Value', None)
        trigger_urgency = data.get('Trigger_Urgency', "Normal")  # Default value is "Normal" if not provided
        default_channel = data.get('Default_Channel', None)
        only_for_targets = data.get('Only_For_Targets', False)  # Default value is False if not provided

        # Validate priority order
        is_valid_priority_order, error_message = self.validate_priority_order(priority_order, existing_priority_orders)
        if not is_valid_priority_order:
            return False, error_message

        # Update the existing priority orders
        existing_priority_orders.add(priority_order)

        # Validate status
        if not isinstance(status, bool):
            return False, 'Status should be a boolean.'

        # Validate trigger value based on rules
        if rule in ["new_patients_expected_in_the_next_3_months",
                    "new_patient_starts_in_a_particular_lot",
                    "decline_in_rx_share_in_the_last_one_month",
                    "low_call_plan_attainment"]:
            if type(trigger_value) is not int:
                return False, f'Invalid Trigger_Value for rule {rule}: Should be an integer.'
        else:
            if type(trigger_value) is not bool:
                return False, f'Invalid Trigger_Value for rule {rule}: Should be a boolean.'

        # Validate trigger urgency
        if not isinstance(trigger_urgency, str):
            return False, 'Trigger_Urgency should be a string.'

        # Validate default channel
        if not isinstance(default_channel, str):
            return False, 'Default_Channel should be a string.'

        # Validate only for targets
        if not isinstance(only_for_targets, bool):
            return False, 'Only_For_Targets should be a boolean.'

        return True, None

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

        elif self.path == '/PT':
            response_data = {}
            for rule in valid_rules:
                item = table_pt.get_item(Key={'Rule': rule}).get('Item', default_values_pt)
                response_data[rule] = item
            self.send_response(200,{'data':response_data})
            self._set_cors_headers()
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, cls=DecimalEncoder).encode())

        elif self.path == '/Suppression':
            response = table_suppression.scan()
            items = response.get('Items', [])
            self._send_response(200, items)

        elif self.path == '/Summary':
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

        else:
            self._send_response(404, {'error': 'Not Found'})


    def do_POST(self):
        global existing_priority_orders 
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
                self._insert_items(table_clc, [data])
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
                    if rule in valid_rules:
                        # Validate data
                        is_valid_data, error_message = self.validate_data_pt(rule, rule_data)
                        if not is_valid_data:
                            self._send_response(400, f'Bad Request: {error_message}')
                            existing_priority_orders = set()
                            return

                        # Update or insert into DynamoDB
                        # Use 'Rule' as the key for put_item
                        table_pt.put_item(Item={**{'Rule': rule}, **default_values_pt, **rule_data})

                        # Fetch the inserted data using the correct key
                        item = table_pt.get_item(Key={'Rule': rule}).get('Item', default_values_pt)
                        inserted_data[rule] = item
                    else:
                        self._send_response(400, f'Bad Request: Invalid rule - {rule}')
                        existing_priority_orders = set()
                        return

                self._send_response(200, {'message': 'Data updated successfully', 'data': inserted_data})
            else:
                self._send_response(400, 'Bad Request: Missing "rules" key in the payload')

        elif self.path == '/Suppression':
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
        existing_priority_orders = set()


if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on http://localhost:8080')
    httpd.serve_forever()
