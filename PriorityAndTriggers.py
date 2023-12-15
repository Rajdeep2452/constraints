from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
from boto3.dynamodb.types import Decimal
 
 
# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
 
# Get a reference to your table
table_pt = dynamodb.Table(table_name_pt)
 
# Default values
default_values = {
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
 
    def validate_priority_order(self, priority_order, existing_priority_orders):
        if not isinstance(priority_order, int):
            return False, 'Priority_Order should be an integer.'
 
        if priority_order < 1 or priority_order > len(valid_rules):
            return False, f'Priority_Order should be between 1 and {len(valid_rules)}.'
 
        if priority_order in existing_priority_orders:
            return False, f'Each rule should have a unique Priority_Order. Duplicate - {priority_order}'
 
       
        return True, None
 
    def validate_data(self, rule, data):
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
 
    def do_GET(self):
        if self.path == '/PT':
            response_data = {}
            for rule in valid_rules:
                item = table_pt.get_item(Key={'Rule': rule}).get('Item', default_values)
                response_data[rule] = item
            self.send_response(200,{'data':response_data})
            self._set_cors_headers()
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data, cls=DecimalEncoder).encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')
 
    def do_POST(self):
        global existing_priority_orders
        if self.path == '/PT':
            content_length = int(self.headers['Content-Length'])
            post_data = json.loads(self.rfile.read(content_length))
 
            if 'rules' in post_data:
                rules_data = post_data['rules']
                inserted_data = {}
 
                for rule, rule_data in rules_data.items():
                    if rule in valid_rules:
                        # Validate data
                        is_valid_data, error_message = self.validate_data(rule, rule_data)
                        if not is_valid_data:
                            self._send_response(400, f'Bad Request: {error_message}')
                            existing_priority_orders = set()
                            return
 
                        # Update or insert into DynamoDB
                        # Use 'Rule' as the key for put_item
                        table_pt.put_item(Item={**{'Rule': rule}, **default_values, **rule_data})
 
                        # Fetch the inserted data using the correct key
                        item = table_pt.get_item(Key={'Rule': rule}).get('Item', default_values)
                        inserted_data[rule] = item
                    else:
                        self._send_response(400, f'Bad Request: Invalid rule - {rule}')
                        existing_priority_orders = set()
                        return
 
                self._send_response(200, {'message': 'Data updated successfully', 'data': inserted_data})
            else:
                self._send_response(400, 'Bad Request: Missing "rules" key in the payload')
        else:
            self._send_response(404, 'Not Found')
        existing_priority_orders = set()
 
 
if __name__ == '__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Server running on http://localhost:8080')
    httpd.serve_forever()
