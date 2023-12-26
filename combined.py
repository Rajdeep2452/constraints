from http.server import BaseHTTPRequestHandler, HTTPServer
from config import *
import json
import boto3
import datetime
import pandas as pd
import urllib.parse
import csv
import io
from boto3.dynamodb.types import Decimal


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


# Get a reference to your table
table_hcp = dynamodb.Table(table_name_hcp)
table_clc = dynamodb.Table(table_name_clc)
table_pt = dynamodb.Table(table_name_pt)
table_suppression = dynamodb.Table(table_name_suppression)
table_summary = dynamodb.Table(table_name_summary)
table_summary = dynamodb.Table(table_name_summary)
suggestions_table = dynamodb.Table(suggestions_table_name)
response_suggestions = suggestions_table.scan()
suggestions_data = response_suggestions['Items']
suggestions_df = pd.DataFrame(suggestions_data)
priority_table = dynamodb.Table(table_name_pt)
response_priority = priority_table.scan()
priority_data = response_priority['Items']
priority_df = pd.DataFrame(priority_data)
hcp_table = dynamodb.Table(hcp_table_name)
response_hcp = hcp_table.scan()
hcp_data = response_hcp['Items']
response_summary = table_summary.scan()
summary_data = response_summary['Items']
summary_detail_table = dynamodb.Table(table_name_summary_detail)
response_summary_detail = summary_detail_table.scan()
summary_detail_data = response_summary_detail['Items']
summary_detail_df = pd.DataFrame(summary_detail_data)

calls_table = dynamodb.Table('Calls_Table')
email_table = dynamodb.Table('Email_Table')
web_table = dynamodb.Table('Web_Table')

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
    'Priority_Order': 0,
    'Trigger_Value': None,
    'Trigger_Urgency': "Normal",
    'Only_For_Targets': False,
    'Default_Channel': "",
    'Segment': "Traditionalist",
    'Recomm_date': datetime.datetime.now().strftime('%dth %b (%a @ %I.%M %p)')
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

rule_cols = {
    "new_patients_expected_in_the_next_3_months": "New_patients_in_next_quarter",
    "no_explicit_consent": "No_Consent",
    "clicked_3rd_party_email": "Clicked_3rd_Party_Email",
    "low_call_plan_attainment": "Best_time",
    "clicked_home_office_email": "Clicked_Home_Office_Email",
    "switch_to_competitor_drug": "Switch_to_Competitor",
    "new_patient_starts_in_a_particular_lot": "New_patients_in_particular_LOT",
    "high_value_website_visits_in_the_last_15_days": "High Value Website Visits",
    "decline_in_rx_share_in_the_last_one_month": "Decline_in_Rx_share_in_the_last_one_month",
    "clicked_rep_triggered_email": "Clicked_Rep_Email"
}

# Create a dictionary to store counts for each channel
channel_counts = {'phone': 0, 'email': 0, 'web': 0}

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

class RequestHandler(BaseHTTPRequestHandler):

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

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')
    
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

        if priority_order != 0 and priority_order in existing_priority_orders:
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

    # def filter_summary_detail_by_action(self, action_param):
    #     if action_param == "empty":
    #         return summary_detail_data
    #     else:
    #         # Convert 'calls' to 'Calls', 'emails' to 'Emails', etc.
    #         action_value = action_param[0].capitalize()
    #         # Filter rows based on the 'Action' column
    #         return [row for row in summary_detail_data if row.get('Action') == action_value]

    def convert_to_csv(self, data):
        # Convert data to CSV string
        csv_output = io.StringIO()
        csv_writer = csv.writer(csv_output)

        # Write header excluding the 'Action' column
        headers = [key for key in data[0].keys() if key != 'Action']
        csv_writer.writerow(headers)

        # Write data excluding the 'Action' column
        for row in data:
            csv_writer.writerow([value for key, value in row.items() if key != 'Action'])

        return csv_output.getvalue()

    def delete_all_rows_from_table(self, key, table):
        try:
            # Scan the table to get all items
            response = table.scan()
            items = response['Items']

            # Delete each item
            for item in items:
                npi_id = item['npi_id']  # Assuming 'npi_id' is your primary key
                table.delete_item(Key={key: npi_id})
            
            print("All rows deleted successfully.")
        
        except Exception as e:
            print(f"Error deleting rows: {e}")
        
    def put_data_in_table(self, filtered_npi, rule_row):
        summary_of_recommendation = ""
        primary_reason = ""
        secondary_reason = ""
        table = None

        if rule_row['Default_Channel'] == 'Phone':
            table = calls_table
            summary_of_recommendation = "Consider reaching out to HCP to promote Brand for new patients expected"
        elif rule_row['Default_Channel'] == 'Email':
            table = email_table
            summary_of_recommendation = "Indicates continued interest and curiosity about the product. Possible opportunity to immediately send an RTE based on pages visited"
        elif rule_row['Default_Channel'] == 'Web':
            table = web_table
            summary_of_recommendation = "Indicates engagement with Brand promotional material. Consider following up with rep-triggered email"


        for _, row in filtered_npi.iterrows():

            row_name = row['Account_Name']
            name_parts = row_name.split(', ')
            full_name = ' '.join(reversed(name_parts))
            full_name = f"Dr. {full_name}"

            if rule_row['Rule'] == "new_patients_expected_in_the_next_3_months":
                primary_reason = f"{full_name} is expected to have {rule_row['Trigger_Value']} new patients in the next 3 months"
                secondary_reason = ""
            elif rule_row['Rule'] == "decline_in_rx_share_in_the_last_one_month":
                primary_reason = f"Recent sales of the {full_name} is affiliated with, has decreased significantly by {rule_row['Trigger_Value']}% in the recent 1 month compared to previous 3 months*"
                secondary_reason = ""
            elif rule_row['Rule'] == "switch_to_competitor_drug":
                primary_reason = f"{full_name}'s only eligible patient has moved away to an alternate therapy"
                secondary_reason = ""
            elif rule_row['Rule'] == "new_patient_starts_in_a_particular_lot":
                primary_reason = f"{full_name} is expected to have {rule_row['Trigger_Value']} new patients in 2L LOT in the next 3 months"
                secondary_reason = ""
            elif rule_row['Rule'] == "no_explicit_consent":
                primary_reason = f"Please consider capturing HCP's consent in the next call \n1. {full_name} has not provided email consent or consent has expired \n2. HCP has a call planned in next <3> days \n3. {rule_row['Trigger_Value']}% HCPs in your territory have already provided consent"
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_3rd_party_email":
                primary_reason =  f"Please consider having a discussion to reinforce the messages in the next call \n1. {full_name} has opened an Approved Email on <Subject> on <date> \n2. HCP has a call planned in next 7 days"
                secondary_reason = ""
            elif rule_row['Rule'] == "low_call_plan_attainment":
                primary_reason = ""
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_home_office_email":
                primary_reason = f"Please consider having a discussion to reinforce the messages in the next call \n1. {full_name} has opened an Approved Email on <Subject> on <date> \n2. HCP has a call planned in next 7 days"
                secondary_reason = ""
            elif rule_row['Rule'] == "high_value_website_visits_in_the_last_15_days":
                primary_reason = f"{full_name} visited brand website thrice in the past 15 days, spending most time on the Efficacy page"
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_rep_triggered_email":
                primary_reason = f"Please consider having a discussion to reinforce the messages in the next call \n1. {full_name} has opened an Approved Email on <Subject> on <date> \n2. HCP has a call planned in next 7 days"
                secondary_reason = ""

            item = {
                'npi_id': str(row['npi_id']),
                'Region': row['region'],
                'Territory': row['territory'],
                'REP': row['rep_name'],
                'HCP_Name': full_name,
                'Priority_Rank': rule_row['Priority_Order'],
                'Summary_of_recommendation': summary_of_recommendation,
                'Primary_Reason': primary_reason, 
                'Secondary_Reason': secondary_reason
            }
            table.put_item(Item=item)

    def show_details(self):
        self.delete_all_rows_from_table("npi_id", calls_table)
        self.delete_all_rows_from_table("npi_id", email_table)
        self.delete_all_rows_from_table("npi_id", web_table)
        for index, rule_row in priority_df.iterrows():
            # suggestions_df_2 = suggestions_df[suggestions_df['Segment'].isin(rule_row['Segment'])].copy()
            if rule_row['Segment'] is None:
                rule_row['Segment'] = []
            elif isinstance(rule_row['Segment'], str):
                rule_row['Segment'] = [rule_row['Segment']]

            # Further filter rules based on segment present in suggestion_row list
            # print(suggestions_df['Segment'])
            suggestions_df_2 = suggestions_df[suggestions_df['Segment'].isin(rule_row['Segment'])].copy()
            if rule_row['Rule'] == "new_patients_expected_in_the_next_3_months" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] > suggestions_df_2['New_patients_in_next_quarter']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "decline_in_rx_share_in_the_last_one_month" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] > suggestions_df_2['Decline_in_Rx_share_in_the_last_one_month']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "switch_to_competitor_drug" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['Switch_to_Competitor']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "new_patient_starts_in_a_particular_lot" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] > suggestions_df_2['New_patients_in_particular_LOT']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "no_explicit_consent" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['No_Consent']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "clicked_3rd_party_email" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['Clicked_3rd_Party_Email']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "low_call_plan_attainment" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['New_patients_in_particular_LOT']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "clicked_home_office_email" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] > suggestions_df_2['Clicked_Home_Office_Email']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "high_value_website_visits_in_the_last_15_days" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['High Value Website Visits']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue
            elif rule_row['Rule'] == "clicked_rep_triggered_email" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] > suggestions_df_2['Clicked_Rep_Email']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
                continue

    def compute_summary(self):
        num_hcp = len(suggestions_data)
        num_rep = max([hcp['rep_id'] for hcp in hcp_data])
        recomm_cycle = 2
        # Format Recomm_Date in the desired format
        recomm_date = priority_df['Recomm_date'].max()

        # Get the number of rows in calls_table
        num_calls = calls_table.scan(Select='COUNT')['Count']

        # Get the number of rows in email_table
        num_email = email_table.scan(Select='COUNT')['Count']

        # Get the number of columns in web_table
        num_web = web_table.scan(Select='COUNT')['Count']


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
                    ':nh': num_hcp,
                    ':nr': num_rep,
                    ':rc': recomm_cycle,
                    ':rd': recomm_date,
                    ':cr': num_calls,
                    ':rte': num_email,
                    ':ins': num_web,
                    ':ac': str(num_calls / num_rep),
                    ':art': str(num_email / num_rep),
                    ':ai': str(num_web / num_rep)
                },
                ReturnValues='ALL_NEW'
            )
        else:
            # Insert new data
            response = table_summary.put_item(
                Item={
                    'id': 1,
                    'Num_HCP': num_hcp,
                    'Num_Rep': num_rep,
                    'Recomm_Cycle': recomm_cycle,
                    'Recomm_Date': recomm_date,
                    'Calls_Recomm': num_calls,
                    'RTE_Recomm': num_email,
                    'Insights': num_web,
                    'Avg_Calls': str(num_calls / num_rep),
                    'Avg_RTE': str(num_email / num_rep),
                    'Avg_Insights': str(num_web / num_rep)
                }
            )
        print("Data computation complete")

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
            self.show_details()
            self.compute_summary()
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


                # Filter rows based on the 'action' parameter
                if 'calls' in action_param:
                    table = calls_table
                elif 'emails' in action_param:
                    table = email_table
                elif 'insights' in action_param:
                    table = web_table
                else:
                    raise ValueError("Invalid action parameter")

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
                    csv_data = self.convert_to_csv(items)

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
                self.delete_all_rows_from_table("id", response)

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
