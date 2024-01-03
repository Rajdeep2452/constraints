import datetime
import random
import csv
import io
from config import *
from boto3.dynamodb.types import Decimal


class Helper:
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

    # Create a dictionary to store counts for each channel
    channel_counts = {'phone': 0, 'email': 0, 'web': 0}

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

    def _get_next_id(table):
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


    def _count_dynamic_fields(post_data):
        # Count the number of 3pe_m* fields dynamically
        count = 0
        while f'3pe_m{count + 1}' in post_data or f'3pe_m{count + 1}_value' in post_data:
            count += 1
        return count


    # def filter_summary_detail_by_action(self, action_param):
    #     if action_param == "empty":
    #         return summary_detail_data
    #     else:
    #         # Convert 'calls' to 'Calls', 'emails' to 'Emails', etc.
    #         action_value = action_param[0].capitalize()
    #         # Filter rows based on the 'Action' column
    #         return [row for row in summary_detail_data if row.get('Action') == action_value]

    def convert_to_csv(data):
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

    def delete_all_rows_from_table(key, table):
        try:
            # Scan the table to get all items
            response = table.scan()
            items = response['Items']

            # Delete each item
            for item in items:
                npi_id = item[key]  # Assuming 'npi_id' is your primary key
                table.delete_item(Key={key: npi_id})
            
            print("All rows deleted successfully.")
        
        except Exception as e:
            print(f"Error deleting rows: {e}")

        # except table.meta.client.exceptions.DynamoDBError as e:
        #     print(f"Error checking NPI ID in DynamoDB: {e}")
        #     return False
        
    def put_data_in_table(filtered_npi, rule_row):
        summary_of_recommendation = ""
        primary_reason = ""
        secondary_reason = ""
        primary_reason_existing= ""
        table = None

        if rule_row['Default_Channel'].lower() in ['phone','call','calls']:
            table = calls_table
            summary_of_recommendation = "Consider reaching out to HCP to promote Brand for new patients expected"
        elif rule_row['Default_Channel'].lower() in ['email','emails']:
            table = email_table
            summary_of_recommendation = "Indicates continued interest and curiosity about the product. Possible opportunity to immediately send an RTE based on pages visited"
        elif rule_row['Default_Channel'].lower() in ['web','insight','insights'] :
            table = web_table
            summary_of_recommendation = "Indicates engagement with Brand promotional material. Consider following up with rep-triggered email"
        else:
            print(f"Unexpected Default_Channel value:{rule_row['Rule']} {rule_row['Default_Channel']}")

        for _, row in filtered_npi.iterrows():

            npi_id = row['npi_id']
            # Check if the npi_id exists in the DynamoDB table
            table_data = table.scan()
            items = table_data.get('Items', [])

            for item in items:
                if item.get('npi_id') == npi_id:
                    # Concatenate primary reason if the npi_id exists
                    primary_reason_existing = f"{item.get('Primary_Reason', '')}\n"
                    print(primary_reason_existing)
                    break

            row_name = row['Account_Name']
            name_parts = row_name.split(', ')
            full_name = ' '.join(reversed(name_parts))
            full_name = f"{full_name}"
            subject = row['Preferred_Content']
            date = datetime.datetime.now() - datetime.timedelta(int(row['rte_last_actvty']))
            random_value = random.randint(1, 14)

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
                primary_reason = f'''Please consider capturing HCP's consent in the next call
                1. {full_name} has not provided email consent or consent has expired
                2. HCP has a call planned in next {random_value} days
                3. <(cieling(current count of hcp*100/total hcp)s> HCPs in your territory have already provided consent'''
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_3rd_party_email":
                primary_reason =  f"Please consider having a discussion to reinforce the messages in the next call \n1. {full_name} has opened an Approved Email on {subject} on {date} \n2. HCP has a call planned in next 7 days"
                secondary_reason = ""
            elif rule_row['Rule'] == "low_call_plan_attainment":
                primary_reason = ""
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_home_office_email":
                primary_reason = f"Please consider having a discussion to reinforce the messages in the next call \n1. {full_name} has opened an Approved Email on {subject} on {date} \n2. HCP has a call planned in next 7 days"
                secondary_reason = ""
            elif rule_row['Rule'] == "high_value_website_visits_in_the_last_15_days":
                primary_reason = f"{full_name} visited brand website thrice in the past 15 days, spending most time on the {subject}"
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_rep_triggered_email":
                primary_reason = f'''Please consider having a discussion to reinforce the messages in the next call
                1. {full_name} has opened an Approved Email on {subject} on {date}
                2. HCP has a call planned in next {random_value} days'''
                secondary_reason = ""

            primary_reason = f"{primary_reason_existing}{primary_reason}"
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

        response_priority = priority_table.scan()
        priority_data = response_priority['Items']
        priority_df = pd.DataFrame(priority_data)

        # Sort priority_df in ascending order based on 'Priority_Order'
        priority_df = priority_df.sort_values(by='Priority_Order', ascending=False)
        for index, rule_row in priority_df.iterrows():
            # suggestions_df_2 = suggestions_df[suggestions_df['Segment'].isin(rule_row['Segment'])].copy()
            if rule_row['Segment'] is None:
                rule_row['Segment'] = []
            elif isinstance(rule_row['Segment'], str):
                rule_row['Segment'] = [rule_row['Segment']]

            # print(rule_row['Rule'])

            # Further filter rules based on segment present in suggestion_row list
            # print(suggestions_df['Segment'])
            suggestions_df_2 = suggestions_df[suggestions_df['Segment'].str.lower().isin(map(str.lower, rule_row['Segment']))].copy()
            # print(suggestions_df_2['Segment'])
            if rule_row['Rule'] == "new_patients_expected_in_the_next_3_months" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] <= suggestions_df_2['New_patients_in_next_quarter']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "decline_in_rx_share_in_the_last_one_month" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] <= suggestions_df_2['Decline_in_Rx_share_in_the_last_one_month']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "switch_to_competitor_drug" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['Switch_to_Competitor']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "new_patient_starts_in_a_particular_lot" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] <= suggestions_df_2['New_patients_in_particular_LOT']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "no_explicit_consent" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['No_Consent']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "clicked_3rd_party_email" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['Clicked_3rd_Party_Email']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "low_call_plan_attainment" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['New_patients_in_particular_LOT']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "clicked_home_office_email" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] <= suggestions_df_2['Clicked_Home_Office_Email']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "high_value_website_visits_in_the_last_15_days" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] == suggestions_df_2['High Value Website Visits']].copy()
                self.put_data_in_table(filtered_npi, rule_row)
            elif rule_row['Rule'] == "clicked_rep_triggered_email" and rule_row['Status'] == True:
                filtered_npi = suggestions_df_2[rule_row['Trigger_Value'] <= suggestions_df_2['Clicked_Rep_Email']].copy()
                self.put_data_in_table(filtered_npi, rule_row)

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
