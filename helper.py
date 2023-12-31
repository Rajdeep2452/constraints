import datetime
import random
import csv
import io
from config import *
from boto3.dynamodb.types import Decimal
from collections import Counter
from math import ceil


class Helper:

    num_rep = 0

    def __init__(self):
        print(suggestions_table_name)
        self.delete_all_rows_from_table('Id', table_hcp)
        self.delete_all_rows_from_table('Id', table_clc)
        try:
            # Insert default values into the DynamoDB table
            table_hcp.put_item(Item=self.default_values_hcp)
            table_clc.put_item(Item=self.default_values_clc)

        except Exception as e:
            print(f"Error initializing table: {e}")

    # Default values for each column
    default_values_hcp = {
        'Calls_Traditionalist': Decimal('4.56'), 'Calls_Digital_savvy': Decimal('0.49'), 'Calls_Hybrid': Decimal('2.28'), 'Calls_Status': True,
        'RTE_Traditionalist': Decimal('0.98'), 'RTE_Digital_savvy': Decimal('0.18'), 'RTE_Hybrid': Decimal('1.06'), 'RTE_Status': True,
        'HOE_Traditionalist': Decimal('3.86'), 'HOE_Digital_savvy': Decimal('5.65'), 'HOE_Hybrid': Decimal('7.12'), 'HOE_Status': True,
        '3P_Media_Traditionalist': Decimal('1.24'), '3P_Media_Digital_savvy': Decimal('5.01'), '3P_Media_Hybrid': Decimal('3.82'), '3P_Status': True,
        'Id': 1
    }


    default_values_clc = {
        'Calls': 184, 'RTE': 103, 'End_date': '2023-12-31', 'Start_date': '2023-01-01', '3P_Media': 14482, 'Status': 'Active', 'HOE': 2400, 'Id':1
    }

    default_values_pt = {
        'Status': False,
        'Priority_Order': 0,
        'Trigger_Value': None,
        'Trigger_Urgency': "Normal",
        'Only_For_Targets': False,
        'Default_Channel': "",
        'Segment': "Traditionalist"
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

    def delete_all_rows_from_table(self, key, table):
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
        table = None

        if rule_row['Default_Channel'].lower() in ['phone','call','calls']:
            table = calls_table
        elif rule_row['Default_Channel'].lower() in ['email','emails']:            table = email_table
        elif rule_row['Default_Channel'].lower() in ['web','insight','insights'] :
            table = web_table
        else:
            print(f"Unexpected Default_Channel value:{rule_row['Rule']} {rule_row['Default_Channel']}")

        for _, row in filtered_npi.iterrows():
            npi_id = row['npi_id']
            # Check if the npi_id exists in the DynamoDB table
            table_data = table.scan()
            items = table_data.get('Items', [])
            primary_reason_existing= ""

            for item in items:
                if item.get('npi_id') == str(npi_id):
                    # Concatenate primary reason if the npi_id exists
                    primary_reason_existing = f'''2) {item.get('Primary_Reason')}'''
                    break

            row_name = row['Account_Name']
            name_parts = row_name.split(', ')
            full_name = ' '.join(reversed(name_parts))
            full_name = f"{full_name}"
            subject = f"on {row['Preferred_Content']}" if pd.notna(row['Preferred_Content']) else ""
            date = datetime.datetime.now() - datetime.timedelta(int(row['rte_last_actvty']))
            random_value = random.randint(1, 14)

            if rule_row['Rule'] == "new_patients_expected_in_the_next_3_months":
                summary_of_recommendation = f"Consider reaching out to HCP to promote Brand for new patients expected"
                primary_reason = f"{full_name} is expected to have {row['New_patients_in_next_quarter']} new patients in the next {str(random.randint(1, 3))} months"
                secondary_reason = ""
            elif rule_row['Rule'] == "decline_in_rx_share_in_the_last_one_month":
                summary_of_recommendation = f"Prioritize HCP engagement and promote key product benefits to address potential market challenges"
                tg_val_int=int(row['Decline_in_Rx_share_in_the_last_one_month'])
                decline_num=random.randint(tg_val_int, 20)
                primary_reason = f"Please consider discussing the possible reasons for recent drop in sales that the HCP is affiliated with during the next call.Recent sales affiliated with {full_name}'s account have experienced a notable {str(decline_num)}% decline over the past month when compared to the preceding {str(random.randint(1, 6))} months."
                secondary_reason = ""
            elif rule_row['Rule'] == "switch_to_competitor_drug":
                summary_of_recommendation = f"Consider reaching out to HCPs to get feedback from the HCPs regarding the switch"
                primary_reason = f"{full_name}'s only eligible patient has moved away to an alternate therapy"
                secondary_reason = ""
            elif rule_row['Rule'] == "new_patient_starts_in_a_particular_lot":
                primary_reason = f"{full_name} is expected to have {row['New_patients_in_particular_LOT']} new patients in 2L LOT in the next 3 months"
                secondary_reason = ""
            elif rule_row['Rule'] == "no_explicit_consent":
                summary_of_recommendation = f"Consent is Expiring: Please consider sending any Approved Email which will automatically reset the consent for the HCP"
                primary_reason = f'''Please consider capturing HCP's consent in the next call, {full_name} has not provided email consent or consent has expired'''
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_3rd_party_email":
                summary_of_recommendation=f"Indicates engagement with Brand promotional material. Consider following up with 3rd party email"
                primary_reason =  f"Please consider having a discussion to reinforce the messages in the next call ,{full_name} has opened an Approved Email {subject} on {date}"
                secondary_reason = ""
            elif rule_row['Rule'] == "low_call_plan_attainment":
                primary_reason = ""
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_home_office_email":
                summary_of_recommendation=f"Indicates engagement with Brand promotional material. Consider following up with hoe-triggered email"
                primary_reason = f"Please consider having a discussion to reinforce the messages in the next call, {full_name} has opened an Approved Email {subject} on {date} "
                secondary_reason = ""
            elif rule_row['Rule'] == "high_value_website_visits_in_the_last_15_days":
                summary_of_recommendation=f"Indicates continued interest and curiosity about the product. Possible opportunity to immediately schedule a call or send an RTE based on pages visited"
                primary_reason = f"{full_name} visited brand website {str(random.randint(3, 10))} times in the past 15 days, spending most time {subject}"
                secondary_reason = ""
            elif rule_row['Rule'] == "clicked_rep_triggered_email":
                summary_of_recommendation=f"Indicates engagement with Brand promotional material. Consider following up with rep-triggered email"
                primary_reason = f'''Please consider having a discussion to reinforce the messages in the next call, {full_name} has opened an Approved Email {subject} on {date}'''
                secondary_reason = ""

            primary_reason = f'''{"1) " if primary_reason_existing != "" else ""}{primary_reason}
                             {primary_reason_existing}'''
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
        self.delete_all_rows_from_table(Helper, "npi_id", calls_table)
        self.delete_all_rows_from_table(Helper, "npi_id", email_table)
        self.delete_all_rows_from_table(Helper, "npi_id", web_table)

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
        helper.num_rep = max(item.get('rep_id', 0) for item in suggestions_data)
         
        recomm_cycle = 2
        # Format Recomm_Date in the desired format
        recomm_date = datetime.datetime.now().strftime('%dth %b (%a @ %I.%M %p)')

        # Get the number of rows in calls_table
        num_calls = calls_table.scan(Select='COUNT')['Count']

        # Get the number of rows in email_table
        num_email = email_table.scan(Select='COUNT')['Count']

        # Get the number of columns in web_table
        num_web = web_table.scan(Select='COUNT')['Count']
        num_rep = helper.num_rep


        # Check if data for the given id exists
        existing_data = table_summary.get_item(Key={'id': 1}).get('Item')
        largest_id_data_clc = self._get_last_added_data(self, table_clc)
        call_limit = self._convert_decimal_to_int(self, largest_id_data_clc['Calls'])
        avg_calls = min(Decimal(num_calls / (num_rep)),Decimal(call_limit/12))
        dynamo_response = calls_table.scan()
        rep_limit = ceil(Decimal(avg_calls))
        num_calls = 0

        # Apply rep limit for calls
        dynamo_data = dynamo_response.get('Items', [])
        rep_count = dict(Counter(item['REP'] for item in dynamo_data))

        # Create a new list with filtered items

        # filtered_dynamo_data = [item for item in dynamo_data if rep_count[item['REP']] <= rep_limit]
        # print(filtered_dynamo_data)

        # Count the number of rows in the filtered data
        num_calls = sum(rep_limit if count > rep_limit else count for count in rep_count.values())

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
                    ':ac': str(round(avg_calls, 2)),
                    ':art': str(round((num_email / num_rep), 2)),
                    ':ai': str(round((num_web / num_rep), 2))
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
                    'Avg_Calls': str(round(avg_calls, 2)),
                    'Avg_RTE': str(round((num_email / num_rep), 2)),
                    'Avg_Insights': str(round((num_web / num_rep), 2))
                }
            )
        print("Data computation complete")

helper = Helper()
