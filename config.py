import boto3
import pandas as pd

# Configure your AWS credentials and region
aws_access_key_id = 'AKIA2YW777YMWVKBBLQB'
aws_secret_access_key = 'HaIAuhnLxaABImmhZVU+GxEPqtzF5/xFN+fpRIOG'
aws_region = 'us-east-1'

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Specify your table name
table_name_hcp = 'HCP_Level_Constraint-Quarterly'
table_name_clc = 'Channel_Level_Constraints-Quarterly'
table_name_pt = 'Priority_and_Triggers'
table_name_suppression = 'Suppression'
table_name_summary = 'Summary'
suggestions_table_name = 'Suggestions_Table'
hcp_table_name = 'HCP_Table'
table_name_summary_detail = 'Summary_detail'
calls_table = dynamodb.Table('Calls_Table')
email_table = dynamodb.Table('Email_Table')
web_table = dynamodb.Table('Web_Table')


# Get reference to the table
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