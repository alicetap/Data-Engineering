import os
import boto3
import json
import psycopg2 as pg
from commons.aws.redshift import  Redshift
from commons.aws.secret_manager import get_redshift_credentials
from commons.log import slack_message

def handler(event, context):

    # Get the Lambda function name
    lambda_name = context.function_name

    # Fetch environment variables stored in Lambda
    iam_role = os.environ['IAM_ROLE'].strip()
    table_name = os.environ['TABLE_NAME'].strip()
    s3_path = os.environ['S3_PATH'].strip()

    slack_channel = os.environ['SLACK_CHANNEL'].strip()
    slack_username = os.environ['SLACK_USERNAME'].strip()

    # Fetch Redshift credentials from the Secret manager
    red_credential = get_redshift_credentials(os.environ['REDSHIFT_SECRET_NAME'].strip(), os.environ['REGION'].strip())
    redshiftingestion = Redshift(red_credential)

    # Start the process in Redshift
    print("Starting process in Redshift")  

    try:
        redshiftingestion.connect_redshift()

        # Unload data
        query = f"""  UNLOAD (' SELECT * FROM {table_name} ') 	
                       TO '{s3_path}' 	
                       IAM_ROLE '{iam_role}' 
                       PARALLEL OFF 
                       FORMAT AS CSV 
                       HEADER ALLOWOVERWRITE;
                 """
        redshiftingestion.execute_query(query)
        print(f'UNLOAD of table {table_name} successfully executed in Redshift.')   

        try:

            # Generate Slack message
            message = f'Lambda {lambda_name} completed successfully! UNLOAD of table {table_name} executed successfully.'      
            slack_message.report_on_slack(message=message, channel=slack_channel, username=slack_username, msg_type="success")
            print("The webhook.send call was successfully executed.")

        except Exception as error:
            print(f"An error occurred while sending the message via webhook: {error}")  

    except (Exception) as e:
        message = f'{e}.\nLambda: {lambda_name}.' 
        print(message)            
        slack_message.report_on_slack(message=message, channel=slack_channel, username=slack_username, msg_type="error")

    finally:
        # Close Redshift connection
        redshiftingestion.close_connection()