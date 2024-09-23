import json
import requests
import os
import boto3
from commons.log import slack_message
from datetime import date, datetime

def lambda_handler(event, context):

    try:
        # Get the Lambda function name
        lambda_name = context.function_name

        # Environment variables for the Slack webhook stored in Lambda
        SLACK_CHANNEL = os.environ['SLACK_CHANNEL'].strip()
        SLACK_USERNAME = os.environ['SLACK_USERNAME'].strip()
        
        # Secret manager variable
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name='us-east-1'
        )

        # Client authentication
        secret_name_client = 'azure_client_secret'
        response_client = client.get_secret_value(SecretId=secret_name_client)
        value_response_client = response_client['SecretString']
        value_json_client = json.loads(value_response_client)

        client_id = value_json_client['client_id'] 
        client_secret = value_json_client['client_secret']

        # Authentication parameters
        auth_url = "https://login.microsoftonline.com/your-tenant-id/oauth2/token"
        payload = f'client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials&resource=https://eastus.yourcloudresource.windows.net/'
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # Fetch token
        response = requests.request("POST", auth_url, data=payload, headers=headers)
        response_dict = json.loads(response.text)
        token = response_dict["access_token"]

        # Parameters for request
        refresh_url = "https://eastus.yourcloudresource.windows.net/servers/your-server/models/your-model/refreshes"

        # Fetch time variables stored in lambda
        start_time = os.environ['START_TIME'].strip()
        end_time = os.environ['END_TIME'].strip()
        time_start = datetime.strptime(start_time, "%H:%M")
        time_end = datetime.strptime(end_time, "%H:%M")

        # Condition to run all tables only within specified times
        if time_start.time() <= datetime.utcnow().time() <= time_end.time():
            payload = json.dumps({
                "Type": "Full",
                "CommitMode": "transactional",
                "MaxParallelism": 2,
                "RetryCount": 2,
                "Objects": []
            })

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + token
            }

            # Invoke refresh
            response = requests.request("POST", refresh_url, headers=headers, data=payload)

            print("Response returned: " + response.text)

        else:
            payload = json.dumps({
                "Type": "Full",
                "CommitMode": "transactional",
                "MaxParallelism": 2,
                "RetryCount": 2,
                "Objects": [
                    {"table": "subscription"},
                    {"table": "campaign"}
                ]
            })

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + token
            }

            # Invoke refresh
            response = requests.request("POST", refresh_url, headers=headers, data=payload)

            print("Response returned: " + response.text)

    except (Exception) as e:
        output_message = f'{e} - (Check this).\nLambda: {lambda_name}.'
        print(output_message)            
        slack_message.report_on_slack(message=output_message, username=SLACK_USERNAME, channel=SLACK_CHANNEL, msg_type="error")