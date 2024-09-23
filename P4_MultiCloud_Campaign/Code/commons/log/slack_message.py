import requests
import json
import boto3
from botocore.exceptions import ClientError

def get_slack_webhook(secret_key: str) -> str:
    print("Getting Webhook access key secret.")
    client = None
    try:
        client = boto3.client(service_name='secretsmanager', region_name='us-east-1')
        get_secret_value_response = client.get_secret_value(SecretId='company-alerts-webhook-urls')
        secret = json.loads(get_secret_value_response['SecretString'].strip())
        webhook_url = secret[secret_key]
        return webhook_url
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    finally:
        if client:
            client.close()

def post_slack_message(message, webhook_url, channel, username):
    
    slack_data = {
        'channel': channel,
        'text': message,
        'username': username
    }
    
    
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )
    
    print(message)

def report_on_slack(message, channel, username, msg_type='success'):
    channel_keys = {
        "_data_alerts": "data_alert_channel_webhook_url"
    }

    try: 
        secret_key = channel_keys[channel]
    except:
        message = "Slack channel not found in channel key list -> commons.log.slack_message.py. Alerts will not be sent"
        print(message)
        return message
    else:
        secret_key = channel_keys[channel]

        webhook_url = get_slack_webhook(secret_key)
        
        if msg_type == "warning":
            message = f'🔴 Warning: {message}\n'
        elif msg_type == "error":
            message = f'🔴 Error: {message}\n'
        elif msg_type == "success":
            message = f'🟢 Success: {message}\n'      

        post_slack_message(message, webhook_url, channel, username)
