import json
import boto3
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from commons.log import slack_message

def lambda_handler(event, context):
        
    try:    

        # Get the Lambda function name
        lambda_name = context.function_name

        # Environment variables for the Slack webhook stored in Lambda
        SLACK_CHANNEL = os.environ['SLACK_CHANNEL'].strip()
        SLACK_USERNAME = os.environ['SLACK_USERNAME'].strip()

        # Environment variable for delete clause in Lambda
        delete_file_s3 = os.environ['DELETE_FILE_S3'].strip()

        # Secret manager variable
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name='us-east-1'
        )

        # Connection string - Azure Blob
        secret_name_str = os.environ['SECRET_NAME'].strip()
        response_str = client.get_secret_value(SecretId=secret_name_str)
        value_response_str = response_str['SecretString']
        value_json_str = json.loads(value_response_str)
        connect_str = value_json_str['connection_string']    

        # Initiate clients
        s3 = boto3.client("s3")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Collect records from the event
        records = event["Records"]
        print("Records received from S3 for update: " + str(len(records)))

        # Loop for file transfer
        for record in records:
            
            # Collect the records
            key = record["s3"]["object"]["key"]
            bucket_name = record["s3"]["bucket"]["name"]
            print("Processing object with key: " + str(key))

            # Fetch file from S3
            file = s3.get_object(Bucket=bucket_name, Key=key)

            # Iterate through the file chunks
            itr = file["Body"].iter_chunks()

            # Initiate azure container client and upload files
            azure_container_client = blob_service_client.get_container_client(container=bucket_name)
            azure_container_client.upload_blob(name=key, data=itr, overwrite=True)
            print("Object " + key + " successfully transferred to Azure")

            # Condition to delete the file in S3 after upload
            if delete_file_s3 == "true":
               s3.delete_object(Bucket=bucket_name, Key=key)

    except (Exception) as e:
        output_message = f'{e}.\nLambda: {lambda_name}.' 
        print(output_message)            
        slack_message.report_on_slack(message=output_message, channel=SLACK_CHANNEL, username=SLACK_USERNAME, msg_type="error")