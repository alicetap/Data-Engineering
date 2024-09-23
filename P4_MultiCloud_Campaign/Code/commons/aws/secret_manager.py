import os
import json
import boto3
from botocore.exceptions import ClientError

def get_redshift_credentials(secret_name: str, region: str) -> dict:
    print("Getting Redshift credentials.")
    credential = {}
    client = None

    try:
        client = boto3.client(service_name='secretsmanager', region_name=region)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'].strip())
        credential['user'] = secret['user'].strip()
        credential['password'] = secret['password'].strip()
        credential['host'] = secret['host'].strip()
        credential['port'] = secret['port']
        
        try:
            secret['database']
        except:
            credential['database'] = secret['dbname'].strip()
        else:
            credential['database'] = secret['database'].strip()

        return credential
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    finally:
        if client:
            client.close()

def get_sharepoint_access_key(secret_name: str, region: str) -> dict:
    print("Getting Sharepoint access key secret.")
    access_key = {}
    client = None
    try:
        client = boto3.client(service_name='secretsmanager', region_name=region)
        get_secret_value_response = client.get_secret_value( SecretId=secret_name )
        secret = json.loads(get_secret_value_response['SecretString'].strip())
        access_key['username'] = secret['username'].strip()
        access_key['password'] = secret['password'].strip()
        return access_key
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