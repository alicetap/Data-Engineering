import os
from commons.aws.redshift import Redshift
from commons.aws.secret_manager import get_sharepoint_access_key, get_redshift_credentials
from sharepoint_downloader import SharepointDownloader
from datetime import date
from commons.log import slack_message

def handler(event, context):

    current_date = date.today()

    sharepoint_downloader = SharepointDownloader(
        '/tmp',
        os.environ['SHAREPOINT_SITE'].strip(),
        os.environ['SHAREPOINT_SITE_NAME'].strip(),
        os.environ['SHAREPOINT_DOC'].strip(),
        os.environ['SHAREPOINT_FOLDER_NAME'].strip(),
        os.environ['SHAREPOINT_FILE_NAME'].strip(),
        os.environ['SHAREPOINT_S3_BUCKET'].strip(),
        os.environ['SHAREPOINT_S3_BUCKET_FILE_DIR'].strip()
    )
    
    # Retrieve variables and access keys from Secret Manager
    slack_channel = getattr(sharepoint_downloader, 'channel')
    slack_username = getattr(sharepoint_downloader, 'username')
    share_credential = get_sharepoint_access_key(os.environ['SHAREPOINT_SECRET_NAME'].strip(), os.environ['REGION'].strip())
    credential = get_redshift_credentials(os.environ['REDSHIFT_SECRET_NAME'].strip(), os.environ['REGION'].strip())

    redshiftingestion = Redshift(credential)

    # Download the file from SharePoint
    sharepoint_downloader.download_file(share_credential)

    # Convert the file from XLSX to CSV
    sharepoint_downloader.convert_xlsx_to_csv()

    # Copy the file to S3
    sharepoint_downloader.copy_file_to_s3()

    sharepoint_s3_complete_path = sharepoint_downloader.get_sharepoint_file_s3_complete_path()

    # Copy data from S3 to Redshift and send a Slack message for monitoring
    if sharepoint_downloader.file_exists_s3 == 1:
        table_list = list(map(str.strip, os.environ['DB_TABLE'].split(',')))
        db_schema = os.environ['DB_SCHEMA'].strip()
        column_list = os.environ['DB_TABLE_COLUMN_LIST'].strip()
        copy_parms = os.environ['COPY_ADDITIONAL_PARMS'].strip()
        try:
            redshiftingestion.load_s3_to_redshift(sharepoint_s3_complete_path, table_list, column_list, copy_parms, db_schema)
            message = f"Table {table_list} loaded to Redshift."
            slack_message.report_on_slack(message=message, channel=slack_channel, username=slack_username, msg_type="success")
        except:
            message = f"Unable to copy table {table_list} to Redshift."
            slack_message.report_on_slack(message=message, channel=slack_channel, username=slack_username, msg_type="error")
            raise Exception(message)    
    return 0
