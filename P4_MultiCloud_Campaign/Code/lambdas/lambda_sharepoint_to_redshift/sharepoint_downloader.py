import urllib.request
import requests
import os
import io
import boto3
import csv

from openpyxl import Workbook, load_workbook
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from commons.log import slack_message


class SharepointDownloader(object):
    def __init__(self,
            out_path,
            sharepoint_site,
            sharepoint_site_name,
            sharepoint_doc,
            sharepoint_folder_name,
            sharepoint_file_name,
            sharepoint_s3_bucket,
            sharepoint_s3_bucket_file_dir
            ) -> None:
        self.out_path = out_path
        self.sharepoint_site = sharepoint_site
        self.sharepoint_site_name = sharepoint_site_name
        self.sharepoint_doc = sharepoint_doc
        self.sharepoint_folder_name = sharepoint_folder_name
        self.sharepoint_file_name = sharepoint_file_name
        self.sharepoint_s3_bucket = sharepoint_s3_bucket
        self.sharepoint_s3_bucket_file_dir = sharepoint_s3_bucket_file_dir
        self.csv_file_name = self.sharepoint_file_name.replace('xlsx','csv')
        self.path_to_sharepoint_file = os.path.join(self.out_path, self.sharepoint_file_name)
        self.path_to_s3_sharepoint_file = os.path.join(self.sharepoint_s3_bucket_file_dir, self.csv_file_name)
        self.path_to_csv_file = os.path.join(self.out_path, self.csv_file_name)
        self.file_exists_s3 = None
        self.channel = os.environ['SLACK_CHANNEL'].strip()
        self.username = os.environ['SLACK_USERNAME'].strip()

    def get_sharepoint_file_s3_complete_path(self) -> str:
        return os.path.join('s3://',  self.sharepoint_s3_bucket, self.path_to_s3_sharepoint_file)

    def connect_sharepoint(self, share_credential):
        try:
            print(f'01.Connecting to SHAREPOINT')
            conn_auth = AuthenticationContext(self.sharepoint_site)
            conn_auth.acquire_token_for_user(share_credential['username'],share_credential['password'])
            conn = ClientContext(self.sharepoint_site, conn_auth)
            return conn
        except:
            message = f"Unable to connect to Sharepoint."
            slack_message.report_on_slack(message=message, channel=self.channel, username=self.username, msg_type="error")
            raise Exception(message)   

    def download_file(self, share_credential):
        print(f"Start the download of file from SHAREPOINT.")
        try:
            conn = self.connect_sharepoint(share_credential)
            print(f'02.Downloading file {self.sharepoint_file_name} from SHAREPOINT to {self.out_path}')
            file_url = f'/sites/{self.sharepoint_site_name}/{self.sharepoint_doc}/{self.sharepoint_folder_name}/{self.sharepoint_file_name}'
            file = File.open_binary(conn, file_url)
            output_file = self.out_path + '/' + self.sharepoint_file_name
            with open(output_file,'wb') as f:
                f.write(file.content)
        except:
            message = f"Unable to download the file{self.sharepoint_file_name}."
            slack_message.report_on_slack(message=message, channel=self.channel, username=self.username, msg_type="warning")
            self.file_exists_s3 = 0
            raise Exception(message)

    def convert_xlsx_to_csv(self):
        print(f'03.Converting file: {self.sharepoint_file_name} to csv at: {self.out_path}')
        try:
            wb = load_workbook(self.path_to_sharepoint_file)
            # Select the active sheet
            ws = wb.active                        
            # Delete the first row, which is empty          
            with open(self.path_to_csv_file, 'w', newline="", encoding='utf-8') as f:
                wr = csv.writer(f,quoting=csv.QUOTE_ALL)
                for x in ws.rows:
                    wr.writerow([cell.value for cell in x])
        except:
            message = f"Unable to convert the file {self.sharepoint_file_name} to CSV."
            slack_message.report_on_slack(message=message, channel=self.channel, username=self.username, msg_type="error")
            self.file_exists_s3 = 0
            raise Exception(message)

    def copy_file_to_s3(self):
        s3 = None
        print(f"04.Copying file from {self.out_path} to s3.")
        try:
            s3 = boto3.client("s3")
            s3.upload_file(self.path_to_csv_file, self.sharepoint_s3_bucket, self.path_to_s3_sharepoint_file)
            self.file_exists_s3 = 1
        except:
            message = f"Unable to copy the file from {self.output_path} to S3."
            slack_message.report_on_slack(message=message, channel=self.channel, username=self.username, msg_type="error")
            self.file_exists_s3 = 0
            raise Exception(message)
        finally:
            if s3:
                print("Closing the S3 client")
                s3.close()        