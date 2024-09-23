# Project 4: Lambdas - Multi-Cloud Campaign

The project aims to provide business areas with data as close to real-time as possible for the Black Friday campaign, utilizing a final visualization in Power BI, without increasing the company's cloud environment costs. To achieve this, we have adapted our existing architecture to meet all requirements during the campaign period. This repository contains the code used in the main Lambda functions of the multi-cloud project:
- Transferring files from SharePoint to Redshift.
- Updating Analysis Services.
- Transferring data from AWS S3 to Azure Blob.
- Transferring data from Redshift to S3.

- **Data Stack**: Redshift, S3, Lambda, AWS, Analysis Services, Azure Blob, Azure, SharePoint.

- **Medium Article**: [Maximizing Data Architecture Efficiency with AWS and Azure.](https://medium.com/@alice_thomaz/146395ca42b3)

- **Environment Variables**
This project is designed to run on an AWS Lambda function, and certain environment variables are required. They can be configured directly in the AWS console. For more details, refer to the [AWS documentation](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html).
The required variables for each project are listed below.

- **Project Organization**
  - :file_folder: commons - Folder containing commonly used base codes, divided between AWS-related codes and log generation.
  - :file_folder: lambdas - Folder with the final Lambda code:

    - :file_folder: lambda_sharepoint_to_redshift - This project transfers data from a SharePoint file to Redshift.
    Environment variables:
      - SLACK_CHANNEL - Name of the Slack channel.
      - SLACK_USERNAME - Slack username.
      - SHAREPOINT_SITE - Link to the SharePoint site.
      - SHAREPOINT_SITE_NAME - Name of the SharePoint site.
      - SHAREPOINT_DOC - Document library.
      - SHAREPOINT_FOLDER_NAME - Name of the SharePoint folder.
      - SHAREPOINT_FILE_NAME - Name of the SharePoint file.
      - SHAREPOINT_S3_BUCKET - Name of the S3 bucket to save data after download.
      - SHAREPOINT_S3_BUCKET_FILE_DIR - Directory of the file in S3.
      - SHAREPOINT_SECRET_NAME - Name of the secret containing SharePoint credentials.
      - REGION - AWS region for service access.
      - REDSHIFT_SECRET_NAME - Name of the secret containing Redshift credentials.
      - DB_TABLE - Name of the final table in Redshift.
      - DB_SCHEMA - Schema of the final table in Redshift.
      - DB_TABLE_COLUMN_LIST - List of columns in the final table in Redshift.
      - COPY_ADDITIONAL_PARMS - Additional parameters for the COPY command from S3 to Redshift.

    - :page_facing_up: lambda_analysis_services_refresh - This project updates the Analysis Services in the Azure account. The project is divided into two blocks because some large tables are updated only once a day during the full refresh, while tables linked to CDC are updated every 2 minutes during the campaign period and should be added in the code.
    Environment variables:
      - SLACK_CHANNEL - Name of the Slack channel.
      - SLACK_USERNAME - Slack username.
      - START_TIME - Start time for the full refresh.
      - END_TIME - End time for the full refresh.

    - :page_facing_up: lambda_s3_to_azure_blob - This project transfers data from AWS S3 to the Azure Blob container. A bucket must be set as a trigger before execution, data collected from this connection will be used to identify which files to pull.
    Environment variables:
      - SLACK_CHANNEL - Name of the Slack channel.
      - SLACK_USERNAME - Slack username.
      - SECRET_NAME - Name of the secret containing the Azure credential string.
      - DELETE_FILE_S3 - Condition (true or false) to indicate if the files in S3 should be deleted.

    - :page_facing_up: lambda_unload_redshift_s3 - This project performs an unload of data from Redshift to S3.
    Environment variables:
      - IAM_ROLE - Role to be used in the UNLOAD command.
      - TABLE_NAME - Table to be unloaded, in the format 'schema.table'.
      - S3_PATH - Path to the file in S3.
      - SLACK_CHANNEL - Name of the Slack channel.
      - SLACK_USERNAME - Slack username.
      - REDSHIFT_SECRET_NAME - Name of the secret containing Redshift credentials.
      - REGION - AWS region for the UNLOAD command and for connecting to Redshift.