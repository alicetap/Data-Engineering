# Project 3: Lambda - Redshift x MySQL

This project aims to facilitate data transfer between Redshift and MySQL. This process plays a crucial role in migrating analytical data to our production environment, allowing it to be reflected on our official product platform. This enhances client access to the content.

- **Data Stack**: Redshift, Mysql, Lambda, AWS.

- **Medium Article**: [Data Transfer Between Redshift and MySQL with Python.](https://medium.com/@alice_thomaz/0bfa0003cee9)

- **Project Organization**
  - :file_folder: commons - Folder containing commonly used base codes, divided between AWS-related codes and log generation.
  - :file_folder: lambda_redshift_x_mysql - Folder with the final Python code for the project.

- **Environment Variables**
This project is designed to run on an AWS Lambda function, and certain environment variables are required. They can be configured directly in the AWS console. For more details, refer to the [AWS documentation](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html).
    - SLACK_CHANNEL - The name of the Slack channel.
    - SLACK_USERNAME - Slack username.
    - REGION - AWS region for service access.
    - REDSHIFT_SECRET - Name of the secret containing Redshift credentials.
    - MYSQL_SECRET - Name of the secret containing MySQL credentials.