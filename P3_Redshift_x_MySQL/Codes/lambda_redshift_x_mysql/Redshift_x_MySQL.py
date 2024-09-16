import json
import os
import boto3
import psycopg2
import mysql.connector
from commons.aws.redshift import Redshift
from commons.aws.mysql import MySQL
from commons.aws.secret_manager import get_redshift_credentials, get_mysql_credentials
from commons.log import slack_message


# Retrieve environment variables saved in Lambda
SLACK_CHANNEL = os.environ['SLACK_CHANNEL'].strip()
SLACK_USERNAME = os.environ['SLACK_USERNAME'].strip()

aws_region = os.environ['REGION'].strip()

mysql_secret = os.environ['MYSQL_SECRET'].strip()
redshift_secret = os.environ['REDSHIFT_SECRET'].strip()


def lambda_handler(event, context):

    # Get the Lambda function name
    lambda_name = context.function_name

    # Retrieve Redshift and MySQL credentials from Secret Manager        
    red_credential = get_redshift_credentials(redshift_secret, aws_region)
    redshift_client = Redshift(red_credential)

    mysql_credential = get_mysql_credentials(mysql_secret, aws_region)
    mysql_client = MySQL(mysql_credential)


    # Start the database process
    try:
        redshift_client.connect_redshift()
        mysql_client.connect_mysql()

        # Create table if it does not exist - MySQL
        query1 = f"""

            CREATE TABLE IF NOT EXISTS `final_content_summary` (
            
                `state` VARCHAR(255),
                `item_id` INTEGER,
                `top_5_position` BIGINT,
                `change` DOUBLE PRECISION,
                `latest_date` timestamp,
                `user_category_id` INTEGER)

                ENGINE=InnoDB 
                DEFAULT CHARSET=utf8
        """
        mysql_client.execute_query_mysql(query1)         
        print("Create table executed successfully in MySQL.")  
        
    
        # Clear final table - MySQL
        query2 = f'TRUNCATE TABLE final_content_summary'
        mysql_client.execute_query_mysql(query2)
        print("Truncate executed successfully in MySQL.") 

        # Select DBT model - Redshift
        query3 = f"""

            SELECT
              state_code,
              content_identifier,
              top_5_ranking,
              change_amount,
              recent_date,
              user_category
            FROM dbt_prod_source.content_summary_source

        """
        redshift_client.execute_query(query3)
        print("Select executed successfully in Redshift.")

        # Fetch the data
        result_set = redshift_client.fetch_results()


        # Prepare insert - MySQL
        insert_data_mysql_query = (
        """
            INSERT INTO `final_content_summary` (`state`, `item_id`, `top_5_position`, `change`, `latest_date`, `user_category_id`) VALUES (%s,%s,%s,%s,%s,%s)
        """
                                  )   
        
        for row in result_set:
            mysql_client.execute_query_mysql(insert_data_mysql_query, row)

        print("Insert executed successfully in MySQL.")

        try:

            # Generate Slack message
            output_message = f'Lambda {lambda_name} completed successfully! Table final_content_summary updated.'      
            slack_message.report_on_slack(message=output_message, channel=SLACK_CHANNEL, username=SLACK_USERNAME, msg_type="success")
            print("The webhook.send call was executed successfully.")

        except Exception as error:
            print(f"An error occurred while sending the message via webhook: {error}")  

    except Exception as e:
        output_message = f'{e}.\nLambda: {lambda_name}.' 
        print(output_message)            
        slack_message.report_on_slack(message=output_message, channel=SLACK_CHANNEL, username=SLACK_USERNAME, msg_type="error")

    finally:
        # Close connection to Redshift and MySQL
        redshift_client.close_connection() 
        mysql_client.close_connection_mysql()