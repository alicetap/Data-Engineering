import os
import boto3
import json
import psycopg2

class Redshift:
    def __init__(self, credentials):
        self.host = credentials['host']
        self.port = credentials.get('port', 5439)
        self.database = credentials['database']
        self.user = credentials['user']
        self.password = credentials['password']
        self.connection = None
        self.cursor = None

    def connect_redshift(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Connected to Redshift")
        except psycopg2.Error as e:
            print(f"Error connecting to Redshift: {e}")          

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
        except psycopg2.Error as e:
            print(f'Unable to execute query: {query}')
            raise e

    def load_s3_to_redshift(self, csv_s3_complete_path, table_list, column_list, copy_parms, db_schema, delete_stmt='', is_incremental=False) -> None:
        self.connect_redshift()
        
        for table in table_list:
            if not is_incremental:
                if delete_stmt == '':
                    print(f'Truncating table {db_schema}.{table} before loading new data.')
                    query = f'TRUNCATE TABLE {db_schema}.{table}'
                else:
                    print(f'Deleting old data from table {db_schema}.{table} before loading new data.')
                    query = delete_stmt
                self.execute_query(query)

        iam_role = os.environ['IAM_ROLE'].strip()
        region = os.environ['REGION'].strip()
        for table in table_list:
            table = table.strip()
            print(f'Loading new data into Redshift table: {db_schema}.{table}.')
            query = f"COPY {db_schema}.{table} {column_list} FROM '{csv_s3_complete_path}' IAM_ROLE '{iam_role}' REGION '{region}' {copy_parms} "
            try:
                self.execute_query(query)
            except:
                print(f'Error on loading table {db_schema}.{table}, try looking at stl_load_errors table on Redshift.')

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
        except psycopg2.Error as e:
            print(f'Unable to execute query: {query}')
            raise e

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection to Redshift closed")