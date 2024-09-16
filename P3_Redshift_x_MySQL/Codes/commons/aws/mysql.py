import os
import boto3
import json
import mysql.connector

class MYSQL:
    def __init__(self, credentials):
        self.host = credentials['host']
        self.port = credentials.get('port', 3306)
        self.database = credentials['database']
        self.user = credentials['user']
        self.password = credentials['password']
        self.connection = None
        self.cursor = None
        self.commit = None

    def connect_mysql(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                use_unicode=True,
                charset="utf8"
            )
            print("Connected to MYSQL")
        except mysql.connector.Error as e:
            print(f"Error connecting to MYSQL: {e}")            

    def execute_query_mysql(self, query, data=None):
        try:
            cursor = self.connection.cursor(buffered=True)

            if (data != None):
                cursor.execute(query, data)
            else:
                cursor.execute(query)

            self.connection.commit()
        except mysql.connector.Error as e:
            print(f'Unable to execute query: {query}')
            raise e
        finally:
            cursor.close()
            
    def close_connection_mysql(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection to MYSQL closed")