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

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection to Redshift closed")  

    def fetch_results(self):
        result_set = self.cursor.fetchall()
        return result_set       