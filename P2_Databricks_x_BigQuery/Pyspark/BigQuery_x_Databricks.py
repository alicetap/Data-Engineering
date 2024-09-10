from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType

import hvac
import os

def get_vault_secrets(vault_url, vault_token):
    client = hvac.Client(url=vault_url, token=vault_token)
    response = client.secrets.kv.read_secret_version(path='{{secret_path}}', mount_point="{{secrets_engine}}")
    print("Secrets retrieved")

    return response

def set_spark_context(secrets):
    # Extract credentials from the secrets
    credentials = secrets['data']['data']['credentials']
    client_email = secrets['data']['data']['client_email']
    project_id = secrets['data']['data']['project_id']
    private_key = secrets['data']['data']['private_key']
    private_key_id = secrets['data']['data']['private_key_id']

    # Create a Spark session
    spark = SparkSession.builder.getOrCreate()

    # Configure Spark with the credentials
    spark.conf.set("credentials", credentials)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email", client_email)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.project.id", project_id)
    spark.sparkContext._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key", private_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key.id", private_key_id)
    print("Spark context configured")

def get_data_from_bigquery(spark, bucket_id, parent_project_id):
    # Table name in BigQuery
    source_table = "{{source_database}}.{{source_table}}"
    print(source_table)

    # Load data from BigQuery
    df = (spark.read.format("bigquery")
          .option("temporaryGcsBucket", bucket_id)
          .option("table", source_table)
          .option("parentProject", parent_project_id)
          .option("viewsEnabled", 'true')
          .load())

    return df

def transform_data(df):
    ndf = (df.withColumn("timestamp_column", df.timestamp_column.cast(TimestampType())) 
           .withColumn("id_column", df.id_column.cast(IntegerType())) 
           .withColumnRenamed("old_date_column", "new_date_column") 
           .withColumnRenamed("timestamp_column", "timestamp_renamed") 
           .withColumnRenamed("old_venture_name", "new_venture_name") 
           .filter(df.venture_column.like('prefix%')))
    return ndf

def write_data_to_databricks(ndf):
    target_table = "{{target_database}}.{{target_table}}"
    ndf.write.mode("overwrite").saveAsTable(target_table)

def execute():
    # Get environment variables for Vault
    vault_url = os.environ.get('VAULT_URL')
    vault_token = os.environ.get('VAULT_TOKEN')

    print("Retrieving secrets from Vault")
    secrets = get_vault_secrets(vault_url, vault_token)

    print("Configuring Spark context")
    set_spark_context(secrets)

    print("Setting up parameters from secrets")
    bucket_id = secrets['data']['data']['bucket_id']
    parent_project_id = secrets['data']['data']['parent_project_id']  

    spark = SparkSession.builder.getOrCreate()

    print("Retrieving data from BigQuery")
    df = get_data_from_bigquery(spark, bucket_id, parent_project_id)

    print("Transforming data")
    ndf = transform_data(df)

    print("Writing data to Databricks")
    write_data_to_databricks(ndf)