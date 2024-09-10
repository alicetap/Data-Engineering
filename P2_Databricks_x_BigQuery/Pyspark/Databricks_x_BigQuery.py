from pyspark.sql import SparkSession
import hvac
import os

def set_spark_context(response):
    # Extract credentials and configurations from the response
    credentials = response['data']['data']['credentials']
    client_email = response['data']['data']['client_email']
    project_id = response['data']['data']['project_id']
    private_key = response['data']['data']['private_key']
    private_key_id = response['data']['data']['private_key_id']

    # Create a Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Configure Spark with the credentials
    spark.conf.set("credentials", credentials)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email", client_email)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.project.id", project_id)
    spark.sparkContext._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key", private_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key.id", private_key_id)

    return response

def execute():
    # Get environment variables for Vault
    print("Retrieving secrets from Vault")
    vault_url = os.environ.get('VAULT_URL')
    vault_token = os.environ.get('VAULT_TOKEN')

    # Connect to Vault and retrieve the secrets
    client = hvac.Client(url=vault_url, token=vault_token)
    response = client.secrets.kv.read_secret_version(path='{{secret_path}}', mount_point="{{secrets_engine}}")
    print("Secrets retrieved")

    # Configure the Spark context with the retrieved secrets
    print("Configuring Spark context")
    set_spark_context(response)
    print("Spark context configured")

    # Create a Spark session
    spark = SparkSession.builder.getOrCreate()

    # Load data from the marketing table into a DataFrame
    print("Loading data from the marketing table into the DataFrame")
    query_source_df = spark.sql("SELECT * FROM {{marketing_database}}.{{marketing_table}}")
    print("Data loaded into DataFrame")

    # Write the data to BigQuery
    print("Writing data to BigQuery from the DataFrame")
    bucket_id = response['data']['data']['bucket_id'] # Temporary GCS bucket ID
    parent_project_id = response['data']['data']['parent_project_id'] # Parent project ID in BigQuery

    table = "{{bigquery_dataset}}.{{bigquery_table}}"
    query_source_df.write.format("bigquery").option("temporaryGcsBucket", bucket_id).option("table", table).option(
        "parentProject", parent_project_id).mode("overwrite").save()
    print("Data writing to BigQuery completed")