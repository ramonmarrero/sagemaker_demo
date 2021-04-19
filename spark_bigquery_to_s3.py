
from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account


def main():
    spark = SparkSession \
        .builder \
        .appName("Spark Demo - Writing to S3") \
        .getOrCreate()

    credentials = service_account.Credentials.from_service_account_file('/opt/ml/processing/input/files/demo_credentials.json')

    project_id = 'ready4prod'
    client = bigquery.Client(credentials= credentials,project=project_id)

    query = """
       SELECT   name,  count FROM   `babynames.names_2014` WHERE   gender = 'M' ORDER BY   count DESC LIMIT   5 """

    df = client.query(query).to_dataframe()

    sparkDF=spark.createDataFrame(df) 
    sparkDF.printSchema()
    sparkDF.show()

    sparkDF.repartition(1).write.mode('overwrite').\
        parquet('s3://hrs-dna-google-analytics/data/')

if __name__ == "__main__":
    main()  
