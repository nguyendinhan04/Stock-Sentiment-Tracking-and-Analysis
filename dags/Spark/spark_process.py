# from pyspark.sql import SparkSession
# import os
# from dotenv import load_dotenv
# from pyspark.sql.types import StringType,IntegerType,StructField, StructType,FloatType

# def main():
#     load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
#     spark = SparkSession.builder.appName("spark_process") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
#         .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
#         .config("spark.hadoop.fs.s3a.endpoint", "host.docker.internal:9000") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#         .getOrCreate()
    

#     ohcl_schema = StructType([
#         StructField("ticker",StringType()),
#         StructField("o",FloatType()),
#         StructField("c",FloatType()),
#         StructField("h",FloatType()),
#         StructField("l",FloatType()),
#         StructField("v",IntegerType()),
#         StructField("vwap",FloatType()),
#         StructField("time",StringType()),
#         StructField("trans",IntegerType()),
#     ])

#     ohcl_df = spark.read \
#         .format("json") \
#         .option("path", "s3a://polygon/OHCL_2025-04-07.json") \
#         .schema(ohcl_schema) \
#         .load()
    
#     ohcl_df.select().limit(10).show()
    

# if __name__ == "__main__":
#     main()