from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name

spark = SparkSession.builder \
    .appName("RAW_INGEST") \
    .master("local[2]") \
    .config("spark.driver.memory", "1500m") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars", "/opt/spark_jars/hadoop-aws-3.3.4.jar,/opt/spark_jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

FILES = "/opt/airflow/scripts/*.parquet"

df = spark.read.parquet(FILES)
print(f"Read {df.count()} rows from {FILES}")

df = df.limit(5000)

enriched_df = df \
    .withColumn("Ingested_at",current_timestamp()) \
    .withColumn("Located_at",input_file_name())

print("Schema after adding metadata columns:")
enriched_df.printSchema()

OUTPUT_PATH = "s3a://raw/taxi_ingest/"

enriched_df.write.mode("append").parquet(OUTPUT_PATH)
print(f"Wrote to {enriched_df.count()} rows to {OUTPUT_PATH }")

spark.stop()