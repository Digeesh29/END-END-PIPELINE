from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
 
spark = SparkSession.builder \
    .appName("FINAL_ANALYTICS") \
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

PATH = "s3a://clean/taxi_trips/*.parquet"
df = spark.read.parquet(PATH)
print(f"Read {df.count()} rows from{PATH}")

df = df.withColumn("year",year(col("pickup_date"))) \
       .withColumn("month",month(col("pickup_date")))
       
df.select("pickup_date","year","month").distinct().show()

FINAL_PATH = "s3a://clean/taxi_analytics/"

df.write.mode("overwrite").partitionBy("year", "month").parquet(FINAL_PATH)

print(f"Wrote partitioned analytics table to {FINAL_PATH}")

sample_query = spark.read.parquet(FINAL_PATH).filter(
    (col("year") == 2026) & (col("month") == 1)
)


print("\n--- Query plan for year=2026, month=1 filter ---")
sample_query.explain()
 
print(f"Rows matching year=2026, month=1: {sample_query.count()}")
 
spark.stop()