import sys
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("VALIDATE") \
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
df = df.cache()

print(f"Number of rows in validated dataset:", df.count())

failures = []

KEY_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "total_amount",
    "pickup_date",
]


for i in KEY_COLUMNS:
    if df.filter(col(i).isNull()).count() > 0:
        failures.append(f"Column {i} has null values")
    else:
        print(f"NULL CHECK PASSED: '{i}' has no nulls")
        
MIN_ROWS = 100
MAX_ROWS = 1000000

if df.count() < MIN_ROWS:
    failures.append(f"Row count is less than {MIN_ROWS}")   
elif df.count() > MAX_ROWS:
    failures.append(f"Row count is greater than {MAX_ROWS}")
else:
    print(f"ROW COUNT CHECK PASSED: Row count is between {MIN_ROWS} and {MAX_ROWS}")
    

today_date = date.today()
future_days = df.filter(col("pickup_date")>today_date).count()
if future_days > 0:
    failures.append(f"Found {future_days} rows with pickup_date in the future")
else:
    print(f"FUTURE DATE CHECK PASSED: No rows with pickup_date in the future")


print("\n--- VALIDATION SUMMARY ---")
if failures:
    print(f"{len(failures)} check(s) FAILED:")
    for f in failures:
        print(f"  - {f}")
    spark.stop()
    sys.exit(1)  
else:
    print("All checks PASSED.")
    spark.stop()
    sys.exit(0)