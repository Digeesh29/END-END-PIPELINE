from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp,to_date, sum as spark_sum,count as spark_count,avg as spark_avg;

spark = SparkSession.builder \
    .appName("TRANSFORM") \
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

RAW_PATH = "s3a://raw/taxi_ingest/*.parquet";
trips = spark.read.parquet(RAW_PATH);
print(f"Read {trips.count()} rows from {RAW_PATH}")


trips = trips.dropDuplicates(["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
                              "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", 
                              "PULocationID", "DOLocationID", "payment_type", "fare_amount",
                              "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", 
                              "total_amount", "congestion_surcharge"]);
print(f"{trips.count()} rows remain after deduplication")



trips = trips \
    .withColumn("trip_distance",col("trip_distance").cast("double")) \
    .withColumn("fare_amount",col("fare_amount").cast("double")) \
    .withColumn("extra",col("extra").cast("double")) \
    .withColumn("mta_tax",col("mta_tax").cast("double")) \
    .withColumn("tip_amount",col("tip_amount").cast("double")) \
    .withColumn("tolls_amount",col("tolls_amount").cast("double")) \
    .withColumn("improvement_surcharge",col("improvement_surcharge").cast("double")) \
    .withColumn("total_amount",col("total_amount").cast("double")) \
    .withColumn("congestion_surcharge",col("congestion_surcharge").cast("double"))\
    .withColumn("pickup_date",to_date(col("tpep_pickup_datetime"))) 
    
    
trips = trips.filter((col("trip_distance") > 0) & 
                     (col("fare_amount") > 0) &
                     (col("total_amount") > 0))

print(f"{trips.count()} rows remain after type casting and filtering")


ZONES_PATH = "/opt/airflow/scripts/NYC_Taxi_Zones.csv";
zones = spark.read.csv(ZONES_PATH, header=True, inferSchema=True)
print(f"Read {zones.count()} rows from {ZONES_PATH}")


zones_pickup = zones.selectExpr(
    "`Location ID` as PULocationID",
    "Zone as pickup_zone",
    "Borough as pickup_borough"
)
zones_dropoff = zones.selectExpr(
    "`Location ID` as DOLocationID",
    "Zone as dropoff_zone",
    "Borough as dropoff_borough"
)

trips = trips \
    .join(zones_pickup,on ="PULocationID",how="left") \
    .join(zones_dropoff,on ="DOLocationID",how="left")\
    .cache();

trips.count() 
    
daily_summary = trips.groupby("pickup_borough","pickup_date").agg(
    spark_sum("total_amount").alias("daily_revenue"),
    spark_count("*").alias("daily_trips"),
    spark_avg("trip_distance").alias("avg_trip_distance"),
);

print("Sample of daily summary:")
daily_summary.show(10)

daily_summary.repartition(2).write.mode("overwrite").parquet("s3a://clean/taxi_daily_summary")
print("Wrote daily summary to s3a://clean/taxi_daily_summary/")
 
trips.repartition(2).write.mode("overwrite").parquet("s3a://clean/taxi_trips/")
print("Wrote cleaned trip-level data to s3a://clean/taxi_trips/")
 
spark.stop()

