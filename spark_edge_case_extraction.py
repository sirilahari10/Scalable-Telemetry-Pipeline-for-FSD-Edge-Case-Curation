from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when, to_timestamp, date_trunc

spark = SparkSession.builder.appName("FSDEdgeCaseExtraction").getOrCreate()

# Load raw telemetry (Simulated Parquet/Protobuf stream)
df_telemetry = spark.read.format("parquet").load("s3://fleet-data-lake/raw/")

# Bind shuffle size: Partition by vehicle and specific hour to prevent OOM on heavy users
windowSpec = Window.partitionBy("vehicle_id", date_trunc("hour", col("timestamp"))).orderBy("timestamp")

# Calculate speed delta to flag severe braking (Phantom Braking metric)
df_metrics = df_telemetry.withColumn("prev_speed", lag("speed_mph", 1).over(windowSpec)) \
                         .withColumn("speed_delta", col("speed_mph") - col("prev_speed"))

# Flag high-value edge cases for the AI team
df_edge_cases = df_metrics.withColumn(
    "is_fsd_disengagement", 
    when((col("autopilot_state") == "ENGAGED") & (col("user_steering_override") == True), True).otherwise(False)
).filter(
    (col("is_fsd_disengagement") == True) | (col("speed_delta") < -15) # 15mph drop in 1 tick
)

# Write to ClickHouse via JDBC or staging S3 bucket
df_edge_cases.write.format("jdbc") \
    .option("url", "jdbc:clickhouse://clickhouse-cluster:8123/fsd_metrics") \
    .option("dbtable", "raw_edge_cases") \
    .mode("append") \
    .save()
