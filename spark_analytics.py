from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder \
    .appName("EcommerceRealTimeAnalytics") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 1. Schéma JSON
schema = StructType([
    StructField("type", StringType(), True),
    StructField("guestId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("url", StringType(), True),
    StructField("payload", StringType(), True)
])

kafka_ip = "10.132.0.3" 

# 2. Lecture du flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_ip}:9092") \
    .option("subscribe", "web-events") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Parsing et typage du timestamp
events = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# --- CONFIGURATION DU DATA LAKE (GCS) ---
# def  bucket créé sur GCP
bucket_name = "tp-bigdata-datalake"
checkpoint_path = f"gs://{bucket_name}/checkpoints/web_events"
storage_path = f"gs://{bucket_name}/data/web_events"

# 4. Écriture dans le Data Lake au format Parquet
# Le checkpoint est OBLIGATOIRE pour le streaming (évite de perdre le fil si crash)
query_storage = events.writeStream \
    .format("parquet") \
    .option("path", storage_path) \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("type") \
    .start()

# 5. Affichage console 
query_console = events.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print(f">>> Analyseur Spark actif. Stockage vers gs://{bucket_name}...")

# Attente des deux flux
spark.streams.awaitAnyTermination()
