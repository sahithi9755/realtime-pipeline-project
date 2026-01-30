from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming.state import GroupStateTimeout
import os

spark = SparkSession.builder.appName("UserActivityPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# CONFIG FROM ENV
# -------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DB_URL = os.getenv("DB_URL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# -------------------------------
# SCHEMA
# -------------------------------
schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("user_id", StringType()),
    StructField("page_url", StringType()),
    StructField("event_type", StringType())
])

# -------------------------------
# READ FROM KAFKA
# -------------------------------
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "user_activity") \
    .option("startingOffsets", "earliest") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("event_time", "2 minutes") \
    .withColumn("event_date", to_date("event_time"))

# -------------------------------
# 1️⃣ PAGE VIEW COUNTS (1-min tumbling)
# -------------------------------
page_views = df.filter(col("event_type") == "page_view") \
    .groupBy(window("event_time", "1 minute"), col("page_url")) \
    .count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "page_url",
        col("count").alias("view_count")
    )

# -------------------------------
# 2️⃣ ACTIVE USERS (5-min sliding)
# -------------------------------
active_users = df.groupBy(
    window("event_time", "5 minutes", "1 minute")
).agg(approx_count_distinct("user_id").alias("active_user_count")) \
.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "active_user_count"
)

# -------------------------------
# 3️⃣ SESSION TRACKING (STATEFUL)
# -------------------------------
session_events = df.filter(col("event_type").isin("session_start", "session_end")) \
    .select("user_id", "event_time", "event_type")

def session_state_func(key, pdf_iter, state):
    user_id = key[0]
    results = []

    if state.exists:
        start_time = state.get("start_time")
    else:
        start_time = None

    for pdf in pdf_iter:
        for _, row in pdf.iterrows():
            if row.event_type == "session_start":
                start_time = row.event_time
                state.update({"start_time": start_time})
                state.setTimeoutDuration(15 * 60 * 1000)

            elif row.event_type == "session_end" and start_time is not None:
                duration = int((row.event_time - start_time).total_seconds())
                results.append((user_id, start_time, row.event_time, duration))
                state.remove()
                start_time = None

    if results:
        return pd.DataFrame(results, columns=[
            "user_id", "session_start_time", "session_end_time", "session_duration_seconds"
        ])
    else:
        return pd.DataFrame(columns=[
            "user_id", "session_start_time", "session_end_time", "session_duration_seconds"
        ])

sessions_df = session_events.groupBy("user_id") \
    .applyInPandasWithState(
        session_state_func,
        outputStructType=StructType([
            StructField("user_id", StringType()),
            StructField("session_start_time", TimestampType()),
            StructField("session_end_time", TimestampType()),
            StructField("session_duration_seconds", LongType())
        ]),
        stateStructType=StructType([
            StructField("start_time", TimestampType())
        ]),
        outputMode="update",
        timeoutConf=GroupStateTimeout.EventTimeTimeout
    )

# -------------------------------
# WRITE RAW TO DATA LAKE
# -------------------------------
df.writeStream \
  .format("parquet") \
  .option("path", "/opt/spark/data/lake") \
  .option("checkpointLocation", "/opt/spark/checkpoints/lake") \
  .partitionBy("event_date") \
  .start()

# -------------------------------
# DB WRITE FUNCTION
# -------------------------------
def write_to_postgres(batch_df, table_name):
    batch_df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

page_views.writeStream.foreachBatch(
    lambda df, _: write_to_postgres(df, "page_view_counts")
).option("checkpointLocation", "/opt/spark/checkpoints/page_views").start()

active_users.writeStream.foreachBatch(
    lambda df, _: write_to_postgres(df, "active_users")
).option("checkpointLocation", "/opt/spark/checkpoints/active_users").start()

sessions_df.writeStream.foreachBatch(
    lambda df, _: write_to_postgres(df, "user_sessions")
).option("checkpointLocation", "/opt/spark/checkpoints/sessions").start()

# -------------------------------
# ENRICHED STREAM TO KAFKA
# -------------------------------
enriched = df.withColumn("processing_time", current_timestamp())

enriched.selectExpr("to_json(struct(*)) AS value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
  .option("topic", "enriched_activity") \
  .option("checkpointLocation", "/opt/spark/checkpoints/enriched") \
  .start()

spark.streams.awaitAnyTermination()
