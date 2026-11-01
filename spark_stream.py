"""Spark Structured Streaming: 5 s event-time windows by job_type -> window, job_type, count, avg_duration.
Usage:  python spark_stream.py files            (reads JSON files from spark_input/, no broker needed)
        python spark_stream.py kafka host:9092  (reads topic aitu.research.jobs; needs spark-sql-kafka package)"""
import sys
from pyspark.sql import SparkSession, functions as F, types as T

mode = sys.argv[1] if len(sys.argv) > 1 else "files"
spark = (SparkSession.builder.appName("aitu-stream").master("local[2]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "3")        # = number of Kafka partitions
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

schema = T.StructType([T.StructField("event_id", T.StringType()), T.StructField("timestamp", T.StringType()),
    T.StructField("research_group", T.StringType()), T.StructField("job_type", T.StringType()),
    T.StructField("duration_sec", T.IntegerType()), T.StructField("status", T.StringType()),
    T.StructField("cpu_pct", T.IntegerType())])

if mode == "kafka":
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", sys.argv[2])
           .option("subscribe", "aitu.research.jobs").option("startingOffsets", "earliest").load())
    events = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
else:
    events = spark.readStream.schema(schema).json("spark_input")

events = events.withColumn("event_time", F.to_timestamp("timestamp"))
result = (events.withWatermark("event_time", "10 seconds")
          .groupBy(F.window("event_time", "5 seconds"), "job_type")
          .agg(F.count("*").alias("count"), F.round(F.avg("duration_sec"), 2).alias("avg_duration")))

collected = {}
def on_batch(df, batch_id):                       # update mode: only changed windows arrive
    for r in df.collect():
        collected[(r["window"]["start"], r["job_type"])] = (r["window"]["end"], r["count"], r["avg_duration"])
    print(f"batch {batch_id}: {df.count()} updated rows")

q = (result.writeStream.outputMode("update").foreachBatch(on_batch)
     .option("checkpointLocation", "/tmp/aitu_ckpt").trigger(availableNow=True).start())
q.awaitTermination()
print("\nwindow | job_type | count | avg_duration")
for (s, jt), (e, c, a) in sorted(collected.items()):
    print(f"{s:%H:%M:%S}-{e:%H:%M:%S} | {jt} | {c} | {a}")
