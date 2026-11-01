"""Optimization experiment at 500 ev/s: spark.sql.shuffle.partitions 200(default) vs 3 vs 1."""
import json, time, statistics
from pyspark.sql import SparkSession, functions as F
res = []
for sp in [200, 3, 1]:
    spark = (SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(sp)).config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    src = spark.readStream.format("rate").option("rowsPerSecond", 500).load()
    ev = (src.withColumn("job_type", F.element_at(F.array(F.lit("ml"), F.lit("image"), F.lit("text"), F.lit("simulation")), (F.col("value") % 4 + 1).cast("int")))
             .withColumn("duration_sec", F.col("value") % 110 + 10))
    agg = (ev.withWatermark("timestamp", "10 seconds").groupBy(F.window("timestamp", "5 seconds"), "job_type")
             .agg(F.count("*").alias("count"), F.avg("duration_sec").alias("avg_duration")))
    q = agg.writeStream.outputMode("update").format("noop").trigger(processingTime="1 second").start()
    time.sleep(10); time.sleep(60 if sp == 200 else 20)
    prog = [x for x in q.recentProgress if x["numInputRows"] > 0][-15:]
    d = [x["durationMs"]["triggerExecution"] for x in prog] or [None]
    res.append({"shuffle_partitions": sp, "batches_completed": len(prog), "avg_batch_ms": round(statistics.mean(d), 1) if d[0] else None, "max_batch_ms": max(d) if d[0] else None})
    print(res[-1], flush=True); q.stop(); spark.stop()
json.dump(res, open("logs/spark_opt.json", "w"), indent=1)
