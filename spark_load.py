"""Spark Structured Streaming load test (local[2], rate source, 1 s micro-batches).
Same transformation as spark_stream.py. Latency = avg micro-batch triggerExecution time (ms)."""
import json, sys, time, psutil, statistics
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "3").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
def tot(p):
    return sum(sum(x.cpu_times()[:2]) for x in [p]+p.children(recursive=True))
res = []
for rate in [10, 100, 500]:
    src = spark.readStream.format("rate").option("rowsPerSecond", rate).load()
    ev = (src.withColumn("job_type", F.element_at(F.array(F.lit("ml"), F.lit("image"), F.lit("text"), F.lit("simulation")), (F.col("value") % 4 + 1).cast("int")))
             .withColumn("duration_sec", (F.col("value") % 110 + 10)))
    agg = (ev.withWatermark("timestamp", "10 seconds").groupBy(F.window("timestamp", "5 seconds"), "job_type")
             .agg(F.count("*").alias("count"), F.avg("duration_sec").alias("avg_duration")))
    q = agg.writeStream.outputMode("update").format("noop").trigger(processingTime="1 second").start()
    time.sleep(8)                                   # warm-up, not measured
    p = psutil.Process(); c0 = tot(p); w0 = time.time(); n0 = len(q.recentProgress)
    time.sleep(20)
    wall = time.time() - w0; cpu = tot(p) - c0
    ch = psutil.Process().children(recursive=True)
    prog = [x for x in q.recentProgress if x["numInputRows"] > 0][-15:]
    rows = sum(x["numInputRows"] for x in prog); dur = [x["durationMs"]["triggerExecution"] for x in prog]
    pr = [x["processedRowsPerSecond"] for x in prog if x.get("processedRowsPerSecond")]
    res.append({"input_rate": rate, "observed_throughput": round(rows / len(prog), 1),
                "avg_batch_ms": round(statistics.mean(dur), 1), "max_batch_ms": max(dur),
                "processed_rows_per_s": round(statistics.mean(pr), 1), "cpu_pct": round(100*cpu/wall,1), "batches": len(prog)})
    q.stop(); print(res[-1], flush=True)
json.dump(res, open("logs/load_spark.json", "w"), indent=1)
