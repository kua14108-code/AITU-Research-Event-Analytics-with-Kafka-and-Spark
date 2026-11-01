"""Kafka producer: sends the supplied events to topic aitu.research.jobs.
Key = research_group  ->  all events of one group land in the same partition (per-group ordering)."""
import csv, json, sys, time
from kafka import KafkaProducer

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
TOPIC = "aitu.research.jobs"
DELAY = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0   # seconds between sends (0 = as fast as possible)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
    acks="all", retries=5, linger_ms=5)

with open("data/events.csv") as f:
    for row in csv.DictReader(f):
        row["duration_sec"] = int(row["duration_sec"]); row["cpu_pct"] = int(row["cpu_pct"])
        md = producer.send(TOPIC, key=row["research_group"], value=row).get(timeout=10)
        print(f"sent {row['event_id']} key={row['research_group']} -> partition {md.partition} offset {md.offset}")
        time.sleep(DELAY)
producer.flush()
