"""Kafka consumer (consumer group 'aitu-analytics'): validates schema, aggregates, prints results."""
import json, sys
from kafka import KafkaConsumer
from stream_logic import Aggregator, validate

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
consumer = KafkaConsumer(
    "aitu.research.jobs", bootstrap_servers=BOOTSTRAP, group_id="aitu-analytics",
    auto_offset_reset="earliest", enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode()),
    consumer_timeout_ms=15000)            # stop after 15 s without data (demo mode)

agg = Aggregator()
for msg in consumer:
    ok, reason = validate(msg.value)
    print(f"p{msg.partition} off={msg.offset} {msg.value.get('event_id')} valid={ok} ({reason})")
    if ok:
        agg.add(msg.value)
    else:
        print("  -> dead-letter (would go to topic aitu.research.jobs.dlq)")
    consumer.commit()                      # at-least-once: commit after processing

print("status:", agg.status, "invalid:", agg.invalid)
print("by job_type:", json.dumps(agg.type_table(), indent=1))
print("avg CPU by group:", agg.group_cpu())
for w in agg.window_table():
    print(w)
