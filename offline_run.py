"""Runs the exact consumer logic on data/events.csv without a broker (execution-generated output)."""
import csv, json
from stream_logic import Aggregator
agg = Aggregator()
with open("data/events.csv") as f:
    for r in csv.DictReader(f):
        r["duration_sec"] = int(r["duration_sec"]); r["cpu_pct"] = int(r["cpu_pct"])
        agg.add(r)
print("status:", agg.status, "invalid:", agg.invalid)
print("by job_type:", json.dumps(agg.type_table(), indent=1))
print("avg CPU by group:", agg.group_cpu())
print("windows:")
for w in agg.window_table():
    print(" ", w)
# negative tests for schema validation
from stream_logic import validate
bad = [{"event_id": "X1"}, {"event_id":"X2","timestamp":"14:00:01","research_group":"G01","job_type":"quantum",
       "duration_sec":1,"status":"completed","cpu_pct":10},
       {"event_id":"X3","timestamp":"14:00:01","research_group":"G01","job_type":"ml","duration_sec":"5",
       "status":"completed","cpu_pct":10},
       {"event_id":"X4","timestamp":"14:00:01","research_group":"G01","job_type":"ml","duration_sec":5,
       "status":"completed","cpu_pct":140}]
for b in bad: print("validate", b.get("event_id"), "->", validate(b))
