"""Shared, broker-independent logic: schema validation, aggregation, 5 s event-time windows."""
from collections import defaultdict
from datetime import datetime, timedelta

EVENT_DATE = "2026-11-01"          # synthetic date prepended to HH:MM:SS timestamps
WINDOW_SEC = 5
JOB_TYPES = {"simulation", "ml", "image", "text"}
STATUSES = {"completed", "failed"}
SCHEMA = {"event_id": str, "timestamp": str, "research_group": str, "job_type": str,
          "duration_sec": (int, float), "status": str, "cpu_pct": (int, float)}


def validate(ev):
    """Return (ok, reason). Checks presence, types, enums and value ranges."""
    if not isinstance(ev, dict):
        return False, "not an object"
    for f, t in SCHEMA.items():
        if f not in ev:
            return False, f"missing field {f}"
        if isinstance(ev[f], bool) or not isinstance(ev[f], t):
            return False, f"bad type for {f}"
    if ev["job_type"] not in JOB_TYPES:
        return False, "unknown job_type"
    if ev["status"] not in STATUSES:
        return False, "unknown status"
    if ev["duration_sec"] < 0 or not 0 <= ev["cpu_pct"] <= 100:
        return False, "value out of range"
    try:
        event_time(ev)
    except ValueError:
        return False, "bad timestamp"
    return True, "ok"


def event_time(ev):
    ts = ev["timestamp"]
    if "T" not in ts and " " not in ts:
        ts = f"{EVENT_DATE}T{ts}"
    return datetime.fromisoformat(ts.replace(" ", "T"))


def window_start(t, size=WINDOW_SEC):
    """Tumbling window [start, start+size): start is inclusive, end exclusive."""
    epoch = int(t.timestamp()) if t.tzinfo else int((t - datetime(1970, 1, 1)).total_seconds())
    return datetime(1970, 1, 1) + timedelta(seconds=epoch - epoch % size)


class Aggregator:
    def __init__(self):
        self.by_type = defaultdict(lambda: {"completed": 0, "failed": 0, "dur_sum": 0.0, "n": 0})
        self.by_group = defaultdict(lambda: {"cpu_sum": 0.0, "n": 0})
        self.windows = defaultdict(lambda: {"ids": [], "failed": 0, "cpu_sum": 0.0})
        self.status = {"completed": 0, "failed": 0}
        self.invalid = 0

    def add(self, ev):
        ok, _ = validate(ev)
        if not ok:
            self.invalid += 1
            return False
        self.status[ev["status"]] += 1
        t = self.by_type[ev["job_type"]]
        t[ev["status"]] += 1; t["dur_sum"] += ev["duration_sec"]; t["n"] += 1
        g = self.by_group[ev["research_group"]]
        g["cpu_sum"] += ev["cpu_pct"]; g["n"] += 1
        w = self.windows[window_start(event_time(ev))]
        w["ids"].append(ev["event_id"]); w["cpu_sum"] += ev["cpu_pct"]
        w["failed"] += ev["status"] == "failed"
        return True

    def type_table(self):
        return {k: {"completed": v["completed"], "failed": v["failed"],
                    "avg_duration": round(v["dur_sum"] / v["n"], 2)} for k, v in sorted(self.by_type.items())}

    def group_cpu(self):
        return {k: round(v["cpu_sum"] / v["n"], 2) for k, v in sorted(self.by_group.items())}

    def window_table(self):
        out = []
        for s in sorted(self.windows):
            w = self.windows[s]; n = len(w["ids"])
            out.append({"window": f"{s:%H:%M:%S}-{s + timedelta(seconds=WINDOW_SEC):%H:%M:%S}",
                        "events": ",".join(w["ids"]), "count": n,
                        "failure_rate": round(w["failed"] / n, 4), "avg_cpu": round(w["cpu_sum"] / n, 2)})
        return out
