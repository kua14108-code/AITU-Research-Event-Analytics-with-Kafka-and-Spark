"""Synthetic load test of the Python processor. Broker stand-in: 3 in-process partition queues
(key=research_group hash -> partition), 1 consumer thread per partition (= consumer group of 3).
Every event is JSON-serialised by the producer and parsed + validated + windowed by the consumer.
Latency = time from send() to end of processing. CPU = process CPU time / wall time (psutil)."""
import json, queue, random, threading, time, statistics, sys, psutil
from datetime import datetime
from stream_logic import Aggregator

P = 3
GROUPS = ["G01", "G02", "G03"]; TYPES = ["simulation", "ml", "image", "text"]

def run(rate, seconds):
    qs = [queue.Queue() for _ in range(P)]
    lat, lock, agg = [], threading.Lock(), Aggregator()
    done = threading.Event(); processed = [0]
    def consumer(q):
        local = []
        while not (done.is_set() and q.empty()):
            try: t0, raw = q.get(timeout=0.05)
            except queue.Empty: continue
            ev = json.loads(raw)
            with lock: agg.add(ev); processed[0] += 1
            local.append((time.perf_counter() - t0) * 1000)
        with lock: lat.extend(local)
    ths = [threading.Thread(target=consumer, args=(q,)) for q in qs]
    [t.start() for t in ths]
    proc = psutil.Process(); proc.cpu_percent(None); c0 = sum(proc.cpu_times()[:2]); w0 = time.perf_counter()
    n = rate * seconds; sent = 0; start = time.perf_counter()
    while sent < n:
        due = start + sent / rate
        now = time.perf_counter()
        if now < due: time.sleep(min(due - now, 0.002)); continue
        g = random.choice(GROUPS)
        ev = {"event_id": f"L{sent}", "timestamp": datetime.now().isoformat(timespec="seconds"), "research_group": g,
              "job_type": random.choice(TYPES), "duration_sec": random.randint(10, 120),
              "status": random.choice(["completed"] * 4 + ["failed"]), "cpu_pct": random.randint(30, 99)}
        qs[hash(g) % P].put((time.perf_counter(), json.dumps(ev))); sent += 1
    send_end = time.perf_counter()
    done.set(); [t.join() for t in ths]
    wall = time.perf_counter() - w0; cpu = sum(proc.cpu_times()[:2]) - c0
    return {"input_rate": rate, "sent": sent, "processed": processed[0],
            "throughput": round(processed[0] / (send_end - start), 1),
            "avg_latency_ms": round(statistics.mean(lat), 2), "p95_latency_ms": round(sorted(lat)[int(.95 * len(lat))], 2),
            "max_latency_ms": round(max(lat), 2), "cpu_pct": round(100 * cpu / wall, 1)}

if __name__ == "__main__":
    out = []
    for rate, sec in [(10, 20), (100, 20), (500, 20), (5000, 10), (20000, 10)]:
        r = run(rate, sec); out.append(r); print(r, flush=True)
    json.dump(out, open("logs/load_python.json", "w"), indent=1)
