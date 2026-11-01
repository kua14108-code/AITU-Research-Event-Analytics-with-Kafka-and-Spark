import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
fig, ax = plt.subplots(figsize=(13, 6.2)); ax.set_xlim(0, 13); ax.set_ylim(0, 6.2); ax.axis("off")
def box(x, y, w, h, t, c, fs=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04", fc=c, ec="#333", lw=1.2))
    ax.text(x + w / 2, y + h / 2, t, ha="center", va="center", fontsize=fs)
def arrow(a, b, t=""):
    ax.annotate("", xy=b, xytext=a, arrowprops=dict(arrowstyle="->", lw=1.5, color="#222"))
    if t: ax.text((a[0]+b[0])/2, (a[1]+b[1])/2 + .12, t, fontsize=7.5, ha="center", color="#444")
for i, g in enumerate(["G01", "G02", "G03"]):
    box(0.2, 4.3 - i * 1.5, 1.7, 1.0, f"Producer\n(research group {g})", "#cfe8ff")
ax.text(1.05, 5.7, "Producers", ha="center", fontweight="bold")
box(3.2, 1.0, 2.6, 4.3, "", "#fff2cc"); ax.text(4.5, 5.45, "Kafka topic aitu.research.jobs", ha="center", fontweight="bold", fontsize=9)
for i in range(3):
    box(3.45, 4.1 - i * 1.3 + (0.0), 2.1, 0.95, f"Partition {i}\nkey = research_group", "#ffe599", 8.5)
for i in range(3): arrow((1.95, 4.8 - i * 1.5), (3.4, 4.55 - i * 1.3), "key" if i == 0 else "")
ax.text(4.5, 1.15, "RF=3 in production, acks=all", ha="center", fontsize=8, style="italic")
box(6.6, 0.9, 2.4, 4.4, "", "#d9ead3"); ax.text(7.8, 5.45, "Consumer group aitu-analytics", ha="center", fontweight="bold", fontsize=9)
for i in range(3):
    box(6.8, 4.1 - i * 1.3, 2.0, 0.95, f"Consumer {i}\nPython processor\nvalidate + 5 s window", "#b6d7a8", 8)
    arrow((5.6, 4.55 - i * 1.3), (6.75, 4.55 - i * 1.3))
box(9.8, 3.6, 2.8, 1.5, "Aggregation layer\ncounts, avg duration,\navg CPU, window state", "#ead1dc")
box(9.8, 2.0, 2.8, 1.1, "Monitoring: consumer lag,\nfailure rate, CPU, latency", "#d0e0e3", 8.5)
box(9.8, 0.2, 2.8, 1.3, "Spark Structured Streaming\nwatermark 10 s, window 5 s\n(window, job_type, count,\navg_duration)", "#f4cccc", 8.5)
for i in range(3): arrow((8.85, 4.55 - i * 1.3), (9.75, 4.4))
arrow((11.2, 3.55), (11.2, 3.15)); arrow((11.2, 1.55), (11.2, 1.95))
ax.plot([5.0, 5.0, 9.2], [1.0, 0.5, 0.5], color="#222", lw=1.5); arrow((9.2, 0.5), (9.75, 0.5))
ax.text(7.0, 0.62, "Spark reads same topic (own consumer group)", fontsize=7.5, ha="center", color="#444")
ax.text(6.5, 6.0, "Kafka-based streaming architecture for AITU research-job events (synthetic)", ha="center", fontsize=11, fontweight="bold")
plt.savefig("architecture.png", dpi=170, bbox_inches="tight")
