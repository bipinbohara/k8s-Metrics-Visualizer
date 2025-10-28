import redis
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import matplotlib.dates as mdates
import re

import csv

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Example key
key = "util:gpt120b-vllm-5675bf8ccb-bs565:gpt120b-vllm"

# Fetch all data
raw_data = r.zrange(key, 0, -1, withscores=True)

# Parse into time-series
timestamps = []
cpu_cores = []
csv_rows = []

for entry, score in raw_data:
    # Score is a float timestamp (milliseconds since epoch)
    dt = datetime.fromtimestamp(score / 1000, tz=timezone.utc)
    timestamps.append(dt)

    # Extract cpu_cores from the string
    match = re.search(r'cpu_cores=([\d.]+)', entry)
    if match:
        cpu_cores.append(float(match.group(1)))

    # Save all fields
    row = {
        "timestamp_utc": dt.isoformat(),
        "cpu_cores": float(re.search(r'cpu_cores=([\d.]+)', entry).group(1)),
        "cpu_time_sec": float(re.search(r'cpu_time_sec=([\d.]+)', entry).group(1)),
        "mem_usage_kb": float(re.search(r'mem_usage_kb=([\d.]+)', entry).group(1)),
        "mem_working_kb": float(re.search(r'mem_working_kb=([\d.]+)', entry).group(1)),
        "rss_kb": float(re.search(r'rss_kb=([\d.]+)', entry).group(1)),
        "logs_bytes": float(re.search(r'logs_bytes=([\d.]+)', entry).group(1)),
        "rootfs_bytes": float(re.search(r'rootfs_bytes=([\d.]+)', entry).group(1))
    }
    csv_rows.append(row)

# Save to CSV
csv_file = "cpu_metrics.csv"
with open(csv_file, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"Data saved to {csv_file}")

# Plot
plt.figure(figsize=(12, 6))
plt.plot(timestamps, cpu_cores, marker='o')
plt.xlabel("Time (UTC)")
plt.ylabel("CPU Cores")
plt.title("CPU Usage Over Time")
plt.grid(True)

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S UTC'))
plt.gcf().autofmt_xdate()

plt.tight_layout()
plt.savefig('cpu_usage.png')
#plt.show()

timestamps = []
cpu_cores = []

for entry, score in raw_data:
    match = re.search(r'cpu_cores=([\d.]+)', entry)
    if match:
        cpu_val = float(match.group(1))
        if cpu_val >= 1.0:
            dt = datetime.fromtimestamp(score / 1000)
            timestamps.append(dt)
            cpu_cores.append(cpu_val)

plt.figure(figsize=(12, 8))
plt.plot(timestamps, cpu_cores, marker='x', linestyle='None')
plt.xlabel("Time (UTC)")
plt.ylabel("CPU Cores")
plt.title("CPU Usage Over Time (CPU ≥ 1)")
plt.grid(True)

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S UTC'))
plt.gcf().autofmt_xdate()

plt.tight_layout()
plt.savefig('integer-CPU.png')
