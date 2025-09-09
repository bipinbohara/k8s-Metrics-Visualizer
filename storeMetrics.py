import os, re, csv
from datetime import datetime, timezone
import redis

# ---- Config (env-overridable) ----
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))
KEY_GLOB   = os.getenv("KEY_GLOB", "util:*")       # scan pattern
CSV_FILE   = os.getenv("CSV_FILE", "cpu_metrics.csv")

# Float regex (handles 1.23, 1e-3, etc.)
NUM = r"([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)"
PAT = {
    "cpu_cores":      re.compile(rf"cpu_cores={NUM}"),
    "cpu_time_sec":   re.compile(rf"cpu_time_sec={NUM}"),
    "mem_usage_kb":   re.compile(rf"mem_usage_kb={NUM}"),
    "mem_working_kb": re.compile(rf"mem_working_kb={NUM}"),
    "rss_kb":         re.compile(rf"rss_kb={NUM}"),
    "logs_bytes":     re.compile(rf"logs_bytes={NUM}"),
    "rootfs_bytes":   re.compile(rf"rootfs_bytes={NUM}"),
}

def parse_metrics(s: str):
    out = {}
    for name, rx in PAT.items():
        m = rx.search(s)
        out[name] = float(m.group(1)) if m else None
    return out

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    fieldnames = ["key","pod","container","timestamp_ms","timestamp_utc"] + list(PAT.keys())
    rows_written = 0

    # Stream directly to CSV so we don't hold everything in RAM
    with open(CSV_FILE, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for key in r.scan_iter(KEY_GLOB):
            # Expect util:<pod>:<container>
            parts = key.split(":", 2)
            pod, container = (parts[1], parts[2]) if len(parts) == 3 else ("", "")

            for entry, score in r.zrange(key, 0, -1, withscores=True):
                ts_ms = int(score)
                ts_iso = datetime.fromtimestamp(ts_ms/1000.0, tz=timezone.utc).isoformat()
                vals = parse_metrics(entry)
                w.writerow({
                    "key": key,
                    "pod": pod,
                    "container": container,
                    "timestamp_ms": ts_ms,
                    "timestamp_utc": ts_iso,
                    **vals
                })
                rows_written += 1

    print(f"[ok] Wrote {rows_written} rows to {CSV_FILE} (pattern: {KEY_GLOB})")

if __name__ == "__main__":
    main()
