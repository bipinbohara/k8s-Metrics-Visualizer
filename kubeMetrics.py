import json
import os
import time
from typing import Any, Dict, List

import requests
import redis

# -------- Defaults (override via env vars) --------
KUBELET_HOST = os.getenv("KUBELET_HOST", "localhost")
KUBELET_PORT = os.getenv("KUBELET_PORT", "8001")
REDIS_HOST   = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB     = int(os.getenv("REDIS_DB", "0"))
REDIS_PASS   = os.getenv("REDIS_PASSWORD")  # optional
POLL_MS      = int(os.getenv("POLL_INTERVAL_MS", "100"))
NS_FILTER    = os.getenv("NAMESPACE_FILTER", "default")

# Comma-separated node names (required)
NODES_ENV    = os.getenv("NODE_NAMES", "")

session = requests.Session()


def fetch_summary(node: str) -> Dict[str, Any]:
    """GET kubelet stats/summary for a node via apiserver proxy."""
    url = f"http://{KUBELET_HOST}:{KUBELET_PORT}/api/v1/nodes/{node}/proxy/stats/summary"
    resp = session.get(url, timeout=5)
    resp.raise_for_status()
    return resp.json()


def format_member(data: Dict[str, float]) -> str:
    """Create the Redis ZSET member payload string."""
    return (
        "cpu_cores={cpu_cores:.6f}, cpu_time_sec={cpu_time_sec:.2f}, "
        "mem_usage_kb={mem_usage_kb:.2f}, mem_working_kb={mem_working_kb:.2f}, "
        "rss_kb={rss_kb:.2f}, logs_bytes={logs_bytes:.0f}, rootfs_bytes={rootfs_bytes:.0f}"
    ).format(**data)


def log_to_redis(rdb: redis.Redis, pod_name: str, container_name: str, data: Dict[str, float]) -> None:
    ts_ms = int(time.time() * 1000)
    key = f"util:{pod_name}:{container_name}"
    member = format_member(data)
    print(f"Logging to Redis: {key} => {member}")
    # redis-py expects mapping {member: score}
    rdb.zadd(key, {member: float(ts_ms)})


def extract_float(d: Dict[str, Any], *keys: str) -> float:
    """Safely get a nested numeric field or 0.0 if missing."""
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return 0.0
        cur = cur[k]
    try:
        return float(cur)
    except (TypeError, ValueError):
        return 0.0


def process_node(rdb: redis.Redis, node: str) -> None:
    try:
        summary = fetch_summary(node)
    except requests.RequestException as e:
        print(f"Error fetching summary for {node}: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"JSON decode error for {node}: {e}")
        return

    pods: List[Dict[str, Any]] = summary.get("pods", [])
    for pod in pods:
        pref = pod.get("podRef", {}) or {}
        pod_ns = pref.get("namespace", "")
        pod_name = pref.get("name", "")
        if pod_ns != NS_FILTER:
            continue

        for c in pod.get("containers", []) or []:
            cpu_cores = extract_float(c, "cpu", "usageNanoCores") / 1e9
            cpu_time_sec = extract_float(c, "cpu", "usageCoreNanoSeconds") / 1e9
            mem_usage_kb = extract_float(c, "memory", "usageBytes") / 1024.0
            mem_working_kb = extract_float(c, "memory", "workingSetBytes") / 1024.0
            rss_kb = extract_float(c, "memory", "rssBytes") / 1024.0
            logs_bytes = extract_float(c, "logs", "usedBytes")
            rootfs_bytes = extract_float(c, "rootfs", "usedBytes")

            data = {
                "cpu_cores": cpu_cores,
                "cpu_time_sec": cpu_time_sec,
                "mem_usage_kb": mem_usage_kb,
                "mem_working_kb": mem_working_kb,
                "rss_kb": rss_kb,
                "logs_bytes": logs_bytes,
                "rootfs_bytes": rootfs_bytes,
            }
            container_name = c.get("name", "unknown")
            log_to_redis(rdb, pod_name, container_name, data)


def main() -> None:
    nodes_env = NODES_ENV.strip()
    if not nodes_env:
        raise SystemExit(
            "Set NODE_NAMES env var to a comma-separated list of node names "
            "(e.g., NODE_NAMES='node1,node2')."
        )
    nodes = [n.strip() for n in nodes_env.split(",") if n.strip()]

    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASS)
    # Optional connectivity check
    try:
        rdb.ping()
    except redis.ConnectionError as e:
        raise SystemExit(f"Unable to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")

    poll_s = POLL_MS / 1000.0
    next_tick = time.time()
    while True:
        start = time.time()
        for node in nodes:
            process_node(rdb, node)
        print(f"Loop duration: {time.time() - start:.6f}s")

        next_tick += poll_s
        sleep_for = max(0.0, next_tick - time.time())
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
