package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "os"
    "strings"
    "time"

    "github.com/go-redis/redis/v8"
    "golang.org/x/net/context"
)

const (
    kubeletHost     = "localhost"
    kubeletPort     = "8001"
    redisHost       = "localhost"
    redisPort       = "6379"
    pollIntervalMs = 100
    namespaceFilter = "default"
)

var ctx = context.Background()

type ContainerStats struct {
    Name   string `json:"name"`
    CPU    struct {
        UsageNanoCores      int64 `json:"usageNanoCores"`
        UsageCoreNanoSeconds int64 `json:"usageCoreNanoSeconds"`
    } `json:"cpu"`
    Memory struct {
        UsageBytes      int64 `json:"usageBytes"`
        WorkingSetBytes int64 `json:"workingSetBytes"`
        RSSBytes        int64 `json:"rssBytes"`
    } `json:"memory"`
    Logs struct {
        UsedBytes int64 `json:"usedBytes"`
    } `json:"logs"`
    Rootfs struct {
        UsedBytes int64 `json:"usedBytes"`
    } `json:"rootfs"`
}

type PodStats struct {
    PodRef struct {
        Name      string `json:"name"`
        Namespace string `json:"namespace"`
    } `json:"podRef"`
    Containers []ContainerStats `json:"containers"`
}

type Summary struct {
    Pods []PodStats `json:"pods"`
}

func fetchSummary(node string) (*Summary, error) {
    url := fmt.Sprintf("http://%s:%s/api/v1/nodes/%s/proxy/stats/summary", kubeletHost, kubeletPort, node)
    resp, err := http.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    body, _ := ioutil.ReadAll(resp.Body)

    var summary Summary
    err = json.Unmarshal(body, &summary)
    if err != nil {
        return nil, err
    }

    return &summary, nil
}

func logToRedis(rdb *redis.Client, podName, containerName string, data map[string]float64) {
    timestamp := time.Now().UnixMilli()
    key := fmt.Sprintf("util:%s:%s", podName, containerName)
    value := fmt.Sprintf("cpu_cores=%.6f, cpu_time_sec=%.2f, mem_usage_kb=%.2f, mem_working_kb=%.2f, rss_kb=%.2f, logs_bytes=%.0f, rootfs_bytes=%.0f",
        data["cpu_cores"], data["cpu_time_sec"], data["mem_usage_kb"], data["mem_working_kb"],
        data["rss_kb"], data["logs_bytes"], data["rootfs_bytes"])
    //rdb.ZAdd(ctx, key, &redis.Z{Score: float64(timestamp), Member: value})
    fmt.Printf("Logging to Redis: %s => %s\n", key, value)

    err := rdb.ZAdd(ctx, key, &redis.Z{Score: float64(timestamp), Member: value}).Err()
    if err != nil {
        fmt.Println("Redis error:", err)
    }
}

func main() {
    rdb := redis.NewClient(&redis.Options{
        Addr: fmt.Sprintf("%s:%s", redisHost, redisPort),
    })

    nodes := os.Getenv("NODE_NAMES") // comma-separated list of node names
    nodeList := strings.Split(nodes, ",")

    ticker := time.NewTicker(time.Duration(pollIntervalMs) * time.Millisecond)
    defer ticker.Stop()

    for range ticker.C {
        start := time.Now()

        for _, node := range nodeList {
            summary, err := fetchSummary(strings.TrimSpace(node))
            if err != nil {
                fmt.Println("Error fetching summary for", node, ":", err)
                continue
            }

            for _, pod := range summary.Pods {
                if pod.PodRef.Namespace == namespaceFilter {
                    for _, c := range pod.Containers {
                        data := map[string]float64{
                            "cpu_cores":     float64(c.CPU.UsageNanoCores) / 1e9,
                            "cpu_time_sec":  float64(c.CPU.UsageCoreNanoSeconds) / 1e9,
                            "mem_usage_kb":  float64(c.Memory.UsageBytes) / 1024.0,
                            "mem_working_kb": float64(c.Memory.WorkingSetBytes) / 1024.0,
                            "rss_kb":        float64(c.Memory.RSSBytes) / 1024.0,
                            "logs_bytes":    float64(c.Logs.UsedBytes),
                            "rootfs_bytes":  float64(c.Rootfs.UsedBytes),
                        }
                        logToRedis(rdb, pod.PodRef.Name, c.Name, data)
                    }
                }
            }
        }

        fmt.Println("Loop duration:", time.Since(start))
    }
}
