#!/bin/bash

# This script runs the kubelet_monitor.go every N seconds in a loop.
# It builds and executes the Go program in-memory without installing.

MONITOR_INTERVAL=1  # Adjust this to change how often it runs (in seconds)

while true; do
    echo "Running kubelet_monitor.go..."
    go run kube-metrics.go
    echo "Sleeping for $MONITOR_INTERVAL seconds..."
    sleep $MONITOR_INTERVAL
    if [ -f stop_monitor ]; then
      echo "Stop signal detected."
      break
    fi
done
