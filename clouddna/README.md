# CloudDNA Telemetry Stream Processor (Stream A)

Stream A consumes mirrored HTTP traffic from the `traffic-mirror` Azure Event Hub, aggregates metrics into **10-second tumbling windows (`window_sec: 10`)**, and publishes structured telemetry arrays to the egress Event Hub (`cdna-telemetry`) for CloudDNA fitness scoring and autoscaling decisions.

## Architecture

```
[ Medusa API Pods ] ---> [ Sidecar Proxy (Port 9001) ]
                                   │
                                   ▼
                       [ Azure Event Hub: traffic-mirror ]
                                   │
                                   ▼
                 [ Azure Function: Stream A (EventHubTrigger) ]
                       - Ingests raw batch
                       - Buckets by 10s timestamp windows
                       - Computes 5 fitness pillars:
                           • performance (p50, p95, p99, RPS, error %, CPU, Mem, queue)
                           • cost (hourly compute cost)
                           • security (failed auth/min, anomalous IP rate)
                           • scaling (replica count, cold start ms)
                           • recovery (restart count, health check fail %)
                                   │
                                   ▼
                       [ Azure Event Hub: cdna-telemetry ]
```

## JSON Telemetry Schema

The output emitted to Event Hub strictly complies with [`clouddna/schema/telemetry-window.schema.json`](./schema/telemetry-window.schema.json):

```json
[
  {
    "app_id": "medusa",
    "ts": "2026-08-22T20:00:00Z",
    "window_sec": 10,
    "performance": {
      "p50_latency_ms": 420,
      "p95_latency_ms": 1380,
      "p99_latency_ms": 2100,
      "throughput_rps": 480,
      "error_rate_pct": 5.4,
      "cpu_utilization_pct": 94,
      "memory_utilization_pct": 78,
      "queue_depth": 142
    },
    "cost": {
      "hourly_compute_cost_usd": 0.18
    },
    "security": {
      "failed_auth_attempts_per_min": 0,
      "anomalous_ip_request_rate": 0.0
    },
    "scaling": {
      "current_replica_count": 2,
      "cold_start_time_ms": 3200
    },
    "recovery": {
      "restart_count": 0,
      "health_check_failure_rate_pct": 0.0
    }
  }
]
```

## Running the Verification Test

You can run the simulated workload test at any time:

```bash
cd clouddna/functions/stream-a
node test-stream.js
```
