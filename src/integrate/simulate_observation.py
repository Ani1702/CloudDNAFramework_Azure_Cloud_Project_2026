"""
Simulates Member 1's Stream A telemetry emitter for local and cloud testing.
"""
import sys
import time
import json
import urllib.request
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

# Ensure project root is in sys.path so 'src' imports work regardless of working directory
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
# Add decision service to sys.path so it can find its own modules (like 'models', 'trigger', etc.)
decision_dir = project_root / "src" / "services" / "decision"
sys.path.insert(0, str(decision_dir))

from src.services.decision.function_app import process_stream_a_window


def generate_stream_a_window(
    app_id: str = "medusa",
    profile: str = "steady",  # steady | spike | degradation | saturation | security | recovery
    rps_override: float = None,
) -> Dict[str, Any]:
    now_iso = datetime.utcnow().isoformat() + "Z"

    base = {
        "app_id": app_id,
        "ts": now_iso,
        "window_sec": 10,
        "performance": {
            "p50_latency_ms": 150.0,
            "p95_latency_ms": 220.0,
            "p99_latency_ms": 280.0,
            "throughput_rps": 100.0 if rps_override is None else rps_override,
            "error_rate_pct": 0.1,
            "cpu_utilization_pct": 35.0,
            "memory_utilization_pct": 40.0,
            "queue_depth": 2.0,
        },
        "cost": {"hourly_compute_cost_usd": 0.09},
        "security": {"failed_auth_attempts_per_min": 0.0, "anomalous_ip_request_rate": 0.0},
        "scaling": {"current_replica_count": 2, "cold_start_time_ms": 0.0},
        "recovery": {"restart_count": 0, "health_check_failure_rate_pct": 0.0},
        "current_production_config": {
            "cpu_cores": 1.0,
            "memory_gb": 1.0,
            "replica_count": 2,
            "node_thread_pool_size": 20,
            "redis_cache_enabled": False,
            "redis_cache_ttl_seconds": 0,
            "rate_limiting_enabled": False,
            "health_check_timeout_sec": 2,
        },
    }

    if profile == "spike":
        base["performance"]["throughput_rps"] = 480.0
        base["performance"]["p95_latency_ms"] = 1050.0
        base["performance"]["cpu_utilization_pct"] = 88.0
        base["performance"]["queue_depth"] = 28.0
        base["cost"]["hourly_compute_cost_usd"] = 0.18
        base["scaling"]["current_replica_count"] = 4
    elif profile == "degradation":
        base["performance"]["p95_latency_ms"] = 1150.0
        base["performance"]["error_rate_pct"] = 6.5
    elif profile == "saturation":
        base["performance"]["cpu_utilization_pct"] = 96.0
        base["performance"]["memory_utilization_pct"] = 92.0
        base["performance"]["throughput_rps"] = 95.0
    elif profile == "security":
        base["security"]["failed_auth_attempts_per_min"] = 35.0
        base["security"]["anomalous_ip_request_rate"] = 12.0
    elif profile == "recovery":
        base["recovery"]["restart_count"] = 4
        base["recovery"]["health_check_failure_rate_pct"] = 25.0

    return base


def run_simulation(endpoint_url: str = None, profile: str = "spike", cycles: int = 3):
    app_id = f"medusa_{profile}"
    print(f"Starting Stream A simulation [Profile: {profile.upper()}] for App: {app_id}...")
    
    print("\n--- Establishing Baseline (Steady) ---")
    for i in range(1, 4):
        steady_window = generate_stream_a_window(app_id=app_id, profile="steady")
        if endpoint_url:
            req = urllib.request.Request(
                endpoint_url,
                data=json.dumps(steady_window).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req).read()
        else:
            process_stream_a_window(steady_window)
        print(f"Sent steady baseline window {i}/3")
        time.sleep(0.1)

    print("\n--- Injecting Anomalous Profile ---")
    for i in range(1, cycles + 1):
        window = generate_stream_a_window(app_id=app_id, profile=profile)
        print(f"\n[Cycle {i}/{cycles}] Window at {window['ts']}:")
        
        if endpoint_url:
            req = urllib.request.Request(
                endpoint_url,
                data=json.dumps(window).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read().decode())
                print("HTTP Response:", json.dumps(result, indent=2))
        else:
            result = process_stream_a_window(window)
            print("Direct Ingestion Result:", json.dumps(result, indent=2))
            
        time.sleep(0.5)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Simulate Stream A telemetry")
    parser.add_argument("--profile", type=str, default="spike", help="Profile to run (e.g. steady, spike, degradation, etc.)")
    parser.add_argument("--url", type=str, default=None, help="Endpoint URL for the decision service")
    parser.add_argument("--cycles", type=int, default=3, help="Number of cycles to run")
    args = parser.parse_args()

    profiles_to_test = ["spike", "degradation", "saturation", "security", "recovery"]
    
    if args.profile == "all":
        print("Testing ALL profiles sequentially...")
        for p in profiles_to_test:
            run_simulation(endpoint_url=args.url, profile=p, cycles=args.cycles)
            print("\n" + "="*60 + "\n")
            time.sleep(1)
    else:
        run_simulation(endpoint_url=args.url, profile=args.profile, cycles=args.cycles)