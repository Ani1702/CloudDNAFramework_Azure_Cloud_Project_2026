"""
Traffic Dispatcher - Experimentation Layer
Member 2 - Cloud DNA Framework

This module handles:
1. Retrieving buffered traffic from Event Hub
2. Dispatching identical traffic to all candidate deployments
3. Synthetic security probe generation and dispatch
4. Collecting response metrics from all candidates
"""

import json
import logging
import asyncio
import time
import aiohttp
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrafficDispatcher:
    """
    Dispatches traffic to candidate deployments for evaluation
    """
    
    def __init__(
        self,
        event_hub_connection_string: str = None,
        event_hub_name: str = "medusa-traffic-mirror"
    ):
        """Initialize traffic dispatcher with Event Hub connection"""
        self.event_hub_name = event_hub_name
        self.event_hub_connection_string = event_hub_connection_string
        
        # Traffic buffer for replay
        self.traffic_buffer = []
        self.security_probes = self._generate_security_probes()
        
        logger.info("Traffic dispatcher initialized")
    
    async def collect_buffered_traffic(
        self,
        buffer_window_seconds: int = 60,
        max_requests: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Collect buffered traffic from Event Hub for replay
        
        Args:
            buffer_window_seconds: Time window to collect traffic from
            max_requests: Maximum number of requests to collect
            
        Returns:
            List of traffic events for replay
        """
        logger.info(f"Collecting buffered traffic from last {buffer_window_seconds} seconds")
        
        if not self.event_hub_connection_string:
            logger.warning("No Event Hub connection string provided, generating synthetic traffic")
            return self._generate_synthetic_traffic(max_requests)
        
        try:
            # Create Event Hub consumer client
            consumer_client = EventHubConsumerClient.from_connection_string(
                self.event_hub_connection_string,
                consumer_group="$Default",
                eventhub_name=self.event_hub_name
            )
            
            traffic_events = []
            cutoff_time = datetime.utcnow() - timedelta(seconds=buffer_window_seconds)
            
            # This is a simplified version - in production you'd need proper partition handling
            async with consumer_client:
                async for partition in consumer_client.get_partition_ids():
                    async for event in consumer_client.receive_from_partition(
                        partition_id=partition,
                        starting_position="-1",  # Start from latest
                        max_batch_size=100
                    ):
                        event_data = json.loads(event.body_as_str())
                        event_time = datetime.fromisoformat(event_data.get('ts', '').replace('Z', '+00:00'))
                        
                        if event_time >= cutoff_time:
                            traffic_events.append(event_data)
                            
                        if len(traffic_events) >= max_requests:
                            break
                    
                    if len(traffic_events) >= max_requests:
                        break
            
            logger.info(f"Collected {len(traffic_events)} traffic events for replay")
            return traffic_events
            
        except Exception as e:
            logger.error(f"Failed to collect buffered traffic: {e}")
            logger.info("Falling back to synthetic traffic generation")
            return self._generate_synthetic_traffic(max_requests)
    
    def _generate_synthetic_traffic(self, max_requests: int) -> List[Dict[str, Any]]:
        """Generate synthetic traffic for testing when Event Hub is unavailable"""
        
        # Common Medusa API endpoints based on the storefront URLs provided
        endpoints = [
            {"method": "GET", "path": "/store/products", "weight": 0.3},
            {"method": "GET", "path": "/store/collections", "weight": 0.2},
            {"method": "GET", "path": "/store/regions", "weight": 0.1},
            {"method": "POST", "path": "/store/carts", "weight": 0.15},
            {"method": "GET", "path": "/store/carts/{id}", "weight": 0.1},
            {"method": "POST", "path": "/store/auth", "weight": 0.05},
            {"method": "GET", "path": "/store/customers/me", "weight": 0.05},
            {"method": "POST", "path": "/store/customers", "weight": 0.03},
            {"method": "GET", "path": "/admin/products", "weight": 0.02}
        ]
        
        traffic_events = []
        
        for i in range(min(max_requests, 100)):  # Limit synthetic traffic
            # Select endpoint based on weights
            weights = [ep["weight"] for ep in endpoints]
            endpoint = np.random.choice(endpoints, p=weights)
            
            # Generate realistic latency and status codes
            if np.random.random() < 0.95:  # 95% success rate
                status_code = 200
                latency_ms = np.random.lognormal(mean=4.5, sigma=0.8)  # ~90ms average
            else:
                status_code = np.random.choice([400, 404, 500], p=[0.6, 0.3, 0.1])
                latency_ms = np.random.lognormal(mean=3.0, sigma=1.0)  # Faster for errors
            
            event = {
                "app_id": "medusa",
                "ts": (datetime.utcnow() - timedelta(seconds=np.random.randint(0, 60))).isoformat() + "Z",
                "method": endpoint["method"],
                "path": endpoint["path"],
                "status_code": status_code,
                "latency_ms": max(10, int(latency_ms)),  # Minimum 10ms
                "headers_hash": hashlib.md5(f"request-{i}".encode()).hexdigest()[:8],
                "body_size_bytes": np.random.randint(100, 5000)
            }
            
            traffic_events.append(event)
        
        logger.info(f"Generated {len(traffic_events)} synthetic traffic events")
        return traffic_events
    
    def _generate_security_probes(self) -> List[Dict[str, Any]]:
        """Generate security probe requests to test candidate resilience"""
        
        probes = [
            # SQL Injection attempts
            {
                "name": "sql_injection",
                "method": "GET",
                "path": "/store/products",
                "params": {"search": "'; DROP TABLE products; --"},
                "expected_status": [400, 403]
            },
            # XSS attempts
            {
                "name": "xss_attempt", 
                "method": "GET",
                "path": "/store/products",
                "params": {"search": "<script>alert('xss')</script>"},
                "expected_status": [400, 403]
            },
            # Path traversal
            {
                "name": "path_traversal",
                "method": "GET", 
                "path": "/store/../../etc/passwd",
                "expected_status": [400, 403, 404]
            },
            # Authentication brute force (multiple rapid requests)
            {
                "name": "auth_bruteforce",
                "method": "POST",
                "path": "/store/auth",
                "body": {"email": "admin@test.com", "password": "wrong123"},
                "repeat": 10,
                "expected_status": [401, 429]
            },
            # Large payload attack
            {
                "name": "large_payload",
                "method": "POST",
                "path": "/store/carts",
                "body": {"items": [{"id": str(i)} for i in range(1000)]},  # Large payload
                "expected_status": [400, 413]
            }
        ]
        
        return probes
    
    async def dispatch_traffic_to_candidates(
        self,
        candidates: List[Dict[str, Any]],
        traffic_events: List[Dict[str, Any]],
        evaluation_window_sec: int = 90
    ) -> Dict[str, Dict[str, Any]]:
        """
        Dispatch traffic to all candidates simultaneously and collect metrics
        
        Args:
            candidates: List of candidate deployment information
            traffic_events: Buffered traffic to replay
            evaluation_window_sec: How long to run the evaluation
            
        Returns:
            Dict mapping candidate_id to collected metrics
        """
        logger.info(f"Dispatching traffic to {len(candidates)} candidates for {evaluation_window_sec}s")
        
        # Prepare candidate endpoints
        candidate_urls = {}
        for candidate in candidates:
            if candidate.get('status') == 'ready':
                candidate_urls[candidate['candidate_id']] = candidate.get('service_url', 
                    f"http://medusa-{candidate['candidate_id'].lower()}.sandbox-{candidate['candidate_id'].lower()}.svc.cluster.local")
        
        if not candidate_urls:
            logger.error("No ready candidates found for traffic dispatch")
            return {}
        
        # Initialize metrics collection
        metrics = {cid: {
            "latencies": [],
            "status_codes": [],
            "error_count": 0,
            "total_requests": 0,
            "security_probe_results": {}
        } for cid in candidate_urls.keys()}
        
        # Create aiohttp session
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            # Dispatch regular traffic
            await self._dispatch_regular_traffic(
                session, candidate_urls, traffic_events, metrics, evaluation_window_sec
            )
            
            # Dispatch security probes
            await self._dispatch_security_probes(
                session, candidate_urls, metrics
            )
        
        # Process and return final metrics
        processed_metrics = {}
        for candidate_id, raw_metrics in metrics.items():
            processed_metrics[candidate_id] = self._process_candidate_metrics(raw_metrics)
        
        return processed_metrics
    
    async def _dispatch_regular_traffic(
        self,
        session: aiohttp.ClientSession,
        candidate_urls: Dict[str, str],
        traffic_events: List[Dict[str, Any]],
        metrics: Dict[str, Dict[str, Any]],
        evaluation_window_sec: int
    ):
        """Dispatch regular buffered traffic to candidates"""
        
        start_time = time.time()
        event_index = 0
        
        while time.time() - start_time < evaluation_window_sec and event_index < len(traffic_events):
            event = traffic_events[event_index]
            
            # Dispatch this event to all candidates simultaneously
            tasks = []
            for candidate_id, base_url in candidate_urls.items():
                task = asyncio.create_task(
                    self._send_request_to_candidate(
                        session, candidate_id, base_url, event, metrics[candidate_id]
                    )
                )
                tasks.append(task)
            
            # Wait for all candidates to process this request
            await asyncio.gather(*tasks, return_exceptions=True)
            
            event_index += 1
            
            # Small delay to simulate realistic traffic pacing
            await asyncio.sleep(0.1)
        
        logger.info(f"Dispatched {event_index} regular traffic events")
    
    async def _send_request_to_candidate(
        self,
        session: aiohttp.ClientSession,
        candidate_id: str,
        base_url: str,
        event: Dict[str, Any],
        metrics: Dict[str, Any]
    ):
        """Send a single request to a candidate and record metrics"""
        
        url = f"{base_url.rstrip('/')}{event['path']}"
        method = event['method']
        
        start_time = time.time()
        
        try:
            async with session.request(method, url) as response:
                latency_ms = (time.time() - start_time) * 1000
                status_code = response.status
                
                # Record metrics
                metrics["latencies"].append(latency_ms)
                metrics["status_codes"].append(status_code)
                metrics["total_requests"] += 1
                
                if status_code >= 400:
                    metrics["error_count"] += 1
                    
        except Exception as e:
            # Handle network errors, timeouts, etc.
            latency_ms = (time.time() - start_time) * 1000
            metrics["latencies"].append(latency_ms)
            metrics["status_codes"].append(0)  # 0 indicates network error
            metrics["error_count"] += 1
            metrics["total_requests"] += 1
            
            logger.debug(f"Request failed for {candidate_id}: {e}")
    
    async def _dispatch_security_probes(
        self,
        session: aiohttp.ClientSession,
        candidate_urls: Dict[str, str],
        metrics: Dict[str, Dict[str, Any]]
    ):
        """Dispatch security probes to all candidates"""
        
        logger.info("Dispatching security probes to candidates")
        
        for probe in self.security_probes:
            for candidate_id, base_url in candidate_urls.items():
                try:
                    result = await self._send_security_probe(
                        session, candidate_id, base_url, probe
                    )
                    metrics[candidate_id]["security_probe_results"][probe["name"]] = result
                    
                except Exception as e:
                    logger.error(f"Security probe {probe['name']} failed for {candidate_id}: {e}")
                    metrics[candidate_id]["security_probe_results"][probe["name"]] = {
                        "success": False,
                        "error": str(e)
                    }
    
    async def _send_security_probe(
        self,
        session: aiohttp.ClientSession,
        candidate_id: str,
        base_url: str,
        probe: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Send a security probe to a candidate"""
        
        url = f"{base_url.rstrip('/')}{probe['path']}"
        method = probe['method']
        
        # Handle repeated probes (like brute force)
        repeat_count = probe.get('repeat', 1)
        results = []
        
        for i in range(repeat_count):
            try:
                kwargs = {}
                if 'params' in probe:
                    kwargs['params'] = probe['params']
                if 'body' in probe:
                    kwargs['json'] = probe['body']
                
                async with session.request(method, url, **kwargs) as response:
                    results.append({
                        "status_code": response.status,
                        "response_time_ms": 0  # Could measure this if needed
                    })
                    
            except Exception as e:
                results.append({
                    "status_code": 0,
                    "error": str(e)
                })
        
        # Evaluate probe results
        expected_statuses = probe.get('expected_status', [200])
        blocked_count = sum(1 for r in results if r.get('status_code', 0) in expected_statuses)
        
        return {
            "probe_name": probe["name"],
            "total_attempts": repeat_count,
            "blocked_attempts": blocked_count,
            "success_rate": blocked_count / repeat_count if repeat_count > 0 else 0,
            "results": results
        }
    
    def _process_candidate_metrics(self, raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw metrics into the expected output format"""
        
        latencies = raw_metrics["latencies"]
        status_codes = raw_metrics["status_codes"]
        
        if not latencies:
            return {
                "performance": {
                    "p50_latency_ms": 0, "p95_latency_ms": 0, "p99_latency_ms": 0,
                    "throughput_rps": 0, "error_rate_pct": 100
                },
                "security_probe_results": raw_metrics["security_probe_results"]
            }
        
        # Calculate percentiles
        latencies_sorted = sorted(latencies)
        n = len(latencies_sorted)
        
        p50 = latencies_sorted[int(0.5 * n)] if n > 0 else 0
        p95 = latencies_sorted[int(0.95 * n)] if n > 1 else latencies_sorted[-1]
        p99 = latencies_sorted[int(0.99 * n)] if n > 2 else latencies_sorted[-1]
        
        # Calculate error rate
        error_count = raw_metrics["error_count"]
        total_requests = raw_metrics["total_requests"]
        error_rate_pct = (error_count / total_requests * 100) if total_requests > 0 else 0
        
        # Estimate throughput (requests per second)
        # This is simplified - in reality we'd measure actual time window
        throughput_rps = total_requests / 90  # Assuming 90s evaluation window
        
        return {
            "performance": {
                "p50_latency_ms": round(p50, 2),
                "p95_latency_ms": round(p95, 2), 
                "p99_latency_ms": round(p99, 2),
                "throughput_rps": round(throughput_rps, 2),
                "error_rate_pct": round(error_rate_pct, 2)
            },
            "security_probe_results": raw_metrics["security_probe_results"]
        }