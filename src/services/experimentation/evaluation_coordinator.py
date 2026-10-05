"""
Evaluation Coordinator - Experimentation Layer
Member 2 - Cloud DNA Framework

This module coordinates the entire experimentation process:
1. Receives trigger events from Decision Layer
2. Generates candidates using the candidate generator
3. Deploys candidates via sandbox orchestrator  
4. Dispatches traffic and collects metrics
5. Emits candidate evaluation results to Cosmos DB
"""

import json
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

from .candidate_generator import CandidateGenerator
from .sandbox_orchestrator import SandboxOrchestrator
from .traffic_dispatcher import TrafficDispatcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EvaluationCoordinator:
    """
    Coordinates the complete experimentation workflow
    """
    
    def __init__(
        self,
        cosmos_endpoint: str = None,
        cosmos_database: str = "clouddna",
        event_hub_connection_string: str = None,
        cdna_config_path: str = "/app/cdna.yaml"
    ):
        """Initialize evaluation coordinator with Azure dependencies"""
        
        # Initialize components
        self.candidate_generator = CandidateGenerator(cdna_config_path)
        self.sandbox_orchestrator = SandboxOrchestrator()
        self.traffic_dispatcher = TrafficDispatcher(event_hub_connection_string)
        
        # Initialize Cosmos DB client
        if cosmos_endpoint:
            try:
                credential = DefaultAzureCredential()
                self.cosmos_client = CosmosClient(cosmos_endpoint, credential)
                self.database = self.cosmos_client.get_database_client(cosmos_database)
                self.candidate_evaluations_container = self.database.get_container_client("candidate_evaluations")
                self.genome_library_container = self.database.get_container_client("genome_library")
                logger.info("Cosmos DB client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Cosmos DB client: {e}")
                self.cosmos_client = None
        else:
            logger.warning("No Cosmos DB endpoint provided - results will only be logged")
            self.cosmos_client = None
        
        logger.info("Evaluation coordinator initialized")
    
    async def process_trigger_event(self, trigger_event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process a trigger event through the complete experimentation pipeline
        
        Args:
            trigger_event: The trigger event from Decision Layer
            
        Returns:
            List of candidate evaluation results
        """
        trigger_id = trigger_event.get('trigger_id', 'unknown')
        logger.info(f"Processing trigger event {trigger_id}")
        
        try:
            # Step 1: Query historical genomes for seeding
            historical_genomes = await self._get_historical_genomes(trigger_event)
            
            # Step 2: Get LLM suggested ranges (would call Member 3's shared client)
            llm_suggested_ranges = await self._get_llm_suggested_ranges(trigger_event, historical_genomes)
            
            # Step 3: Generate candidates
            candidates = self.candidate_generator.generate_candidates(
                trigger_event=trigger_event,
                historical_genomes=historical_genomes,
                llm_suggested_ranges=llm_suggested_ranges,
                n_candidates=4  # Based on cdna.yaml max_concurrent_candidates
            )
            
            logger.info(f"Generated {len(candidates)} candidates for evaluation")
            
            # Step 4: Deploy candidates to sandbox
            base_deployment_config = self._extract_deployment_config()
            deployment_results = await self.sandbox_orchestrator.deploy_candidates(
                candidates, base_deployment_config
            )
            
            # Filter successful deployments
            ready_deployments = [d for d in deployment_results if d['status'] == 'ready']
            logger.info(f"{len(ready_deployments)} candidates successfully deployed and ready")
            
            if not ready_deployments:
                logger.error("No candidates deployed successfully - aborting evaluation")
                return []
            
            # Step 5: Collect buffered traffic
            evaluation_window_sec = self.candidate_generator.cdna_config.get('sandbox', {}).get('evaluation_window_sec', 90)
            traffic_events = await self.traffic_dispatcher.collect_buffered_traffic(
                buffer_window_seconds=60,
                max_requests=1000
            )
            
            # Step 6: Dispatch traffic and collect metrics
            candidate_metrics = await self.traffic_dispatcher.dispatch_traffic_to_candidates(
                ready_deployments, traffic_events, evaluation_window_sec
            )
            
            # Step 7: Generate evaluation results
            evaluation_results = []
            for deployment in ready_deployments:
                candidate_id = deployment['candidate_id']
                candidate = next(c for c in candidates if c['candidate_id'] == candidate_id)
                
                metrics = candidate_metrics.get(candidate_id, {})
                
                evaluation_result = {
                    "trigger_id": trigger_id,
                    "candidate_id": candidate_id,
                    "genome": candidate['genome'],
                    "generation_method": candidate['generation_method'],
                    "readiness_ts": deployment['readiness_ts'],
                    "raw_metrics": self._enrich_metrics_with_cost_and_scaling(metrics, candidate['genome']),
                    "security_probe_results": metrics.get('security_probe_results', {}),
                    "evaluation_completed_at": datetime.utcnow().isoformat() + "Z"
                }
                
                evaluation_results.append(evaluation_result)
            
            # Step 8: Store results in Cosmos DB
            await self._store_evaluation_results(evaluation_results)
            
            # Step 9: Cleanup sandbox deployments
            candidate_ids_to_cleanup = [d['candidate_id'] for d in deployment_results]
            await self.sandbox_orchestrator.cleanup_candidates(candidate_ids_to_cleanup)
            
            logger.info(f"Completed evaluation for trigger {trigger_id} with {len(evaluation_results)} results")
            return evaluation_results
            
        except Exception as e:
            logger.error(f"Error processing trigger event {trigger_id}: {e}")
            # Attempt cleanup on failure
            try:
                candidate_ids = [c.get('candidate_id') for c in candidates if c.get('candidate_id')]
                if candidate_ids:
                    await self.sandbox_orchestrator.cleanup_candidates(candidate_ids)
            except:
                pass
            raise
    
    async def _get_historical_genomes(self, trigger_event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query top-k historical genomes for this fingerprint from Cosmos DB"""
        
        if not self.cosmos_client:
            logger.warning("No Cosmos DB client available for historical genome lookup")
            return []
        
        try:
            app_id = trigger_event.get('app_id', 'medusa')
            fingerprint_vector = trigger_event.get('fingerprint_vector', [])
            
            # Query genome library for similar fingerprints
            # This is simplified - in production you'd implement proper vector similarity search
            query = """
            SELECT TOP 5 g.genome, g.fitness_score, g.fingerprint_vector
            FROM g 
            WHERE g.app_id = @app_id 
            ORDER BY g.fitness_score DESC
            """
            
            items = list(self.genome_library_container.query_items(
                query=query,
                parameters=[{"name": "@app_id", "value": app_id}],
                enable_cross_partition_query=True
            ))
            
            logger.info(f"Retrieved {len(items)} historical genomes for seeding")
            return items
            
        except Exception as e:
            logger.error(f"Failed to retrieve historical genomes: {e}")
            return []
    
    async def _get_llm_suggested_ranges(
        self, 
        trigger_event: Dict[str, Any], 
        historical_genomes: List[Dict[str, Any]]
    ) -> Optional[Dict[str, List[float]]]:
        """
        Get LLM suggested parameter ranges via Member 3's shared client
        This is a placeholder - Member 3 will implement the actual LLM client
        """
        
        # TODO: Call Member 3's llm_client.py when available
        # For now, return None to use full cdna.yaml ranges
        
        logger.info("LLM prior seeding not yet implemented - using full parameter ranges")
        return None
    
    def _extract_deployment_config(self) -> Dict[str, Any]:
        """Extract deployment configuration from cdna.yaml"""
        return {
            "container_registry_image": self.candidate_generator.cdna_config.get('deployment', {}).get('container_registry_image', 'acrclouddna.azurecr.io/medusa:latest'),
            "platform": self.candidate_generator.cdna_config.get('deployment', {}).get('platform', 'aks'),
            "namespace_prefix": "sandbox"
        }
    
    def _enrich_metrics_with_cost_and_scaling(
        self, 
        metrics: Dict[str, Any], 
        genome: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich performance metrics with cost and scaling information
        
        Args:
            metrics: Raw metrics from traffic dispatcher
            genome: Candidate genome configuration
            
        Returns:
            Enriched metrics in the expected schema format
        """
        
        # Start with performance metrics from traffic dispatcher
        enriched = {
            "performance": metrics.get("performance", {}),
            "cost": self._calculate_cost_metrics(genome),
            "security": self._extract_security_metrics(metrics),
            "scaling": self._calculate_scaling_metrics(genome),
            "recovery": self._calculate_recovery_metrics(metrics)
        }
        
        return enriched
    
    def _calculate_cost_metrics(self, genome: Dict[str, Any]) -> Dict[str, float]:
        """Calculate estimated cost metrics based on resource allocation"""
        
        # Simplified cost calculation based on Azure AKS pricing
        # CPU: ~$0.096 per vCPU per hour
        # Memory: ~$0.0144 per GB per hour
        
        cpu_cores = genome.get('cpu_cores', 1.0)
        memory_gb = genome.get('memory_gb', 1.0) 
        replica_count = genome.get('replica_count', 1)
        
        hourly_cpu_cost = cpu_cores * replica_count * 0.096
        hourly_memory_cost = memory_gb * replica_count * 0.0144
        hourly_total_cost = hourly_cpu_cost + hourly_memory_cost
        
        return {
            "hourly_compute_cost_usd": round(hourly_total_cost, 4)
        }
    
    def _extract_security_metrics(self, metrics: Dict[str, Any]) -> Dict[str, float]:
        """Extract security metrics from probe results"""
        
        security_results = metrics.get('security_probe_results', {})
        
        # Count successful blocks
        auth_bruteforce_blocked = False
        malformed_req_5xx_rate = 0.0
        
        if 'auth_bruteforce' in security_results:
            auth_result = security_results['auth_bruteforce']
            # Consider successful if > 80% of brute force attempts were blocked
            auth_bruteforce_blocked = auth_result.get('success_rate', 0) > 0.8
        
        # Calculate malformed request handling rate
        malformed_probes = ['sql_injection', 'xss_attempt', 'path_traversal']
        blocked_count = 0
        total_probes = 0
        
        for probe_name in malformed_probes:
            if probe_name in security_results:
                probe_result = security_results[probe_name]
                if probe_result.get('success_rate', 0) > 0.5:  # More than 50% blocked
                    blocked_count += 1
                total_probes += 1
        
        if total_probes > 0:
            malformed_req_5xx_rate = 1.0 - (blocked_count / total_probes)  # Rate of failures to block
        
        return {
            "failed_auth_attempts_per_min": 0,  # Would be calculated from real monitoring
            "anomalous_ip_request_rate": malformed_req_5xx_rate
        }
    
    def _calculate_scaling_metrics(self, genome: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate scaling-related metrics"""
        
        replica_count = genome.get('replica_count', 1)
        
        # Simplified cold start calculation based on resource allocation
        # Larger containers typically have longer cold start times
        cpu_cores = genome.get('cpu_cores', 1.0)
        memory_gb = genome.get('memory_gb', 1.0)
        
        # Estimate cold start time: base + resource overhead
        base_cold_start = 2000  # 2 seconds base
        resource_overhead = (cpu_cores * 500) + (memory_gb * 300)  # ms
        cold_start_time_ms = int(base_cold_start + resource_overhead)
        
        return {
            "current_replica_count": replica_count,
            "cold_start_time_ms": cold_start_time_ms
        }
    
    def _calculate_recovery_metrics(self, metrics: Dict[str, Any]) -> Dict[str, float]:
        """Calculate recovery/resilience metrics"""
        
        performance = metrics.get('performance', {})
        error_rate_pct = performance.get('error_rate_pct', 0)
        
        # Simplified recovery metrics
        # In production, these would come from actual health check monitoring
        
        return {
            "restart_count": 0,  # Would track actual restarts during evaluation
            "health_check_failure_rate_pct": min(error_rate_pct, 10.0)  # Cap at 10%
        }
    
    async def _store_evaluation_results(self, evaluation_results: List[Dict[str, Any]]):
        """Store evaluation results in Cosmos DB"""
        
        if not self.cosmos_client:
            logger.warning("No Cosmos DB client - results will only be logged")
            for result in evaluation_results:
                logger.info(f"Evaluation result: {json.dumps(result, indent=2)}")
            return
        
        try:
            for result in evaluation_results:
                # Add required partition key and document ID
                result['id'] = result['candidate_id']
                result['_ts'] = int(datetime.utcnow().timestamp())
                
                # Store in candidate_evaluations container
                self.candidate_evaluations_container.create_item(body=result)
                
                logger.info(f"Stored evaluation result for candidate {result['candidate_id']}")
                
        except Exception as e:
            logger.error(f"Failed to store evaluation results: {e}")
            # Log results as fallback
            for result in evaluation_results:
                logger.info(f"Evaluation result (fallback): {json.dumps(result, indent=2)}")


# Azure Function entry point
async def main(trigger_event: str) -> str:
    """
    Azure Function entry point for processing trigger events
    
    Args:
        trigger_event: JSON string containing the trigger event
        
    Returns:
        JSON string containing evaluation results
    """
    
    try:
        # Parse trigger event
        event_data = json.loads(trigger_event)
        
        # Initialize coordinator
        coordinator = EvaluationCoordinator(
            cosmos_endpoint="https://clouddna-cosmos.documents.azure.com:443/",  # This would be from environment
            event_hub_connection_string=None  # This would be from environment
        )
        
        # Process the event
        results = await coordinator.process_trigger_event(event_data)
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return json.dumps({"error": str(e)})


# For local testing
if __name__ == "__main__":
    # Example trigger event for testing
    test_trigger_event = {
        "app_id": "medusa",
        "trigger_id": "trig-20260915-001",
        "timestamp": "2026-09-15T12:00:00Z",
        "trigger_category": "TRAFFIC_SPIKE",
        "severity": "HIGH",
        "forecasted_load": {
            "throughput_rps_at_eval_end": 480.0,
            "method": "linear_extrapolation"
        },
        "fingerprint_vector": [0.85, 0.35, 0.05, 0.70, 0.00],
        "fingerprint_version": "v1",
        "current_production_config": {
            "cpu_cores": 1.0,
            "memory_gb": 1.0, 
            "replica_count": 2,
            "node_thread_pool_size": 20,
            "redis_cache_enabled": False,
            "redis_cache_ttl_seconds": 0,
            "rate_limiting_enabled": False,
            "health_check_timeout_sec": 2
        },
        "fast_path_match": {
            "found": False,
            "matched_genome_id": None,
            "similarity": 0.0
        }
    }
    
    async def test_main():
        coordinator = EvaluationCoordinator()
        results = await coordinator.process_trigger_event(test_trigger_event)
        print(json.dumps(results, indent=2))
    
    asyncio.run(test_main())