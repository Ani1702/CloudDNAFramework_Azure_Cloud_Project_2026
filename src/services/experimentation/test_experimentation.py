"""
Test Suite for Experimentation Layer
Member 2 - Cloud DNA Framework

Test the core functionality of the experimentation components
"""

import json
import asyncio
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from candidate_generator import CandidateGenerator, GenomeConfig, TunableParameter
from sandbox_orchestrator import SandboxOrchestrator
from traffic_dispatcher import TrafficDispatcher
from evaluation_coordinator import EvaluationCoordinator


class TestCandidateGenerator:
    """Test the candidate generation functionality"""
    
    def test_load_default_config(self):
        """Test loading default configuration when cdna.yaml is not available"""
        generator = CandidateGenerator("/nonexistent/path")
        
        # Should load default config
        assert generator.cdna_config['app_id'] == 'medusa'
        assert len(generator.tunable_params) == 10
        assert generator.max_concurrent_candidates == 4
    
    def test_parse_tunable_parameters(self):
        """Test parsing tunable parameters from config"""
        generator = CandidateGenerator("/nonexistent/path")
        
        # Check specific parameters
        assert 'replica_count' in generator.tunable_params
        assert generator.tunable_params['replica_count'].type == 'int'
        assert generator.tunable_params['replica_count'].min == 1
        assert generator.tunable_params['replica_count'].max == 10
        
        assert 'cpu_cores' in generator.tunable_params
        assert generator.tunable_params['cpu_cores'].type == 'float'
        
        assert 'redis_cache_enabled' in generator.tunable_params
        assert generator.tunable_params['redis_cache_enabled'].type == 'bool'
    
    def test_generate_candidates(self):
        """Test candidate generation with various inputs"""
        generator = CandidateGenerator("/nonexistent/path")
        
        trigger_event = {
            'trigger_id': 'test-trigger-001',
            'trigger_category': 'TRAFFIC_SPIKE'
        }
        
        candidates = generator.generate_candidates(
            trigger_event=trigger_event,
            n_candidates=3
        )
        
        # Should generate requested number of candidates
        assert len(candidates) == 3
        
        # Each candidate should have required fields
        for candidate in candidates:
            assert 'trigger_id' in candidate
            assert 'candidate_id' in candidate
            assert 'genome' in candidate
            assert 'generation_method' in candidate
            
            # Validate genome structure
            genome = candidate['genome']
            assert 'replica_count' in genome
            assert 'cpu_cores' in genome
            assert 'memory_gb' in genome
            assert isinstance(genome['replica_count'], int)
            assert isinstance(genome['cpu_cores'], float)
    
    def test_validate_genome(self):
        """Test genome validation against constraints"""
        generator = CandidateGenerator("/nonexistent/path")
        
        # Valid genome
        valid_genome = {
            'replica_count': 3,
            'cpu_cores': 1.5,
            'memory_gb': 2.0,
            'node_thread_pool_size': 32,
            'redis_cache_enabled': True,
            'redis_cache_ttl_seconds': 300,
            'db_connection_pool_max': 20,
            'rate_limiting_enabled': False,
            'rate_limit_rps': 200,
            'health_check_timeout_sec': 5
        }
        
        is_valid, errors = generator.validate_genome(valid_genome)
        assert is_valid == True
        assert len(errors) == 0
        
        # Invalid genome - missing parameters
        invalid_genome = {
            'replica_count': 3,
            'cpu_cores': 1.5
            # Missing other required parameters
        }
        
        is_valid, errors = generator.validate_genome(invalid_genome)
        assert is_valid == False
        assert len(errors) > 0
        assert any('Missing required parameter' in error for error in errors)
        
        # Invalid genome - constraint violations
        constraint_violation_genome = {
            'replica_count': 15,  # Above max of 10
            'cpu_cores': -1.0,    # Below min of 0.25
            'memory_gb': 2.0,
            'node_thread_pool_size': 32,
            'redis_cache_enabled': True,
            'redis_cache_ttl_seconds': 300,
            'db_connection_pool_max': 20,
            'rate_limiting_enabled': False,
            'rate_limit_rps': 200,
            'health_check_timeout_sec': 5
        }
        
        is_valid, errors = generator.validate_genome(constraint_violation_genome)
        assert is_valid == False
        assert any('above maximum' in error for error in errors)
        assert any('below minimum' in error for error in errors)
    
    def test_mutation_candidate(self):
        """Test mutation-based candidate generation"""
        generator = CandidateGenerator("/nonexistent/path")
        
        base_genome = {
            'replica_count': 2,
            'cpu_cores': 1.0,
            'memory_gb': 1.0,
            'node_thread_pool_size': 20,
            'redis_cache_enabled': False,
            'redis_cache_ttl_seconds': 0,
            'db_connection_pool_max': 10,
            'rate_limiting_enabled': False,
            'rate_limit_rps': 100,
            'health_check_timeout_sec': 2
        }
        
        mutated_genome = generator.generate_mutation_candidate(
            base_genome, 'TRAFFIC_SPIKE', mutation_rate=0.5
        )
        
        # Should return a valid genome
        assert isinstance(mutated_genome, dict)
        assert len(mutated_genome) == len(base_genome)
        
        # At least some parameters should be different (with high probability)
        differences = sum(1 for key in base_genome if base_genome[key] != mutated_genome.get(key))
        # With 50% mutation rate and 10 parameters, expect some changes
        # This is probabilistic, so we allow for some variance


class TestTrafficDispatcher:
    """Test traffic dispatching functionality"""
    
    def test_generate_synthetic_traffic(self):
        """Test synthetic traffic generation when Event Hub unavailable"""
        dispatcher = TrafficDispatcher()
        
        traffic_events = dispatcher._generate_synthetic_traffic(50)
        
        # Should generate requested number of events
        assert len(traffic_events) <= 50  # May generate less
        
        # Each event should have required fields
        for event in traffic_events:
            assert 'app_id' in event
            assert 'ts' in event
            assert 'method' in event
            assert 'path' in event
            assert 'status_code' in event
            assert 'latency_ms' in event
            
            # Validate field types
            assert isinstance(event['status_code'], int)
            assert isinstance(event['latency_ms'], int)
            assert event['latency_ms'] >= 10  # Minimum latency
    
    def test_generate_security_probes(self):
        """Test security probe generation"""
        dispatcher = TrafficDispatcher()
        
        probes = dispatcher.security_probes
        
        # Should have various probe types
        probe_names = [probe['name'] for probe in probes]
        assert 'sql_injection' in probe_names
        assert 'xss_attempt' in probe_names
        assert 'auth_bruteforce' in probe_names
        
        # Each probe should have required fields
        for probe in probes:
            assert 'name' in probe
            assert 'method' in probe
            assert 'path' in probe
            assert 'expected_status' in probe
    
    def test_process_candidate_metrics(self):
        """Test metrics processing and calculation"""
        dispatcher = TrafficDispatcher()
        
        raw_metrics = {
            "latencies": [100, 150, 200, 250, 300],
            "status_codes": [200, 200, 200, 404, 500],
            "error_count": 2,
            "total_requests": 5,
            "security_probe_results": {
                "sql_injection": {"success_rate": 0.8},
                "auth_bruteforce": {"success_rate": 0.9}
            }
        }
        
        processed = dispatcher._process_candidate_metrics(raw_metrics)
        
        # Should calculate percentiles correctly
        assert processed['performance']['p50_latency_ms'] == 200.0  # Median
        assert processed['performance']['p95_latency_ms'] == 300.0  # 95th percentile
        assert processed['performance']['error_rate_pct'] == 40.0   # 2/5 * 100
        
        # Should include security results
        assert 'security_probe_results' in processed


class TestEvaluationCoordinator:
    """Test the evaluation coordination functionality"""
    
    def test_extract_deployment_config(self):
        """Test deployment configuration extraction"""
        coordinator = EvaluationCoordinator()
        
        config = coordinator._extract_deployment_config()
        
        assert 'container_registry_image' in config
        assert 'platform' in config
        assert config['platform'] in ['aks', 'container_apps']
    
    def test_calculate_cost_metrics(self):
        """Test cost calculation based on resource allocation"""
        coordinator = EvaluationCoordinator()
        
        genome = {
            'cpu_cores': 2.0,
            'memory_gb': 4.0,
            'replica_count': 3
        }
        
        cost_metrics = coordinator._calculate_cost_metrics(genome)
        
        assert 'hourly_compute_cost_usd' in cost_metrics
        assert isinstance(cost_metrics['hourly_compute_cost_usd'], float)
        assert cost_metrics['hourly_compute_cost_usd'] > 0
        
        # Cost should increase with more resources
        smaller_genome = {
            'cpu_cores': 1.0,
            'memory_gb': 1.0,
            'replica_count': 1
        }
        
        smaller_cost = coordinator._calculate_cost_metrics(smaller_genome)
        assert smaller_cost['hourly_compute_cost_usd'] < cost_metrics['hourly_compute_cost_usd']
    
    def test_calculate_scaling_metrics(self):
        """Test scaling metrics calculation"""
        coordinator = EvaluationCoordinator()
        
        genome = {
            'replica_count': 5,
            'cpu_cores': 2.0,
            'memory_gb': 4.0
        }
        
        scaling_metrics = coordinator._calculate_scaling_metrics(genome)
        
        assert 'current_replica_count' in scaling_metrics
        assert 'cold_start_time_ms' in scaling_metrics
        assert scaling_metrics['current_replica_count'] == 5
        assert isinstance(scaling_metrics['cold_start_time_ms'], int)
    
    def test_extract_security_metrics(self):
        """Test security metrics extraction from probe results"""
        coordinator = EvaluationCoordinator()
        
        metrics = {
            'security_probe_results': {
                'auth_bruteforce': {'success_rate': 0.85},
                'sql_injection': {'success_rate': 0.95},
                'xss_attempt': {'success_rate': 0.80}
            }
        }
        
        security_metrics = coordinator._extract_security_metrics(metrics)
        
        assert 'failed_auth_attempts_per_min' in security_metrics
        assert 'anomalous_ip_request_rate' in security_metrics
        assert isinstance(security_metrics['anomalous_ip_request_rate'], float)


# Integration test for the full workflow
class TestIntegration:
    """Integration tests for the complete experimentation workflow"""
    
    @pytest.mark.asyncio
    async def test_sample_trigger_processing(self):
        """Test processing a sample trigger event (mocked external dependencies)"""
        
        # Sample trigger event
        trigger_event = {
            "app_id": "medusa",
            "trigger_id": "test-trigger-001",
            "timestamp": "2026-09-15T12:00:00Z",
            "trigger_category": "TRAFFIC_SPIKE",
            "severity": "HIGH",
            "current_production_config": {
                "cpu_cores": 1.0,
                "memory_gb": 1.0,
                "replica_count": 2,
                "node_thread_pool_size": 20,
                "redis_cache_enabled": False
            },
            "fast_path_match": {"found": False}
        }
        
        # Mock external dependencies
        with patch('evaluation_coordinator.CosmosClient') as mock_cosmos:
            with patch('sandbox_orchestrator.config') as mock_k8s_config:
                with patch('traffic_dispatcher.EventHubConsumerClient') as mock_eventhub:
                    
                    # Initialize coordinator with mocked dependencies
                    coordinator = EvaluationCoordinator(cosmos_endpoint=None)
                    
                    # Mock the external method calls
                    coordinator._get_historical_genomes = Mock(return_value=[])
                    coordinator._get_llm_suggested_ranges = Mock(return_value=None)
                    coordinator._store_evaluation_results = Mock()
                    
                    # Mock sandbox orchestrator methods
                    coordinator.sandbox_orchestrator.deploy_candidates = Mock(return_value=[
                        {
                            "candidate_id": "cand-test-001",
                            "status": "ready",
                            "readiness_ts": "2026-09-15T12:01:00Z",
                            "service_url": "http://test-service"
                        }
                    ])
                    coordinator.sandbox_orchestrator.cleanup_candidates = Mock()
                    
                    # Mock traffic dispatcher methods  
                    coordinator.traffic_dispatcher.collect_buffered_traffic = Mock(return_value=[
                        {
                            "method": "GET",
                            "path": "/store/products", 
                            "status_code": 200,
                            "latency_ms": 150
                        }
                    ])
                    coordinator.traffic_dispatcher.dispatch_traffic_to_candidates = Mock(return_value={
                        "cand-test-001": {
                            "performance": {
                                "p50_latency_ms": 150.0,
                                "p95_latency_ms": 200.0,
                                "throughput_rps": 10.0,
                                "error_rate_pct": 0.0
                            },
                            "security_probe_results": {}
                        }
                    })
                    
                    # Process the trigger event
                    results = await coordinator.process_trigger_event(trigger_event)
                    
                    # Validate results
                    assert len(results) >= 1
                    result = results[0]
                    
                    assert result['trigger_id'] == 'test-trigger-001'
                    assert 'candidate_id' in result
                    assert 'genome' in result
                    assert 'raw_metrics' in result
                    
                    # Validate metrics structure
                    metrics = result['raw_metrics']
                    assert 'performance' in metrics
                    assert 'cost' in metrics
                    assert 'security' in metrics
                    assert 'scaling' in metrics
                    assert 'recovery' in metrics


if __name__ == "__main__":
    # Run tests
    import subprocess
    import sys
    
    # Install pytest if not available
    try:
        import pytest
    except ImportError:
        print("Installing pytest...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest", "pytest-asyncio"])
        import pytest
    
    # Run the test suite
    pytest.main([__file__, "-v"])