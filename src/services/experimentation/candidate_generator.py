"""
Candidate Generator - Experimentation Layer
Member 2 - Cloud DNA Framework

This module generates candidate configurations using:
1. Rule-based guardrails from cdna.yaml min/max constraints
2. Optuna Bayesian optimizer
3. Historical genome seeding
4. Nemotron prior-seeding (via Member 3's shared client)
"""

import optuna
import yaml
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import numpy as np
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GenomeConfig:
    """Represents a candidate genome configuration"""
    replica_count: int
    cpu_cores: float
    memory_gb: float
    node_thread_pool_size: int
    redis_cache_enabled: bool
    redis_cache_ttl_seconds: int
    db_connection_pool_max: int
    rate_limiting_enabled: bool
    rate_limit_rps: int
    health_check_timeout_sec: int

@dataclass 
class TunableParameter:
    """Represents a tunable parameter from cdna.yaml"""
    name: str
    type: str
    min: Optional[float] = None
    max: Optional[float] = None
    default: Any = None
    restart_required: bool = False

class CandidateGenerator:
    """
    Generates candidate configurations for experimentation
    """
    
    def __init__(self, cdna_config_path: str = "/app/cdna.yaml"):
        """Initialize with cdna.yaml configuration"""
        self.cdna_config = self._load_cdna_config(cdna_config_path)
        self.tunable_params = self._parse_tunable_parameters()
        self.max_concurrent_candidates = self.cdna_config.get('sandbox', {}).get('max_concurrent_candidates', 4)
        
    def _load_cdna_config(self, config_path: str) -> Dict[str, Any]:
        """Load and parse cdna.yaml configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"cdna.yaml not found at {config_path}, using default config")
            # Return default configuration for testing
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Default configuration for testing when cdna.yaml is not available"""
        return {
            "app_id": "medusa",
            "tunable_parameters": [
                {"name": "replica_count", "type": "int", "min": 1, "max": 10, "default": 2, "restart_required": False},
                {"name": "cpu_cores", "type": "float", "min": 0.25, "max": 2.0, "default": 1.0, "restart_required": True},
                {"name": "memory_gb", "type": "float", "min": 0.5, "max": 4.0, "default": 1.0, "restart_required": True},
                {"name": "node_thread_pool_size", "type": "int", "min": 4, "max": 128, "default": 20, "restart_required": True},
                {"name": "redis_cache_enabled", "type": "bool", "default": False, "restart_required": True},
                {"name": "redis_cache_ttl_seconds", "type": "int", "min": 0, "max": 3600, "default": 0, "restart_required": False},
                {"name": "db_connection_pool_max", "type": "int", "min": 5, "max": 100, "default": 10, "restart_required": True},
                {"name": "rate_limiting_enabled", "type": "bool", "default": False, "restart_required": True},
                {"name": "rate_limit_rps", "type": "int", "min": 10, "max": 1000, "default": 100, "restart_required": False},
                {"name": "health_check_timeout_sec", "type": "int", "min": 1, "max": 10, "default": 2, "restart_required": False}
            ],
            "sandbox": {
                "max_concurrent_candidates": 4,
                "evaluation_window_sec": 90
            }
        }
    
    def _parse_tunable_parameters(self) -> Dict[str, TunableParameter]:
        """Parse tunable parameters from cdna.yaml"""
        params = {}
        for param_config in self.cdna_config.get('tunable_parameters', []):
            param = TunableParameter(
                name=param_config['name'],
                type=param_config['type'],
                min=param_config.get('min'),
                max=param_config.get('max'),
                default=param_config.get('default'),
                restart_required=param_config.get('restart_required', False)
            )
            params[param.name] = param
        return params
    
    def generate_candidates(
        self, 
        trigger_event: Dict[str, Any],
        historical_genomes: List[Dict[str, Any]] = None,
        llm_suggested_ranges: Dict[str, List[float]] = None,
        n_candidates: int = 4
    ) -> List[Dict[str, Any]]:
        """
        Generate N candidate configurations using Optuna Bayesian optimization
        
        Args:
            trigger_event: The trigger event that initiated this experiment
            historical_genomes: Top-k historical genomes for seeding
            llm_suggested_ranges: Nemotron-suggested parameter ranges (optional)
            n_candidates: Number of candidates to generate
            
        Returns:
            List of candidate configurations
        """
        logger.info(f"Generating {n_candidates} candidates for trigger {trigger_event.get('trigger_id')}")
        
        # Limit candidates based on sandbox constraints
        n_candidates = min(n_candidates, self.max_concurrent_candidates)
        
        candidates = []
        
        # Create Optuna study for Bayesian optimization
        study_name = f"cdna_experiment_{trigger_event.get('trigger_id', 'unknown')}"
        study = optuna.create_study(
            study_name=study_name,
            direction='maximize',  # We'll maximize a composite fitness score
            sampler=optuna.samplers.TPESampler(
                n_startup_trials=min(10, len(historical_genomes) if historical_genomes else 0),
                seed=42
            )
        )
        
        # Seed with historical genomes if available
        if historical_genomes:
            self._seed_study_with_historical_genomes(study, historical_genomes)
        
        # Generate candidates
        for i in range(n_candidates):
            trial = study.ask()
            
            candidate_genome = self._suggest_genome(
                trial, 
                llm_suggested_ranges,
                method="bayesian_optimizer" if i > 0 else "historical_seed" if historical_genomes else "bayesian_optimizer"
            )
            
            candidate = {
                "trigger_id": trigger_event.get('trigger_id'),
                "candidate_id": f"cand-{trigger_event.get('trigger_id', 'unknown')}-{i+1:03d}",
                "genome": candidate_genome,
                "generation_method": "bayesian_optimizer",
                "generated_at": datetime.utcnow().isoformat() + "Z"
            }
            
            candidates.append(candidate)
            
            logger.info(f"Generated candidate {candidate['candidate_id']} with method {candidate['generation_method']}")
        
        return candidates
    
    def _seed_study_with_historical_genomes(self, study: optuna.Study, historical_genomes: List[Dict[str, Any]]):
        """Seed Optuna study with historical genome configurations"""
        logger.info(f"Seeding study with {len(historical_genomes)} historical genomes")
        
        for i, genome_record in enumerate(historical_genomes):
            genome = genome_record.get('genome', {})
            # Use historical fitness score if available, otherwise use a neutral score
            fitness_score = genome_record.get('fitness_score', 0.5)
            
            # Create trial parameters from genome
            trial_params = {}
            for param_name, param in self.tunable_params.items():
                if param_name in genome:
                    trial_params[param_name] = genome[param_name]
            
            # Add trial to study
            if trial_params:
                study.enqueue_trial(trial_params, skip_if_exists=True)
    
    def _suggest_genome(
        self, 
        trial: optuna.Trial, 
        llm_suggested_ranges: Dict[str, List[float]] = None,
        method: str = "bayesian_optimizer"
    ) -> Dict[str, Any]:
        """
        Suggest parameter values for a genome using Optuna trial
        
        Args:
            trial: Optuna trial object
            llm_suggested_ranges: Optional LLM-suggested parameter ranges
            method: Generation method for tracking
            
        Returns:
            Dictionary containing genome configuration
        """
        genome = {}
        
        for param_name, param in self.tunable_params.items():
            if param.type == 'int':
                # Use LLM suggested range if available, otherwise use cdna.yaml constraints
                if llm_suggested_ranges and param_name in llm_suggested_ranges:
                    range_vals = llm_suggested_ranges[param_name]
                    low = max(int(range_vals[0]), param.min or 0)
                    high = min(int(range_vals[1]), param.max or 1000)
                else:
                    low = param.min or 1
                    high = param.max or 100
                
                genome[param_name] = trial.suggest_int(param_name, low, high)
                
            elif param.type == 'float':
                if llm_suggested_ranges and param_name in llm_suggested_ranges:
                    range_vals = llm_suggested_ranges[param_name]
                    low = max(float(range_vals[0]), param.min or 0.0)
                    high = min(float(range_vals[1]), param.max or 10.0)
                else:
                    low = param.min or 0.1
                    high = param.max or 10.0
                
                genome[param_name] = trial.suggest_float(param_name, low, high)
                
            elif param.type == 'bool':
                genome[param_name] = trial.suggest_categorical(param_name, [True, False])
        
        return genome
    
    def generate_mutation_candidate(
        self, 
        base_genome: Dict[str, Any], 
        trigger_category: str,
        mutation_rate: float = 0.3
    ) -> Dict[str, Any]:
        """
        Generate a mutation-based candidate from a base genome
        
        Args:
            base_genome: Base genome to mutate
            trigger_category: Type of trigger (affects which parameters to focus on)
            mutation_rate: Probability of mutating each parameter
            
        Returns:
            Mutated genome configuration
        """
        mutated_genome = base_genome.copy()
        
        # Focus mutations based on trigger category
        focus_params = self._get_focus_parameters(trigger_category)
        
        for param_name, param in self.tunable_params.items():
            # Higher mutation probability for focused parameters
            current_rate = mutation_rate * (2.0 if param_name in focus_params else 1.0)
            
            if np.random.random() < current_rate:
                if param.type == 'int':
                    delta = np.random.randint(-2, 3)  # Small random change
                    new_val = base_genome[param_name] + delta
                    new_val = max(param.min or 1, min(param.max or 100, new_val))
                    mutated_genome[param_name] = new_val
                    
                elif param.type == 'float':
                    delta = np.random.uniform(-0.5, 0.5)  # Small random change
                    new_val = base_genome[param_name] + delta
                    new_val = max(param.min or 0.1, min(param.max or 10.0, new_val))
                    mutated_genome[param_name] = new_val
                    
                elif param.type == 'bool':
                    mutated_genome[param_name] = not base_genome[param_name]
        
        return mutated_genome
    
    def _get_focus_parameters(self, trigger_category: str) -> List[str]:
        """
        Get parameters to focus on based on trigger category
        
        Args:
            trigger_category: The type of trigger event
            
        Returns:
            List of parameter names to focus mutations on
        """
        focus_map = {
            "TRAFFIC_SPIKE": ["replica_count", "cpu_cores", "memory_gb", "rate_limiting_enabled", "rate_limit_rps"],
            "PERFORMANCE_DEGRADATION": ["cpu_cores", "memory_gb", "node_thread_pool_size", "redis_cache_enabled"],
            "RESOURCE_SATURATION": ["replica_count", "cpu_cores", "memory_gb", "db_connection_pool_max"],
            "SECURITY_ANOMALY": ["rate_limiting_enabled", "rate_limit_rps", "health_check_timeout_sec"],
            "FAILURE_RECOVERY": ["replica_count", "health_check_timeout_sec", "db_connection_pool_max"]
        }
        
        return focus_map.get(trigger_category, list(self.tunable_params.keys()))
    
    def validate_genome(self, genome: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a genome against cdna.yaml constraints
        
        Args:
            genome: Genome configuration to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        for param_name, param in self.tunable_params.items():
            if param_name not in genome:
                errors.append(f"Missing required parameter: {param_name}")
                continue
            
            value = genome[param_name]
            
            if param.type == 'int' and not isinstance(value, int):
                errors.append(f"Parameter {param_name} must be int, got {type(value)}")
            elif param.type == 'float' and not isinstance(value, (int, float)):
                errors.append(f"Parameter {param_name} must be float, got {type(value)}")
            elif param.type == 'bool' and not isinstance(value, bool):
                errors.append(f"Parameter {param_name} must be bool, got {type(value)}")
            
            # Check min/max constraints
            if param.min is not None and value < param.min:
                errors.append(f"Parameter {param_name} value {value} below minimum {param.min}")
            if param.max is not None and value > param.max:
                errors.append(f"Parameter {param_name} value {value} above maximum {param.max}")
        
        return len(errors) == 0, errors