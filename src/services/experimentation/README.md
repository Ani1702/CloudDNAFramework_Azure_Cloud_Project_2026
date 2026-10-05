# Experimentation Layer - Member 2

## Overview

The Experimentation Layer is responsible for generating, deploying, and evaluating candidate configurations in response to trigger events from the Decision Layer. This layer implements the core "genetic algorithm" aspect of the Cloud DNA Framework by creating configuration variants and testing them in isolated sandbox environments.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Decision Layer │────│ Event Grid Topic │────│ Function Trigger│
│   (Member 3)    │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Experimentation Coordinator                      │
├─────────────────┬─────────────────┬─────────────────┬───────────────┤
│ Candidate       │ Sandbox         │ Traffic         │ Metrics       │
│ Generator       │ Orchestrator    │ Dispatcher      │ Collector     │
│                 │                 │                 │               │
│ • Optuna        │ • Kubernetes    │ • Event Hub     │ • Performance │
│ • Bayesian      │ • Deployments   │ • Traffic       │ • Cost        │
│ • Guardrails    │ • Namespaces    │   Replay        │ • Security    │
│ • Historical    │ • Services      │ • Security      │ • Scaling     │
│   Seeding       │ • Readiness     │   Probes        │ • Recovery    │
└─────────────────┴─────────────────┴─────────────────┴───────────────┘
                                    │
                                    ▼
                         ┌──────────────────┐
                         │   Cosmos DB      │
                         │ Evaluation       │
                         │   Results        │
                         └──────────────────┘
```

## Components

### 1. Candidate Generator (`candidate_generator.py`)

**Purpose**: Generate configuration candidates using Bayesian optimization and rule-based constraints.

**Key Features**:
- **Optuna Integration**: Uses Bayesian optimization (TPE sampler) for intelligent parameter search
- **Constraint Validation**: Enforces min/max limits from `cdna.yaml` configuration
- **Historical Seeding**: Warm-starts optimization with successful historical configurations
- **LLM Prior Integration**: Accepts narrowed parameter ranges from Nemotron (via Member 3's client)
- **Trigger-Aware Mutations**: Focuses parameter changes based on trigger category

**Input**: 
- Trigger event from Decision Layer
- Historical genomes from Cosmos DB
- Optional LLM-suggested parameter ranges

**Output**: 
- List of candidate genome configurations
- Generation metadata (method, timestamp)

### 2. Sandbox Orchestrator (`sandbox_orchestrator.py`)

**Purpose**: Deploy candidate configurations to isolated Kubernetes sandbox environments.

**Key Features**:
- **Namespace Isolation**: Each candidate gets its own namespace (`sandbox-{candidate-id}`)
- **Zero-Downtime Deployment**: Starts with `replicas=0`, scales up for evaluation
- **Resource Management**: Applies CPU/memory limits from genome configuration
- **Readiness Alignment**: Waits for all candidates to be ready before starting evaluation
- **Automatic Cleanup**: Removes sandbox resources after evaluation completes

**Input**: 
- List of candidate configurations
- Base deployment template from `cdna.yaml`

**Output**: 
- Deployment status and service URLs for each candidate
- Synchronized readiness timestamps

### 3. Traffic Dispatcher (`traffic_dispatcher.py`)

**Purpose**: Replay buffered traffic to all candidates simultaneously and collect performance metrics.

**Key Features**:
- **Traffic Replay**: Retrieves buffered requests from Event Hub and replays to all candidates
- **Synthetic Traffic**: Generates realistic traffic patterns when Event Hub is unavailable
- **Security Probes**: Tests candidate resilience with SQL injection, XSS, brute force attempts
- **Parallel Dispatch**: Sends identical traffic to all candidates simultaneously
- **Metrics Collection**: Gathers latency, throughput, error rates, and security probe results

**Input**: 
- Candidate deployment endpoints
- Buffered traffic from Event Hub
- Evaluation window duration

**Output**: 
- Performance metrics (latency percentiles, throughput, error rates)
- Security probe results (blocked attacks, failure rates)

### 4. Evaluation Coordinator (`evaluation_coordinator.py`)

**Purpose**: Orchestrate the complete experimentation workflow from trigger to results.

**Key Features**:
- **End-to-End Orchestration**: Manages the complete evaluation pipeline
- **Resource Coordination**: Coordinates between generator, orchestrator, and dispatcher
- **Results Processing**: Enriches metrics with cost, scaling, and recovery calculations
- **Database Integration**: Stores evaluation results in Cosmos DB for Action Layer consumption
- **Error Handling**: Implements cleanup and recovery for failed evaluations

**Input**: 
- Trigger events from Event Grid (Decision Layer)

**Output**: 
- Complete candidate evaluation results stored in Cosmos DB
- Formatted according to schema specification

## Azure Function App (`function_app.py`)

**Purpose**: Serverless entry points for the experimentation workflow.

**Endpoints**:
- `ExperimentationTrigger`: Event Grid trigger for processing trigger events
- `ExperimentationHealthCheck`: Health monitoring endpoint
- `ManualExperimentationTrigger`: Manual testing endpoint
- `GetExperimentationStatus`: Query evaluation status

## Data Flow

1. **Trigger Receipt**: Event Grid delivers trigger event when `fast_path_match.found = false`
2. **Historical Query**: Retrieve top-k similar genomes from Cosmos DB genome library
3. **Candidate Generation**: Use Optuna + constraints to generate N candidates (max 4)
4. **Sandbox Deployment**: Deploy candidates to isolated Kubernetes namespaces
5. **Traffic Collection**: Retrieve buffered traffic from Event Hub (60s window)
6. **Evaluation Execution**: Replay traffic + security probes to all candidates (90s window)
7. **Metrics Processing**: Calculate performance, cost, security, scaling, recovery metrics
8. **Results Storage**: Store evaluation results in Cosmos DB candidate_evaluations container
9. **Cleanup**: Remove sandbox deployments and temporary resources

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `COSMOS_ENDPOINT` | Cosmos DB endpoint URL | Yes |
| `EVENT_HUB_CONNECTION_STRING` | Event Hub connection for traffic buffer | No* |
| `APP_INSIGHTS_KEY` | Application Insights instrumentation key | Yes |
| `AzureWebJobsStorage` | Storage account for function app | Yes |

*When not provided, synthetic traffic is generated for testing

### CDNA.yaml Integration

The layer reads configuration from the `cdna.yaml` manifest:

```yaml
sandbox:
  shadow_replica_min: 0
  max_concurrent_candidates: 4  # Limits parallel evaluations
  evaluation_window_sec: 90     # How long to run each evaluation

tunable_parameters:            # Defines optimization search space
  - name: replica_count
    type: int
    min: 1
    max: 10
    # ... additional parameters
```

## Deployment

### Prerequisites
- Azure CLI installed and configured
- Kubernetes cluster (AKS) with appropriate permissions
- Cosmos DB account with required containers
- Event Hub namespace for traffic mirroring

### Infrastructure Deployment

```bash
# Deploy infrastructure
az deployment group create \
  --resource-group azure-cloud-dna-rg \
  --template-file bicep/experimentation-infrastructure.bicep

# Deploy function code
./deploy.sh
```

### Manual Testing

```bash
# Health check
curl https://clouddna-exp-dev-func.azurewebsites.net/api/experimentation/health

# Manual trigger with sample event
curl -X POST https://clouddna-exp-dev-func.azurewebsites.net/api/experimentation/trigger \
  -H "Content-Type: application/json" \
  -d @sample-trigger-event.json
```

## Schema Compliance

### Input Schema: `trigger_event`

```json
{
  "app_id": "medusa",
  "trigger_id": "trig-20260915-001", 
  "timestamp": "2026-09-15T12:00:00Z",
  "trigger_category": "TRAFFIC_SPIKE",
  "severity": "HIGH",
  "current_production_config": { "cpu_cores": 1.0, "..." },
  "fast_path_match": { "found": false }
}
```

### Output Schema: `candidate_evaluation_result`

```json
{
  "trigger_id": "trig-20260915-001",
  "candidate_id": "cand-001", 
  "genome": { "cpu_cores": 2.0, "memory_gb": 2.0, "..." },
  "generation_method": "bayesian_optimizer",
  "readiness_ts": "2026-09-15T12:01:12Z",
  "raw_metrics": {
    "performance": { "p50_latency_ms": 180.0, "..." },
    "cost": { "hourly_compute_cost_usd": 0.15 },
    "security": { "failed_auth_attempts_per_min": 0, "..." },
    "scaling": { "current_replica_count": 5, "..." },
    "recovery": { "restart_count": 0, "..." }
  },
  "security_probe_results": { "auth_bruteforce_blocked": true, "..." }
}
```

## Monitoring and Debugging

### Application Insights Queries

```kusto
// Function execution times
requests 
| where name == "ExperimentationTrigger"
| summarize avg(duration), max(duration), count() by bin(timestamp, 5m)

// Error analysis  
exceptions
| where outerMessage contains "experimentation"
| summarize count() by outerMessage, bin(timestamp, 1h)

// Candidate evaluation success rate
traces
| where message contains "Completed experimentation"
| extend candidates_evaluated = toint(extract(@"generated (\d+) candidate", 1, message))
| summarize avg(candidates_evaluated), count() by bin(timestamp, 1h)
```

### Common Issues

1. **Kubernetes Permission Errors**: Ensure Function App managed identity has AKS Cluster User role
2. **Cosmos DB Access Denied**: Verify Cosmos DB Data Contributor role assignment  
3. **Event Hub Connection**: Check Event Hub connection string and Data Receiver role
4. **Container Image Pull**: Ensure ACR Pull role for accessing Medusa container images
5. **Resource Quotas**: AKS cluster may hit CPU/memory quotas with multiple sandbox deployments

## Testing Strategy

### Unit Tests
- Candidate generation with various parameter constraints
- Genome validation against cdna.yaml rules
- Cost calculation accuracy
- Security probe result processing

### Integration Tests  
- End-to-end trigger event processing
- Kubernetes deployment and cleanup
- Cosmos DB result storage
- Event Hub traffic collection

### Load Tests
- Multiple concurrent trigger events
- Large candidate evaluation sets
- Extended evaluation windows
- Resource cleanup under failure conditions

## Performance Considerations

### Optimization Targets
- **Evaluation Latency**: Complete evaluation within 5 minutes
- **Resource Efficiency**: Minimize AKS resource usage during evaluation
- **Concurrent Capacity**: Handle up to 4 trigger events simultaneously 
- **Storage Throughput**: Store results without Cosmos DB throttling

### Scaling Limits
- **Max Candidates per Trigger**: 4 (configurable in cdna.yaml)
- **Evaluation Window**: 90 seconds (configurable)
- **Traffic Replay Buffer**: 60 seconds, 1000 requests max
- **Kubernetes Namespaces**: Auto-cleanup prevents accumulation

## Future Enhancements

1. **Advanced Optimization**: Multi-objective optimization, constraint handling
2. **Intelligent Seeding**: Vector similarity search for historical genomes  
3. **Dynamic Evaluation**: Adaptive evaluation windows based on traffic patterns
4. **Cost Optimization**: Spot instance usage for sandbox deployments
5. **ML Integration**: Reinforcement learning for parameter space exploration

## Dependencies

### External Services
- **Azure Kubernetes Service**: Sandbox deployment platform
- **Azure Cosmos DB**: Results storage and historical genome lookup
- **Azure Event Hub**: Buffered traffic source
- **Azure Event Grid**: Trigger event delivery from Decision Layer
- **Azure Container Registry**: Medusa container image storage

### Python Packages
- `optuna`: Bayesian optimization framework
- `kubernetes`: Kubernetes API client
- `azure-cosmos`: Cosmos DB SDK
- `azure-eventhub`: Event Hub SDK
- `aiohttp`: Async HTTP client for traffic dispatch
- `numpy`: Numerical operations for metrics calculation

## Contact

**Owner**: Member 2  
**Layer**: Experimentation  
**Dependencies**: Decision Layer (input), Action Layer (output)  
**Tools**: Optuna, Azure Container Apps/AKS, Azure Functions, Python