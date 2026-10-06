"""
Sandbox Orchestrator - Experimentation Layer
Member 2 - Cloud DNA Framework

This module handles:
1. Deploying candidate configurations to sandbox namespaces
2. Managing min_replicas=0 deployments  
3. Readiness probe alignment
4. Traffic dispatcher coordination
"""

import json
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SandboxOrchestrator:
    """
    Orchestrates sandbox deployments for candidate evaluation
    """
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        """Initialize Kubernetes client"""
        try:
            if kubeconfig_path:
                config.load_kube_config(config_file=kubeconfig_path)
            else:
                # Try in-cluster config first, fallback to local kubeconfig
                try:
                    config.load_incluster_config()
                except:
                    config.load_kube_config()
            
            self.apps_v1 = client.AppsV1Api()
            self.core_v1 = client.CoreV1Api()
            self.networking_v1 = client.NetworkingV1Api()
            
            logger.info("Kubernetes client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            raise
    
    async def deploy_candidates(
        self,
        candidates: List[Dict[str, Any]],
        base_deployment_config: Dict[str, Any],
        timeout_seconds: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Deploy multiple candidates to sandbox namespaces
        
        Args:
            candidates: List of candidate configurations
            base_deployment_config: Base deployment configuration from cdna.yaml
            timeout_seconds: Maximum time to wait for all deployments
            
        Returns:
            List of deployment results with readiness timestamps
        """
        logger.info(f"Deploying {len(candidates)} candidates to sandbox environments")
        
        deployment_tasks = []
        for candidate in candidates:
            task = asyncio.create_task(
                self._deploy_single_candidate(candidate, base_deployment_config)
            )
            deployment_tasks.append(task)
        
        # Wait for all deployments to complete
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*deployment_tasks, return_exceptions=True),
                timeout=timeout_seconds
            )
        except asyncio.TimeoutError:
            logger.error(f"Deployment timeout after {timeout_seconds} seconds")
            # Cancel remaining tasks
            for task in deployment_tasks:
                if not task.done():
                    task.cancel()
            raise
        
        # Process results and handle exceptions
        deployment_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Candidate {candidates[i]['candidate_id']} deployment failed: {result}")
                deployment_results.append({
                    "candidate_id": candidates[i]['candidate_id'],
                    "status": "failed",
                    "error": str(result),
                    "readiness_ts": None
                })
            else:
                deployment_results.append(result)
        
        # Wait for readiness alignment
        successful_deployments = [r for r in deployment_results if r['status'] == 'ready']
        if successful_deployments:
            alignment_ts = await self._align_readiness_windows(successful_deployments)
            logger.info(f"All candidates aligned at {alignment_ts}")
        
        return deployment_results
    
    async def _deploy_single_candidate(
        self,
        candidate: Dict[str, Any],
        base_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Deploy a single candidate to its sandbox namespace
        
        Args:
            candidate: Candidate configuration
            base_config: Base deployment configuration
            
        Returns:
            Deployment result with readiness status
        """
        candidate_id = candidate['candidate_id']
        genome = candidate['genome']
        
        logger.info(f"Deploying candidate {candidate_id}")
        
        try:
            # Create sandbox namespace
            namespace = f"sandbox-{candidate_id.lower()}"
            await self._ensure_namespace(namespace)
            
            # Generate deployment manifest
            deployment_manifest = self._create_deployment_manifest(
                candidate_id, genome, base_config, namespace
            )
            
            # Deploy to Kubernetes
            await self._apply_deployment(deployment_manifest, namespace)
            
            # Create service for the deployment
            service_manifest = self._create_service_manifest(candidate_id, namespace)
            await self._apply_service(service_manifest, namespace)
            
            # Wait for readiness
            readiness_ts = await self._wait_for_readiness(candidate_id, namespace)
            
            return {
                "candidate_id": candidate_id,
                "namespace": namespace,
                "status": "ready",
                "readiness_ts": readiness_ts,
                "deployment_name": f"medusa-{candidate_id.lower()}",
                "service_url": f"http://medusa-{candidate_id.lower()}.{namespace}.svc.cluster.local"
            }
            
        except Exception as e:
            logger.error(f"Failed to deploy candidate {candidate_id}: {e}")
            return {
                "candidate_id": candidate_id,
                "status": "failed",
                "error": str(e),
                "readiness_ts": None
            }
    
    async def _ensure_namespace(self, namespace: str):
        """Ensure namespace exists, create if not"""
        try:
            self.core_v1.read_namespace(name=namespace)
            logger.debug(f"Namespace {namespace} already exists")
        except ApiException as e:
            if e.status == 404:
                # Create namespace
                namespace_manifest = client.V1Namespace(
                    metadata=client.V1ObjectMeta(
                        name=namespace,
                        labels={
                            "app": "cloud-dna-sandbox",
                            "managed-by": "experimentation-layer"
                        }
                    )
                )
                self.core_v1.create_namespace(body=namespace_manifest)
                logger.info(f"Created namespace {namespace}")
            else:
                raise
    
    def _create_deployment_manifest(
        self,
        candidate_id: str,
        genome: Dict[str, Any],
        base_config: Dict[str, Any],
        namespace: str
    ) -> client.V1Deployment:
        """
        Create Kubernetes deployment manifest for candidate
        
        Args:
            candidate_id: Unique candidate identifier
            genome: Candidate genome configuration
            base_config: Base deployment configuration
            namespace: Target namespace
            
        Returns:
            Kubernetes deployment manifest
        """
        app_name = f"medusa-{candidate_id.lower()}"
        
        # Build environment variables from genome
        env_vars = []
        
        # Map genome parameters to environment variables
        if genome.get('node_thread_pool_size'):
            env_vars.append(
                client.V1EnvVar(name="UV_THREADPOOL_SIZE", value=str(genome['node_thread_pool_size']))
            )
        
        if genome.get('redis_cache_enabled'):
            env_vars.append(
                client.V1EnvVar(name="REDIS_CACHE_ENABLED", value=str(genome['redis_cache_enabled']).lower())
            )
            
        if genome.get('redis_cache_ttl_seconds'):
            env_vars.append(
                client.V1EnvVar(name="REDIS_CACHE_TTL", value=str(genome['redis_cache_ttl_seconds']))
            )
            
        if genome.get('db_connection_pool_max'):
            env_vars.append(
                client.V1EnvVar(name="DB_POOL_MAX", value=str(genome['db_connection_pool_max']))
            )
            
        if genome.get('rate_limiting_enabled'):
            env_vars.append(
                client.V1EnvVar(name="RATE_LIMITING_ENABLED", value=str(genome['rate_limiting_enabled']).lower())
            )
            
        if genome.get('rate_limit_rps'):
            env_vars.append(
                client.V1EnvVar(name="RATE_LIMIT_RPS", value=str(genome['rate_limit_rps']))
            )
        
        # Container definition
        container = client.V1Container(
            name="medusa",
            image=base_config.get('container_registry_image', 'acrclouddna.azurecr.io/medusa:latest'),
            ports=[client.V1ContainerPort(container_port=9000)],
            env=env_vars,
            resources=client.V1ResourceRequirements(
                requests={
                    "cpu": str(genome.get('cpu_cores', 1.0)),
                    "memory": f"{genome.get('memory_gb', 1.0)}Gi"
                },
                limits={
                    "cpu": str(genome.get('cpu_cores', 1.0)),
                    "memory": f"{genome.get('memory_gb', 1.0)}Gi"
                }
            ),
            readiness_probe=client.V1Probe(
                http_get=client.V1HTTPGetAction(
                    path="/health",
                    port=9000
                ),
                initial_delay_seconds=10,
                period_seconds=5,
                timeout_seconds=genome.get('health_check_timeout_sec', 2),
                failure_threshold=3
            ),
            liveness_probe=client.V1Probe(
                http_get=client.V1HTTPGetAction(
                    path="/health",
                    port=9000
                ),
                initial_delay_seconds=30,
                period_seconds=10,
                timeout_seconds=genome.get('health_check_timeout_sec', 2),
                failure_threshold=3
            )
        )
        
        # Pod template
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels={
                    "app": app_name,
                    "candidate-id": candidate_id,
                    "layer": "experimentation"
                }
            ),
            spec=client.V1PodSpec(containers=[container])
        )
        
        # Deployment spec
        deployment_spec = client.V1DeploymentSpec(
            replicas=0,  # Start with 0 replicas as per requirements
            selector=client.V1LabelSelector(
                match_labels={"app": app_name}
            ),
            template=pod_template
        )
        
        # Deployment manifest
        deployment = client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(
                name=app_name,
                namespace=namespace,
                labels={
                    "app": app_name,
                    "candidate-id": candidate_id,
                    "layer": "experimentation"
                }
            ),
            spec=deployment_spec
        )
        
        return deployment
    
    def _create_service_manifest(self, candidate_id: str, namespace: str) -> client.V1Service:
        """Create Kubernetes service manifest for candidate"""
        app_name = f"medusa-{candidate_id.lower()}"
        
        service = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=app_name,
                namespace=namespace,
                labels={
                    "app": app_name,
                    "candidate-id": candidate_id,
                    "layer": "experimentation"
                }
            ),
            spec=client.V1ServiceSpec(
                selector={"app": app_name},
                ports=[
                    client.V1ServicePort(
                        name="http",
                        port=80,
                        target_port=9000,
                        protocol="TCP"
                    )
                ],
                type="ClusterIP"
            )
        )
        
        return service
    
    async def _apply_deployment(self, deployment: client.V1Deployment, namespace: str):
        """Apply deployment to Kubernetes"""
        try:
            self.apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
            logger.info(f"Created deployment {deployment.metadata.name} in namespace {namespace}")
        except ApiException as e:
            if e.status == 409:  # Already exists
                self.apps_v1.patch_namespaced_deployment(
                    name=deployment.metadata.name,
                    namespace=namespace,
                    body=deployment
                )
                logger.info(f"Updated deployment {deployment.metadata.name} in namespace {namespace}")
            else:
                raise
    
    async def _apply_service(self, service: client.V1Service, namespace: str):
        """Apply service to Kubernetes"""
        try:
            self.core_v1.create_namespaced_service(namespace=namespace, body=service)
            logger.info(f"Created service {service.metadata.name} in namespace {namespace}")
        except ApiException as e:
            if e.status == 409:  # Already exists
                self.core_v1.patch_namespaced_service(
                    name=service.metadata.name,
                    namespace=namespace,
                    body=service
                )
                logger.info(f"Updated service {service.metadata.name} in namespace {namespace}")
            else:
                raise
    
    async def _wait_for_readiness(
        self,
        candidate_id: str,
        namespace: str,
        timeout_seconds: int = 300
    ) -> str:
        """
        Wait for candidate deployment to become ready
        
        Args:
            candidate_id: Candidate identifier
            namespace: Kubernetes namespace
            timeout_seconds: Maximum time to wait
            
        Returns:
            ISO timestamp when deployment became ready
        """
        app_name = f"medusa-{candidate_id.lower()}"
        start_time = time.time()
        
        logger.info(f"Waiting for candidate {candidate_id} to become ready...")
        
        # First scale up to target replica count (this should be derived from genome)
        # For now, start with 1 replica for evaluation
        deployment = self.apps_v1.read_namespaced_deployment(name=app_name, namespace=namespace)
        deployment.spec.replicas = 1
        
        self.apps_v1.patch_namespaced_deployment(
            name=app_name,
            namespace=namespace,
            body=deployment
        )
        
        while time.time() - start_time < timeout_seconds:
            try:
                deployment = self.apps_v1.read_namespaced_deployment(name=app_name, namespace=namespace)
                
                if (deployment.status.ready_replicas and 
                    deployment.status.ready_replicas >= 1):
                    
                    readiness_ts = datetime.utcnow().isoformat() + "Z"
                    logger.info(f"Candidate {candidate_id} is ready at {readiness_ts}")
                    return readiness_ts
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.warning(f"Error checking readiness for {candidate_id}: {e}")
                await asyncio.sleep(5)
        
        raise TimeoutError(f"Candidate {candidate_id} did not become ready within {timeout_seconds} seconds")
    
    async def _align_readiness_windows(
        self,
        successful_deployments: List[Dict[str, Any]]
    ) -> str:
        """
        Align all candidates to start evaluation at the same time
        Wait for the slowest candidate to be ready before proceeding
        
        Args:
            successful_deployments: List of successful deployment results
            
        Returns:
            Aligned start timestamp for evaluation
        """
        logger.info("Aligning readiness windows for synchronized evaluation")
        
        # Find the latest readiness timestamp
        latest_ready_time = max(
            datetime.fromisoformat(dep['readiness_ts'].replace('Z', '+00:00'))
            for dep in successful_deployments
        )
        
        # Add a small buffer to ensure all services are truly ready
        alignment_time = latest_ready_time + timedelta(seconds=10)
        alignment_ts = alignment_time.isoformat() + "Z"
        
        logger.info(f"Evaluation window aligned to start at {alignment_ts}")
        return alignment_ts
    
    async def cleanup_candidates(self, candidate_ids: List[str]):
        """
        Clean up sandbox deployments after evaluation
        
        Args:
            candidate_ids: List of candidate IDs to clean up
        """
        logger.info(f"Cleaning up {len(candidate_ids)} candidate deployments")
        
        for candidate_id in candidate_ids:
            try:
                namespace = f"sandbox-{candidate_id.lower()}"
                app_name = f"medusa-{candidate_id.lower()}"
                
                # Delete deployment
                self.apps_v1.delete_namespaced_deployment(
                    name=app_name,
                    namespace=namespace,
                    body=client.V1DeleteOptions(propagation_policy='Background')
                )
                
                # Delete service
                self.core_v1.delete_namespaced_service(
                    name=app_name,
                    namespace=namespace
                )
                
                # Delete namespace (this will clean up everything)
                self.core_v1.delete_namespace(
                    name=namespace,
                    body=client.V1DeleteOptions(propagation_policy='Background')
                )
                
                logger.info(f"Cleaned up candidate {candidate_id}")
                
            except Exception as e:
                logger.error(f"Failed to cleanup candidate {candidate_id}: {e}")
    
    async def get_deployment_status(self, candidate_id: str) -> Dict[str, Any]:
        """Get current status of a candidate deployment"""
        namespace = f"sandbox-{candidate_id.lower()}"
        app_name = f"medusa-{candidate_id.lower()}"
        
        try:
            deployment = self.apps_v1.read_namespaced_deployment(name=app_name, namespace=namespace)
            
            return {
                "candidate_id": candidate_id,
                "namespace": namespace,
                "replicas": deployment.spec.replicas,
                "ready_replicas": deployment.status.ready_replicas or 0,
                "available_replicas": deployment.status.available_replicas or 0,
                "status": "ready" if deployment.status.ready_replicas else "not_ready"
            }
            
        except ApiException as e:
            if e.status == 404:
                return {
                    "candidate_id": candidate_id,
                    "status": "not_found"
                }
            else:
                raise