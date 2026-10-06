from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger("clouddna.genome_library")


class GenomeLibraryClient:
    def __init__(self, cosmos_client: Optional[Any] = None, database_name: str = "clouddna", container_name: str = "genome_library"):
        self.container = None
        self._local_cache: Dict[str, List[Dict[str, Any]]] = {}

        if cosmos_client is not None:
            try:
                self.container = cosmos_client.get_database_client(database_name).get_container_client(container_name)
                logger.info("GenomeLibraryClient connected to Cosmos DB container: %s", container_name)
            except Exception as e:
                logger.warning("Could not connect to Cosmos DB genome_library (%s). Using local store.", e)
        else:
            logger.info("GenomeLibraryClient initialized in local in-memory mode.")

    def get_genome_library(self, app_id: str) -> List[Dict[str, Any]]:
        if self.container:
            try:
                query = "SELECT * FROM c WHERE c.app_id = @app_id"
                parameters = [{"name": "@app_id", "value": app_id}]
                items = list(self.container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=False
                ))
                return items
            except Exception as e:
                logger.error("Error querying genome_library from Cosmos DB: %s", e)

        return self._local_cache.get(app_id, [])

    def record_promoted_genome(self, app_id: str, record: Dict[str, Any]):
        if app_id not in self._local_cache:
            self._local_cache[app_id] = []
        self._local_cache[app_id].append(record)

        if self.container:
            try:
                self.container.upsert_item(record)
            except Exception as e:
                logger.error("Failed to persist promoted genome to Cosmos DB: %s", e)


def get_genome_library(app_id: str, cosmos_client: Optional[Any] = None) -> List[Dict[str, Any]]:
    client = GenomeLibraryClient(cosmos_client)
    return client.get_genome_library(app_id)