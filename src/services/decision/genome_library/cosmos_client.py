def get_genome_library(app_id: str, cosmos_client) -> list[dict]:
    container = cosmos_client.get_database_client("clouddna").get_container_client("genome_library")
    return list(container.query_items(
        query="SELECT * FROM c WHERE c.app_id = @app_id",
        parameters=[{"name": "@app_id", "value": app_id}],
        enable_cross_partition_query=False,
    ))