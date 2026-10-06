def promote(winning_genome: dict, app_id: str, deploy_fn) -> None:
    """deploy_fn: injected callable that actually applies config — kubectl patch,
    or trigger an Azure DevOps pipeline release. Kept as a parameter so this function
    is testable without touching a real cluster."""
    deploy_fn(app_id, winning_genome)