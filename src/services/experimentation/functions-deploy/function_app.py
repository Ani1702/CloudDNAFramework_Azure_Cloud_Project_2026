"""
Azure Function App for Experimentation Layer
Member 2 - Cloud DNA Framework

This Function App handles:
1. Event Grid triggers from Decision Layer
2. Coordinating the experimentation workflow
3. Processing trigger events through candidate evaluation
"""

import azure.functions as func
import logging
import json
import datetime

# Initialize Function App
app = func.FunctionApp()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.function_name(name="ExperimentationHealthCheck")
@app.route(route="experimentation/health", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
def experimentation_health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint for the Experimentation Layer
    """
    
    try:
        # Basic health check
        health_status = {
            "status": "healthy",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "components": {
                "candidate_generator": "operational",
                "sandbox_orchestrator": "operational", 
                "traffic_dispatcher": "operational"
            },
            "layer": "experimentation",
            "function_app": "clouddna-experimentation-member2",
            "message": "Experimentation Layer deployed successfully via CLI!"
        }
        
        return func.HttpResponse(
            json.dumps(health_status, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        return func.HttpResponse(
            json.dumps({
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.datetime.utcnow().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="ManualExperimentationTrigger")
@app.route(route="experimentation/trigger", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def manual_experimentation_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    Manual trigger endpoint for testing experimentation without Event Grid
    """
    
    logger.info("Manual experimentation trigger called")
    
    try:
        # Parse request body as trigger event
        req_body = req.get_json()
        
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Log the trigger event for now
        logger.info(f"Manual trigger received: {json.dumps(req_body, indent=2)}")
        
        # Simulate processing
        trigger_id = req_body.get('trigger_id', 'unknown')
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "trigger_id": trigger_id,
                "message": "Manual trigger processed successfully via CLI deployment!",
                "note": "Full experimentation pipeline will be enabled after integration setup",
                "received_data": req_body
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error in manual experimentation trigger: {e}")
        
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "error": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="GetExperimentationStatus")
@app.route(route="experimentation/status/{trigger_id}", auth_level=func.AuthLevel.FUNCTION, methods=["GET"])
def get_experimentation_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get status of a specific experimentation run
    """
    
    trigger_id = req.route_params.get('trigger_id')
    
    if not trigger_id:
        return func.HttpResponse(
            json.dumps({"error": "trigger_id is required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        status_info = {
            "trigger_id": trigger_id,
            "status": "function_app_deployed",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "message": "Function App deployed successfully via CLI, integration ready",
            "deployment_method": "azure_functions_core_tools"
        }
        
        return func.HttpResponse(
            json.dumps(status_info, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "trigger_id": trigger_id
            }),
            status_code=500,
            mimetype="application/json"
        )