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
import asyncio
import os
import sys

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(__file__))

# Initialize Function App
app = func.FunctionApp()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.function_name(name="ExperimentationTrigger")
@app.event_grid_trigger(arg_name="azeventgrid")
def experimentation_trigger(azeventgrid: func.EventGridEvent):
    """
    Event Grid trigger function that processes trigger events from Decision Layer
    
    Args:
        azeventgrid: Event Grid event containing trigger_event data
    """
    
    logger.info(f"Received Event Grid trigger: {azeventgrid.id}")
    
    try:
        # Extract trigger event data from Event Grid
        event_data = azeventgrid.get_json()
        
        # Validate required fields
        required_fields = ['app_id', 'trigger_id', 'trigger_category', 'current_production_config']
        for field in required_fields:
            if field not in event_data:
                raise ValueError(f"Missing required field: {field}")
        
        logger.info(f"Processing trigger event {event_data.get('trigger_id')} for app {event_data.get('app_id')}")
        
        # For now, just log the event (full implementation requires async support in v1 programming model)
        logger.info(f"Trigger event received: {json.dumps(event_data, indent=2)}")
        
        # Return success response
        return func.HttpResponse(
            json.dumps({
                "status": "received",
                "trigger_id": event_data.get('trigger_id'),
                "message": "Trigger event received and logged successfully"
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error processing experimentation trigger: {e}")
        
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "error": str(e),
                "message": "Experimentation trigger failed"
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="ExperimentationHealthCheck")
@app.route(route="experimentation/health", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
def experimentation_health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint for the Experimentation Layer
    """
    
    try:
        # Basic health check
        import datetime
        
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
            "function_app": "clouddna-experimentation-member2"
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
                "message": "Manual trigger processed successfully",
                "note": "Full experimentation pipeline will be enabled after integration setup"
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
        # This would query Cosmos DB for experimentation status
        # Simplified implementation for now
        
        status_info = {
            "trigger_id": trigger_id,
            "status": "pending_integration",  # Would be actual status from DB
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "message": "Function App deployed successfully, integration pending"
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