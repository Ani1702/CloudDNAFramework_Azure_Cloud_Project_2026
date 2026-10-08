#!/bin/bash

# Deployment script for Experimentation Layer
# Member 2 - Cloud DNA Framework

set -e  # Exit on any error

# Configuration
RESOURCE_GROUP="azure-cloud-dna-rg"
LOCATION="eastus"
FUNCTION_APP_NAME="clouddna-exp-dev-func"
COSMOS_DB_NAME="clouddna-cosmos"
EVENT_GRID_TOPIC_NAME="clouddna-decision-events"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Deploying Experimentation Layer Infrastructure${NC}"

# Check if logged in to Azure
if ! az account show > /dev/null 2>&1; then
    echo -e "${RED}❌ Not logged in to Azure. Please run 'az login' first.${NC}"
    exit 1
fi

# Check if resource group exists
if ! az group show --name $RESOURCE_GROUP > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Resource group $RESOURCE_GROUP not found. Creating...${NC}"
    az group create --name $RESOURCE_GROUP --location $LOCATION
fi

echo -e "${GREEN}📦 Deploying infrastructure with Bicep template...${NC}"

# Deploy infrastructure
az deployment group create \
    --resource-group $RESOURCE_GROUP \
    --template-file bicep/experimentation-infrastructure.bicep \
    --parameters \
        namePrefix="clouddna-exp" \
        environment="dev" \
        cosmosDbAccountName=$COSMOS_DB_NAME \
        eventGridTopicName=$EVENT_GRID_TOPIC_NAME

echo -e "${GREEN}✅ Infrastructure deployed successfully${NC}"

# Build and package function app
echo -e "${GREEN}📦 Building and packaging function app...${NC}"

# Create deployment package
rm -rf deployment_package
mkdir deployment_package

# Copy function files
cp -r *.py deployment_package/
cp requirements.txt deployment_package/
cp host.json deployment_package/

# Create zip package
cd deployment_package
zip -r ../function-app.zip .
cd ..

echo -e "${GREEN}📤 Deploying function app code...${NC}"

# Deploy function app code
az functionapp deployment source config-zip \
    --resource-group $RESOURCE_GROUP \
    --name $FUNCTION_APP_NAME \
    --src function-app.zip

# Set additional app settings
echo -e "${GREEN}⚙️  Configuring function app settings...${NC}"

# Get Cosmos DB connection details
COSMOS_ENDPOINT=$(az cosmosdb show --resource-group $RESOURCE_GROUP --name $COSMOS_DB_NAME --query documentEndpoint -o tsv)

# Update function app settings
az functionapp config appsettings set \
    --resource-group $RESOURCE_GROUP \
    --name $FUNCTION_APP_NAME \
    --settings \
        "COSMOS_ENDPOINT=$COSMOS_ENDPOINT"

# Get function app URL
FUNCTION_APP_URL=$(az functionapp show --resource-group $RESOURCE_GROUP --name $FUNCTION_APP_NAME --query defaultHostName -o tsv)

echo -e "${GREEN}✅ Experimentation Layer deployment completed successfully!${NC}"
echo ""
echo -e "${GREEN}📊 Deployment Summary:${NC}"
echo -e "  Function App Name: ${YELLOW}$FUNCTION_APP_NAME${NC}"
echo -e "  Function App URL:  ${YELLOW}https://$FUNCTION_APP_URL${NC}"
echo -e "  Resource Group:    ${YELLOW}$RESOURCE_GROUP${NC}"
echo ""
echo -e "${GREEN}🧪 Test endpoints:${NC}"
echo -e "  Health Check:      ${YELLOW}https://$FUNCTION_APP_URL/api/experimentation/health${NC}"
echo -e "  Manual Trigger:    ${YELLOW}https://$FUNCTION_APP_URL/api/experimentation/trigger${NC}"
echo ""
echo -e "${GREEN}📝 Next steps:${NC}"
echo "  1. Configure Event Hub connection string in function app settings"
echo "  2. Test the manual trigger endpoint with a sample trigger event"
echo "  3. Verify Event Grid subscription is receiving events from Decision Layer"
echo "  4. Monitor function execution logs in Application Insights"

# Cleanup temporary files
rm -rf deployment_package
rm -f function-app.zip

echo -e "${GREEN}🎉 Deployment complete!${NC}"