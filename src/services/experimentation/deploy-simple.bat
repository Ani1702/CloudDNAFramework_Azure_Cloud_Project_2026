@echo off
echo Deploying Experimentation Layer via Azure CLI
echo.

REM Set variables
set RESOURCE_GROUP=azure-cloud-dna-rg
set FUNCTION_APP_NAME=clouddna-experimentation-member2
set STORAGE_ACCOUNT_NAME=clouddnaexpstore%RANDOM%
set LOCATION=centralindia

echo Creating storage account...
az storage account create ^
  --name %STORAGE_ACCOUNT_NAME% ^
  --location %LOCATION% ^
  --resource-group %RESOURCE_GROUP% ^
  --sku Standard_LRS

echo Creating Function App...
az functionapp create ^
  --resource-group %RESOURCE_GROUP% ^
  --consumption-plan-location %LOCATION% ^
  --runtime python ^
  --runtime-version 3.11 ^
  --functions-version 4 ^
  --name %FUNCTION_APP_NAME% ^
  --os-type windows ^
  --storage-account %STORAGE_ACCOUNT_NAME%

echo Function App created successfully!
echo Name: %FUNCTION_APP_NAME%
echo.
echo Next step: Deploy your Python code to the Function App
pause