#!/bin/bash

# Configuration
PROJECT_ID="suny-prj-grants-ai-dev"
REGION="us-central1"
REPO_NAME="suny-gar-grants-ai-docker-repo"
SERVICE_NAME="pure-data-pipeline"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:latest"

echo "Step 1: Building Docker image..."
gcloud builds submit --tag $IMAGE_NAME .

if [ $? -ne 0 ]; then
  echo "Docker build failed!"
  exit 1
fi

echo "Step 2: Deploying Cloud Run Job..."
# Deploying as a Job because it's a batch process
gcloud run jobs deploy $SERVICE_NAME \
  --image=$IMAGE_NAME \
  --region=$REGION \
  --set-env-vars="PROJECT_ID=${PROJECT_ID}" \
  --set-env-vars="BQ_DATASET=grantai" \
  --set-env-vars="GCS_BUCKET=pure-data-pipeline-raw-dev" \
  --set-env-vars="API_KEY=cbdb3c44-a0b9-406a-a26e-cd17ebb20608" \
  --set-env-vars="GEMINI_API_KEY=AIzaSyBdnfqcVWHaQnb8CnB3tumAV_hVIw_2F1I"

if [ $? -eq 0 ]; then
  echo "Deployment successful!"
else
  echo "Deployment failed!"
  exit 1
fi