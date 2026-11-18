#!/bin/bash

# Container name
CONTAINER_NAME="voice-agent-hub"

# Build the image
echo "Building Docker image..."
docker build -t $CONTAINER_NAME .
