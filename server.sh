#!/bin/bash

# 默认环境
DEFAULT_ENV="test"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            ENV="$2"
            shift
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [-e|--env test|uat|prod]"
            echo "  -e, --env    Set environment (test or uat or prod), default is test"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# 设置环境变量，如果没有指定则使用默认值
ENV=${ENV:-$DEFAULT_ENV}

# Container name
CONTAINER_NAME="voice-agent-hub"

# Check if container exists and remove it
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping existing container..."
    docker stop $CONTAINER_NAME
    echo "Removing existing container..."
    docker rm $CONTAINER_NAME
fi

mkdir -p "$(pwd)/project/logs"
mkdir -p "$(pwd)/project/dbfiles"

# Run new container
echo "Starting new container..."
docker run -d \
    --net host \
    --name $CONTAINER_NAME \
    -p 0.0.0.0:8001:8001 \
    -v "$(pwd)/project":/app \
    -e TZ="Asia/Shanghai" \
    -e ENV=$ENV \
    $CONTAINER_NAME 
    
echo "Container started successfully!"
echo "API is available at http://localhost:8001"

docker logs --tail 20 $CONTAINER_NAME
