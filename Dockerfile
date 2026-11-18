# Use an official Python runtime as a parent image
FROM docker.m.daocloud.io/python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Add build parameters for environment selection
ARG ENV=test

# Copy the requirements file into the container at /app
COPY project/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

# Remove requirements.txt
RUN rm /app/requirements.txt

# Copy the app.py file into the container at /app
# COPY ./project /app

# Set environment variables
ENV ENV=${ENV}
ENV PYTHONPATH=/app
ENV PORT=8001

# Expose the port
EXPOSE ${PORT}

# Run the uvicorn server when the container launches
# --host 0.0.0.0 makes the server accessible from outside the container
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--log-config", "log_conf.yaml", "--timeout-graceful-shutdown", "30"]
