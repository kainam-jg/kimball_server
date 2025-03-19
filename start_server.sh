#!/bin/bash

# Define variables
HOST="0.0.0.0"
PORT="8000"
APP_MODULE="main:app"  # Updated to start FastAPI from main.py
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/server.log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Start FastAPI server with Uvicorn, running in the background and logging output
echo "Starting FastAPI server on $HOST:$PORT..."
nohup uvicorn "$APP_MODULE" --host "$HOST" --port "$PORT" --reload > "$LOG_FILE" 2>&1 &

# Save the process ID (PID) for easy management
echo $! > fastapi_server.pid
echo "FastAPI server started with PID: $(cat fastapi_server.pid)"
echo "Logs are being saved to $LOG_FILE"