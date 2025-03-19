#!/bin/bash

# Define the port the server is running on
PORT="8000"

# Find the process ID (PID) using the port
PID=$(lsof -ti :$PORT)

# Check if a process is found
if [ -z "$PID" ]; then
    echo "No process found running on port $PORT."
else
    # Kill the process
    kill -9 $PID
    echo "Killed process $PID running on port $PORT."
fi
