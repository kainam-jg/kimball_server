#!/bin/bash

PORT="8000"
PID=$(lsof -ti :$PORT)

if [ -z "$PID" ]; then
    echo "No process found running on port $PORT."
else
    echo "Killing entire process group for PID $PID running on port $PORT..."
    kill -TERM -$(ps -o pgid= $PID | grep -o '[0-9]*')
    echo "Sent SIGTERM to process group."
fi

