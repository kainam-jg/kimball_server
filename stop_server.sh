#!/bin/bash

PORT="8000"

echo "Checking for process on port $PORT..."

# Extract just the PID using grep + sed
PID=$(ss -ltnp "sport = :$PORT" | grep -oP 'pid=\K[0-9]+')

if [ -z "$PID" ]; then
    echo "No process found running on port $PORT."
else
    echo "Found PID $PID using port $PORT"

    PGID=$(ps -o pgid= -p $PID | grep -o '[0-9]*')
    echo "Killing full process group for PGID $PGID..."
    kill -TERM -$PGID
    echo "Sent SIGTERM to process group $PGID"
fi

