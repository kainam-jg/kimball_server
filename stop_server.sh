#!/bin/bash

PORT="8000"

echo "Checking for process on port $PORT..."

# Get the PID of the process listening on the given port
PID=$(ss -ltnp "sport = :$PORT" | awk 'NR>1 {gsub(/pid=|,/, "", $NF); print $NF}')

if [ -z "$PID" ]; then
    echo "No process found running on port $PORT."
else
    echo "Found PID $PID using port $PORT"

    echo "Killing full process group for PID $PID..."
    PGID=$(ps -o pgid= -p $PID | grep -o '[0-9]*')
    kill -TERM -$PGID
    echo "Sent SIGTERM to process group $PGID"
fi

