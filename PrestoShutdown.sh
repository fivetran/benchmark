#!/bin/bash
curl -H 'Content-Type: application/json' -X PUT -d '"SHUTTING_DOWN"' http://localhost:8080/v1/info/state

HOSTNAME=\"$(hostname)\"

# Wait for all tasks to finish running
while true; do 
    STATE=$(curl http://localhost:8080/v1/info/state)
    TASKS=$(curl http://localhost:8080/v1/task)
    gcloud logging write presto-shutdown "{\"hostname\":$HOSTNAME,\"state\":$STATE,\"tasks\":$TASKS}" --payload-type json
    sleep 1
    if [ $TASKS = '[]' ]; then 
        exit 0
    fi
done