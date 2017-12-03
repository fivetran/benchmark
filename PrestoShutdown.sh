#!/bin/bash
curl -H 'Content-Type: application/json' -X PUT -d '"SHUTTING_DOWN"' http://localhost:8080/v1/info/state

HOSTNAME=\"$(hostname)\"
STATE=$(curl http://localhost:8080/v1/info/state)
gcloud logging write presto-shutdown "{\"hostname\":$HOSTNAME,\"state\":$STATE}" --payload-type json

# Wait 5s for coordinator to acknowledge worker is shutting down
sleep 5

# Wait for all tasks to finish running
while true; do 
    TASKS=$(curl http://localhost:8080/v1/task)
    gcloud logging write presto-shutdown "{\"hostname\":$HOSTNAME,\"state\":$STATE,\"tasks\":$TASKS}" --payload-type json
    if [ $TASKS = '[]' ]; then 
        exit 0
    fi
    sleep 1
done