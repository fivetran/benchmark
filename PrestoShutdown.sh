#!/bin/bash
curl -H 'Content-Type: application/json' -X PUT -d '"SHUTTING_DOWN"' http://localhost:8080/v1/info/state
