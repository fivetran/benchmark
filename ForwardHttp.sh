# Forward 8080 to remote worker so we can profile it
CLUSTER=tpcds-50mb

gcloud compute ssh ${CLUSTER}-m \
    --ssh-flag="-4NL" \
    --ssh-flag="8080:localhost:8080"