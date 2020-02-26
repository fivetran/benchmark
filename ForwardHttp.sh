# Forward 8080 to remote worker so we can view the web UI
gcloud compute \
    --project "digital-arbor-400" \
    ssh tpcds-presto-m \
    --ssh-flag="-4NL" \
    --ssh-flag="8080:tpcds-presto-m:8080"