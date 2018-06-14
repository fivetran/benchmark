# Forward 8080 to remote worker so we can profile it
gcloud compute ssh tpcds-presto-m \
    --zone "us-central1-f" \
    --ssh-flag="-4NL" \
    --ssh-flag="8088:tpcds-presto-m:8088" &
gcloud compute ssh tpcds-presto-m \
    --zone "us-central1-f" \
    --ssh-flag="-4NL" \
    --ssh-flag="8080:tpcds-presto-m:8080"