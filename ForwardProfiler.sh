# Forward 10999 to remote worker so we can profile it
gcloud compute ssh tpcds-w-0 \
    --zone "us-central1-f" \
    --ssh-flag="-4NL" \
    --ssh-flag="10999:localhost:10999"