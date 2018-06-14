
gcloud compute \
      --project "digital-arbor-400" \
      instances delete "tpcds-presto-m" \
      --zone "us-central1-f" \
      --quiet &
gcloud compute \
      --project "digital-arbor-400" \
      instance-groups managed delete "tpcds-presto-w" \
      --zone "us-central1-f" \
      --quiet