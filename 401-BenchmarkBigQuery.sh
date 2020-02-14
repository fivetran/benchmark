#!/bin/bash
set -e

export PROJECT=singular-vector-135519
export DATASET=tpcds_100

# Warm-up
find warmup/ -name warmup_*.sql | sort -V | {
  while read line; do 
    echo "$line"
    cat "$line" | bq --project_id=${PROJECT} --dataset_id=${DATASET} \
      query \
      --use_legacy_sql=false \
      --batch=false \
      --format=none
  done
}

# Test
mkdir -p results
echo "Query,Started,Ended,Billing Tier,Bytes" > results/BigQueryResults.csv

find query/ -name query*.sql | sort -V | {
  while read -r f; do
    echo $f
    QUERY=`basename $f | head -c -5`
    ID=${QUERY}_$(date +%s)

    cat "$f" \
      | bq \
        --project_id=${PROJECT} \
        --dataset_id=${DATASET} \
        query \
        --use_cache=false \
        --use_legacy_sql=false \
        --batch=false \
        --maximum_billing_tier=10 \
        --job_id=$ID \
        --format=none

    JOB=$(bq --project_id=${PROJECT} --format=json show -j ${ID})

    STARTED=$(json statistics.startTime <<< $JOB )
    ENDED=$(json statistics.endTime <<< $JOB )

    BILLING_TIER=$(json statistics.query.billingTier <<< $JOB )
    BYTES=$(json statistics.query.totalBytesBilled <<< $JOB )

    echo "$f,$STARTED,$ENDED,$BILLING_TIER,$BYTES" >> results/BigQueryResults.csv
  done
}
