#!/bin/bash
set -e

export ACCOUNT=singular-vector-135519
export PROJECT=tpcds


if [ -n "$1" ] && [ -n "$2" ]; then
  export ACCOUNT="$1"
  export PROJECT="$2"
fi
echo account: $ACCOUNT project: $PROJECT >&2

# Warm-up
find warmup/ -name warmup_*.sql | sort -V | {
  while read line; do 
    echo "$line"
    cat "$line" | bq --project_id=${ACCOUNT} --dataset_id=${PROJECT} \
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
        --project_id=${ACCOUNT} \
        --dataset_id=${PROJECT} \
        query \
        --use_legacy_sql=false \
        --batch=false \
        --maximum_billing_tier=10 \
        --job_id=$ID \
        --format=none

    JOB=$(bq --project_id=${ACCOUNT} --format=json show -j ${ID})

    STARTED=$(json statistics.startTime <<< $JOB )
    ENDED=$(json statistics.endTime <<< $JOB )

    BILLING_TIER=$(json statistics.query.billingTier <<< $JOB )
    BYTES=$(json statistics.query.totalBytesBilled <<< $JOB )

    echo "$f,$STARTED,$ENDED,$BILLING_TIER,$BYTES" >> results/BigQueryResults.csv
  done
}
