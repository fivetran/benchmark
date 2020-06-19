#!/bin/bash
set -e

export PROJECT=fivetran-bq-reserved
export DATASET=tpcds_1000

# Warm-up
echo 'Warmup.sql...'
while read line;
do
    echo "$line"
    echo "$line" | bq --project_id=${PROJECT} --dataset_id=${DATASET} \
      query \
      --use_legacy_sql=false \
      --batch=false \
      --format=none
done < Warmup.sql

# Test
echo "Query,Started,Ended,Billing Tier,Bytes" > BigQueryResults.csv

for FILE in query/*.sql; 
do
    echo ${FILE}
    QUERY=`basename ${FILE} | head -c 7`
    ID=${QUERY}_$(date +%s)

    cat "${FILE}" \
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

    echo "${FILE},$STARTED,$ENDED,$BILLING_TIER,$BYTES" >> results/BigQueryResults.csv
done