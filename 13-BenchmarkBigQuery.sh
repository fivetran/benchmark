set -e

ACCOUNT=singular-vector-135519

# Warm-up
while read line;
do 
  echo "$line"
  bq --project_id=${ACCOUNT} --dataset_id=tpcds \
    query --use_legacy_sql=false --batch=false --format=none <<< $line
done < Warmup.sql

# Test
mkdir -p results
echo "Query,Started,Ended,Billing Tier,Bytes" > results/BigQueryResults.csv
for f in query/*.sql; 
do
  echo $f
  RM_DIR=${f#query/}
  QUERY=${RM_DIR%.sql}
  ID=${QUERY}_$(date +%s)
  bq --project_id=${ACCOUNT} --dataset_id=tpcds \
    query --use_legacy_sql=false --batch=false --maximum_billing_tier=10 --job_id=$ID --format=none < $f
  JOB=$(bq --project_id=${ACCOUNT} --format=json show -j ${ID})
  STARTED=$(json statistics.startTime <<< $JOB )
  ENDED=$(json statistics.endTime <<< $JOB )
  BILLING_TIER=$(json statistics.query.billingTier <<< $JOB )
  BYTES=$(json statistics.query.totalBytesBilled <<< $JOB )
  echo "$f,$STARTED,$ENDED,$BILLING_TIER,$BYTES" >> results/BigQueryResults.csv
done
