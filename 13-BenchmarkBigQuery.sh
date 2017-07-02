set -e

ACCOUNT=singular-vector-135519

# Warm-up
while read line;
do 
  echo "$line"
  bq --project_id=${ACCOUNT} --dataset_id=tpcds query --use_legacy_sql=false "$line"
done < Warmup.sql

# Test
echo "Query,Started,Ended,Billing Tier,Bytes" > BigQueryResults.csv
for f in query/*.sql; 
do
  echo $f
  ID=$(echo $f | tr / _ | tr . _)
  cat $f | bq --project_id=${ACCOUNT} --dataset_id=tpcds query --use_legacy_sql=false --batch=false --job_id=$ID
  JOB=$(bq --project_id=${ACCOUNT} --format=prettyjson show -j ${ID})
  STARTED=$(echo $JOB | json statistics.startTime)
  ENDED=$(echo $JOB | json statistics.endTime)
  BILLING_TIER=$(echo $JOB | json statistics.query.billingTier)
  BYTES=$(echo $JOB | json statistics.query.totalBytesBilled)
  echo "$f,$STARTED,$ENDED,$BILLING_TIER,$BYTES" >> BigQueryResults.csv
done
