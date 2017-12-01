# Run all TPCDS queries on Presto
# Query presto for runtimes after you are done
# Run this on the dataproc cluster master
set -e 

SCHEMA=tpcds_parquet_50mb

# Read all tables to warm up google cloud storage
echo 'Warmup.sql...'
while read line;
do
  echo "$line"
  presto --catalog=hive --schema=${SCHEMA} --execute "$line" > /dev/null
done < Warmup.sql

# Run each query
for f in query/*.sql; 
do
  echo "$f"
  presto --catalog=hive --schema ${SCHEMA} -f $f > /dev/null
done

presto --catalog=hive --schema ${SCHEMA} -f PrestoTiming.sql