# Run all TPCDS queries on Presto
# Query presto for runtimes after you are done
# Run this on the dataproc cluster master
set -e 

# Read all tables to warm up google cloud storage
echo 'Warmup.sql...'
presto --catalog=hive --schema=tpcds_parquet -f Warmup.sql > /dev/null

# Run each query
for f in query/*.sql; 
do
  echo $f
  presto --catalog=hive --schema tpcds_parquet -f $f > /dev/null
done

presto --catalog=hive --schema tpcds_parquet -f PrestoTiming.sql