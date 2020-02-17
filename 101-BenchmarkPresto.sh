# Run all TPCDS queries on Presto
# Query presto for runtimes after you are done
set -e 

# Warmup
echo "Warmup.sql"
presto --catalog hive --schema tpcds_hdfs -f Warmup.sql > /dev/null

# Run each query
ls query/*.sql | while read f;
do 
  echo "$f"
  presto --catalog=hive --schema tpcds_hdfs -f $f > /dev/null
done