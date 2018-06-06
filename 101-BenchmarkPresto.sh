# Run all TPCDS queries on Presto
# Query presto for runtimes after you are done
# Run this on the dataproc cluster master
set -e 

# Randomize the order if $1 is present
if [ -z $1 ]; then 
  ls query/*.sql > order$1.txt
else 
  ls query/*.sql | sort -R > order$1.txt
fi

# Run each query
while read f;
do 
  echo "$f"
  presto --catalog=hive --schema tpcds_hdfs -f $f > /dev/null
done < order$1.txt