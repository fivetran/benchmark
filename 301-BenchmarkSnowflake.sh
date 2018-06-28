#!/bin/sh
set -e 

for qu in `find warmup/ -type f -name 'query*.sql' | sort -V` ; do
  echo "Warmup $qu..."
  bash ./SnowflakeQueryRunner.sh timing "$qu"
done

if [ -f SnowflakeResults.csv ]; then
  temp=`mktemp SnowflakeResults_XXXXXXXXX.csv`
  echo "moving previous timing run to $temp"
  mv -v SnowflakeResults.csv $temp
fi

echo 'Query,Time' > SnowflakeResults.csv
for qu in `find query/ -type f -name 'query*.sql' | sort -V` ; do
  echo "Running $qu..."
  bash ./SnowflakeQueryRunner.sh timing "$qu" >> SnowflakeResults.csv
done
