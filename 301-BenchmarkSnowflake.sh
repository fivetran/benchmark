#!/bin/sh
set -e 

warehouse="$1"
db="$2"
runName="$3"

export SNOWFLAKE_WAREHOUSE="$warehouse"
export SNOWFLAKE_DATABASE="$db"

if [ -z "$runName" ]; then
  runName=`head -c 4 /dev/urandom | xxd -p`
fi

echo warehouse=$SNOWFLAKE_WAREHOUSE database=$SNOWFLAKE_DATABASE

for qu in `find warmup/ -type f -name 'warmup*.sql' | sort -V` ; do
  echo "Warmup $qu..."
  ./SnowflakeQueryRunner.sh ddl "$qu"
done

mkdir -p results

output=results/SnowflakeResults_${runName}.csv
echo Saving output to $output
if [ -f $output ]; then
  temp=`mktemp results/SnowflakeResults_XXXXXXXXX.csv`
  echo "moving previous timing run to $temp"
  mv -v $output $temp
fi

echo 'Query,Time' > $output
for qu in `find query/ -type f -name 'query*.sql' | sort -V` ; do
  echo "Running $qu..."
  ./SnowflakeQueryRunner.sh timing "$qu" >> $output
done
