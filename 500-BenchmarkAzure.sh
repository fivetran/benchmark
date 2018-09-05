#!/bin/bash

set -e 

tempdir=`mktemp -d _work_XXXXXXXXXX`

cleanup() {
  rm -rf "$tempdir" 2>/dev/null || :
}
trap cleanup TERM KILL

runName="$1"

if [ -z "$runName" ]; then
  runName=`head -c 4 /dev/urandom | xxd -p`
fi

mkdir -p results
output=results/AzureResults_${runName}.csv
echo Saving output to $output
if [ -f $output ]; then
  temp=`mktemp results/AzureResults${runName}_XXXXXXXXX.csv`
  echo "moving previous timing run to $temp"
  mv -v $output $temp
fi

echo "Warming up..."
for qu in `find microsoft_sql/ -type f -name 'warmup*.sql' | sort -V` ; do
  ./AzureQueryRunner.sh ddl "$qu"
done

echo 'Query,Time' > $output
for qu in `find microsoft_sql/ -type f -name 'query*.sql' | sort -V` ; do
  echo "Running $qu..."
  {
    echo -n "$qu,"
    ./AzureQueryRunner.sh timing "$qu"
  } >> $output
done

cleanup
