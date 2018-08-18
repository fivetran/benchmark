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
  temp=`mktemp results/SnowflakeResults_${runName}_XXXXXXXXX.csv`
  echo "moving previous timing run to $temp"
  mv -v $output $temp
fi

echo 'Query,Time' > $output
for qu in `find query/ -type f -name 'query*.sql' | sort -V` ; do
  echo "Running $qu..."
  {
    echo -n "$qu,"
    ./AzureQueryRunner.sh timing "$qu"
  } >> $output
done

cleanup
