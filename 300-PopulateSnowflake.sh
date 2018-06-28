#!/bin/sh

set -e 

s3Path=`echo "$1" | sed -Ee 's;/;\\\\/;g'`
warehouse="$2"
db="$3"

export SNOWFLAKE_WAREHOUSE="$warehouse"
export SNOWFLAKE_DATABASE="$db"

echo warehouse=$SNOWFLAKE_WAREHOUSE database=$SNOWFLAKE_DATABASE

cleanupFiles=""

cleanup() {
  rm -v $cleanupFiles
}
trap cleanup TERM KILL

echo "Running create table ddl..." 1>&2
bash SnowflakeQueryRunner.sh ddl SnowflakeTableDdl.sql
echo "Completed create table ddl..." 1>&2

tmpfile=`mktemp SnowflakeCopy_XXXXXXXXXXX.sql`
cat SnowflakeCopy.sql | sed -Ee "s/___s3_path__/${s3Path}/g" \
  | tee "$tmpfile"
cleanupFiles="$tmpfile $cleanupFiles"

echo "Running copy ddl..." 1>&2
bash SnowflakeQueryRunner.sh ddl "$tmpfile"

cleanup
