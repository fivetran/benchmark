#!/bin/sh

set -e 

s3Path=`echo "$1" | sed -Ee 's;/;\\\\/;g'`
warehouse="$2"
db="$3"

export AZURE_ACCOUNT=fivetran
export AZURE_USER=developers
if [ ! -f ~/.azure_password ]; then
  echo "No password set, do this:"
  echo "echo {mypassword} > ~/.azure_password"
  exit 1
fi
export AZURE_PWD=`cat ~/.azure_password`

if [ -z "$AZURE_WAREHOUSE" ] || \
   [ -z "$AZURE_DATABASE" ]; then
  echo "missing \$AZURE_WAREHOUSE or \$AZURE_DATABASE" 1>&2
  AZURE_WAREHOUSE=tpcds
  AZURE_DATABASE=tpcds
fi

tempdir=`mktemp -d _work_XXXXXXXXXX`

cleanup() {
  rm -rf "$tempdir" 2>/dev/null || :
}
trap cleanup TERM KILL

echo "Running create table ddl..." 1>&2
bash AzureQueryRunner.sh ddl AzureTableDdl.sql
echo "Completed create table ddl..." 1>&2

exit 1

tmpfile=`mktemp AzureCopy_XXXXXXXXXXX.sql`
cat AzureCopy.sql | sed -Ee "s/___s3_path__/${s3Path}/g" \
  | tee "$tmpfile"
cleanupFiles="$tmpfile $cleanupFiles"

echo "Running copy ddl..." 1>&2
bash AzureQueryRunner.sh ddl "$tmpfile"

cleanup
