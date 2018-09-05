#!/bin/bash

set -e 

base=`pwd`
datadir=$1
tmpfile=$2

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
  rm $tmpfile
}
trap cleanup TERM KILL

echo "Running create table ddl..." 1>&2
bash AzureQueryRunner.sh ddl AzureTableClear.sql || :
bash AzureQueryRunner.sh ddl AzureTableDdl.sql
echo "Completed create table ddl..." 1>&2


function upload() {
  table=$1
  shift 1
  files=$@
  echo Loading ${table}...
  echo $@ \
    | tr ' ' '\0' \
    | xargs -0 -t -P4 -n1 -I__ \
      bash $base/AzureQueryRunner.sh bcp $table in __
}

cd $1
upload call_center `find . -type f -regex ".*/call_center_[0-9_]+\.dat"`
upload catalog_page `find . -type f -regex ".*/catalog_page_[0-9_]+\.dat"`
upload catalog_returns `find . -type f -regex ".*/catalog_returns_[0-9_]+\.dat"`
upload catalog_sales `find . -type f -regex ".*/catalog_sales_[0-9_]+\.dat"`
upload customer `find . -type f -regex ".*/customer_[0-9_]+\.dat"`
upload customer_address `find . -type f -regex ".*/customer_address_[0-9_]+\.dat"`
upload customer_demographics `find . -type f -regex ".*/customer_demographics_[0-9_]+\.dat"`
upload date_dim `find . -type f -regex ".*/date_dim_[0-9_]+\.dat"`
upload household_demographics `find . -type f -regex ".*/household_demographics_[0-9_]+\.dat"`
upload income_band `find . -type f -regex ".*/income_band_[0-9_]+\.dat"`
upload inventory `find . -type f -regex ".*/inventory_[0-9_]+\.dat"`
upload item `find . -type f -regex ".*/item_[0-9_]+\.dat"`
upload promotion `find . -type f -regex ".*/promotion_[0-9_]+\.dat"`
upload reason `find . -type f -regex ".*/reason_[0-9_]+\.dat"`
upload ship_mode `find . -type f -regex ".*/ship_mode_[0-9_]+\.dat"`
upload store `find . -type f -regex ".*/store_[0-9_]+\.dat"`
upload store_returns `find . -type f -regex ".*/store_returns_[0-9_]+\.dat"`
upload store_sales `find . -type f -regex ".*/store_sales_[0-9_]+\.dat"`
upload time_dim `find . -type f -regex ".*/time_dim_[0-9_]+\.dat"`
upload warehouse `find . -type f -regex ".*/warehouse_[0-9_]+\.dat"`
upload web_page `find . -type f -regex ".*/web_page_[0-9_]+\.dat"`
upload web_returns `find . -type f -regex ".*/web_returns_[0-9_]+\.dat"`
upload web_sales `find . -type f -regex ".*/web_sales_[0-9_]+\.dat"`
upload web_site `find . -type f -regex ".*/web_site_[0-9_]+\.dat"`

cleanup
