#!/bin/bash

set -e 

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

function upload() {
  table=$1
  shift 1
  files=$@
  echo $files
  cat $files | sed 's/|$//g' > $tempdir/output
  head -n3 $tempdir/output
  bash ./AzureQueryRunner.sh bcp $table in $tempdir/output
}

upload call_center call_center/*            
upload catalog_page catalog_page/*           
upload catalog_returns catalog_returns/*        
upload catalog_sales catalog_sales/*          
upload customer customer/*               
upload customer_address customer_address/*       
upload customer_demographics customer_demographics/*  
upload date_dim date_dim/*               
upload household_demographics household_demographics/* 
upload income_band income_band/*            
upload inventory inventory/*              
upload item item/*                   
upload promotion promotion/*              
upload reason reason/*                 
upload ship_mode ship_mode/*              
upload store store/*                  
upload store_returns store_returns/*          
upload store_sales store_sales/*            
upload time_dim time_dim/*               
upload warehouse warehouse/*              
upload web_page web_page/*               
upload web_returns web_returns/*            
upload web_sales web_sales/*              
upload web_site web_site/*               

cleanup
