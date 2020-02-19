#!/bin/sh
set -e 

cat Warmup.sql 

ls query/*.sql | while read f;
do 
  cat $f
done