# Run this locally
# Don't forget to export PGPASSWORD=?
set -e 

echo 'Warmup.sql...'
psql --host tpcds.cw43lptekopo.us-east-1.redshift.amazonaws.com --port 5439 --user developers tpcds -f Warmup.sql > /dev/null

for f in redshift/*.sql; 
do
  echo $f
  time psql --host tpcds.cw43lptekopo.us-east-1.redshift.amazonaws.com --port 5439 --user developers tpcds -f $f > /dev/null
done

psql --host tpcds.cw43lptekopo.us-east-1.redshift.amazonaws.com --port 5439 --user developers tpcds -f RedshiftTiming.sql