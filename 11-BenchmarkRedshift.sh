# Run this locally
# Don't forget to export PGPASSWORD=?
set -e 

HOST=tpcds.cw43lptekopo.us-east-1.redshift.amazonaws.com
DB=tpcds
USER=developers
export PGPASSWORD=Charteau1

echo 'Create tables...'
psql --host ${HOST} --port 5439 --user ${USER} ${DB} -f PopulateRedshift.sql 

echo 'Warmup.sql...'
psql --host ${HOST} --port 5439 --user ${USER} ${DB} -f Warmup.sql > /dev/null

for f in query/*.sql; 
do
  echo $f
  time psql --host ${HOST} --port 5439 --user ${USER} ${DB} -f $f > /dev/null
done

psql --host ${HOST} --port 5439 --user ${USER} ${DB} -f RedshiftTiming.sql