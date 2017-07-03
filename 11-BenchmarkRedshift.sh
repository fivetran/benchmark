# You will probably need to run this on an EC2 instance or it will fail in the middle
# See 6-ConnectToEc2Instance.sh
# Don't forget to export PGPASSWORD=?
set -e 

HOST=tpcds.cw43lptekopo.us-east-1.redshift.amazonaws.com
DB=tpcds
USER=developers
export PGPASSWORD=Charteau1

echo 'Create tables...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done < PopulateRedshift.sql

echo 'Warmup.sql...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done < Warmup.sql

for f in query/*.sql; 
do
  echo $f
  SQL=$( cat $f | sed -e 's/Substr/Substring/g' )
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --output /dev/null \
    --command "$SQL"
done

echo 'RedshiftTiming.sql...'
mkdir -p results
psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
  --no-align --field-separator '\t' --output results/RedshiftResults.tsv \
  --file results/RedshiftTiming.sql