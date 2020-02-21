set -e 

export HOST=tpcds-benchmark.cw43lptekopo.us-east-1.redshift.amazonaws.com
export DB=dev
export PGPASSWORD=NumeroFoo0
export USER=tpcds_user

echo 'Warmup.sql...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done < Warmup.sql

for FILE in query/*.sql; 
do
  echo $FILE
  sed -i '' -e 's/Substr(/Substring(/g' $FILE
  psql \
    --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --output /dev/null \
    --file ${FILE}
done