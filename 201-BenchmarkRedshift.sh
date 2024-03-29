set -e 

export HOST=benchmark-05x.cnr9x5mk2fpk.us-east-2.redshift.amazonaws.com
export DB=dev
export PGPASSWORD=AtAtDocks1
export USER=tpcds_user

echo 'Warmup.sql...'
while read line;
do
  date
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done < Warmup.sql

for FILE in query/*.sql; 
do
  echo $FILE
  date
  sed -i '' -e 's/Substr(/Substring(/g' $FILE
  psql \
    --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --output /dev/null \
    --file ${FILE}
done