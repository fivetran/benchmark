set -e 

# Set HOST, PGPASSWORD, OUTPUT manually
if [ -z "$HOST" ]; then 
  echo "You must set HOST"
  exit 1
fi 
if [ -z "$PGPASSWORD" ]; then 
  echo "You must set PGPASSWORD"
  exit 1
fi 
if [ -z "$OUTPUT" ]; then 
  echo "You must set OUTPUT"
  exit 1
fi 

DB=dev
USER=tpcds_user

echo 'Warmup.sql...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done < Warmup.sql

echo 'Query,Time' > ${OUTPUT}
for FILE in query/*.sql; 
do
  echo $FILE
  sed -i -e 's/Substr(/Substring(/g' $FILE
  /usr/bin/time -f "%e" psql \
    --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --output /dev/null \
    --file query-${OUTPUT}.sql &> time-${OUTPUT}.txt
  RUNTIME=$(cat time-${OUTPUT}.txt)
  echo "Elapsed: ${RUNTIME}s"
  echo ${FILE},${RUNTIME} >> ${OUTPUT}
done