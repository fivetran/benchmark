# You will probably need to run this on an EC2 instance or it will fail in the middle
# See 6-ConnectToEc2Instance.sh
# Don't forget to export PGPASSWORD=?
set -e 

HOST=tpcds.cw43lptekopo.us-east-1.redshift.amazonaws.com
DB=public
USER=tpcds_user
export PGPASSWORD=OaklandOffice1

echo 'Warmup.sql...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done < Warmup.sql

echo 'Query,Time' > RedshiftResults.csv
for FILE in query/*.sql; 
do
  echo $FILE
  sed -i -e 's/Substr/Substring/g' $FILE
  /usr/bin/time -f "%e" psql \
    --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --output /dev/null \
    --file ${FILE} &> time.txt
  RUNTIME=$(cat time.txt)
  echo "Elapsed: ${RUNTIME}s"
  echo ${FILE},${RUNTIME} >> RedshiftResults.csv
done