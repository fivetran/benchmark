# Run export SNOWSQL_PWD=<password> before running this script
set -e 

echo 'Warmup.sql...'
while read line;
do
    snowsql -q "$line" \
        --accountname fivetran \
        --username tpcds_user \
        --dbname tpcds \
        --schemaname public \
        --warehouse tpcds 
done < Warmup.sql

echo 'Query,Time' > RedshiftResults.csv
for FILE in query/*.sql; 
do
    echo $FILE
    /usr/bin/time -f "%e" snowsql -f $FILE \
        --accountname fivetran \
        --username tpcds_user \
        --dbname tpcds \
        --schemaname public \
        --warehouse tpcds 
    RUNTIME=$(cat time.txt)
    echo "Elapsed: ${RUNTIME}s"
    echo ${FILE},${RUNTIME} >> RedshiftResults.csv
done