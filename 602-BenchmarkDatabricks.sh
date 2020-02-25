
echo "-- Don't forget to set `spark.sql.crossJoin.enable true` in your spark config"
cat Warmup.sql 

ls query/*.sql | while read f;
do 
  cat <<EOF

-- COMMAND ----------

EOF
  cat $f
done
