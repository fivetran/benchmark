#!/bin/sh

set -e

export SNOWSQL_ACCOUNT=fivetran
export SNOWSQL_USER=taylor
if [ ! -f ~/.snowflake_password ]; then
  echo "No password set, do this:"
  echo "echo {mypassword} > ~/.snowflake_password"
  exit 1
fi
export SNOWSQL_PWD=`cat ~/.snowflake_password`

if [ -z "$SNOWFLAKE_WAREHOUSE" ] || \
   [ -z "$SNOWFLAKE_DATABASE" ]; then
  echo "missing \$SNOWFLAKE_WAREHOUSE or \$SNOWFLAKE_DATABASE" 1>&2
  SNOWFLAKE_WAREHOUSE=tpcds
  SNOWFLAKE_DATABASE=tpcds
fi

tempdir=`mktemp -d _work_XXXXXXXXXX`

cleanup() {
  rm -rf "$tempdir" 2>/dev/null || :
}
trap cleanup TERM KILL

runsql() {

  snowsql --noup \
    -o output_format=csv \
    -o auto_completion=False \
    -o timing=True \
    -o header=False \
    -o friendly=False \
    -o rowset_size=10000 \
    -w "$SNOWFLAKE_WAREHOUSE" -d "$SNOWFLAKE_DATABASE" -s PUBLIC
}

runtime() {

  mkfifo $tempdir/outsql

  touch $tempdir/output

  snowsql --noup \
    -o output_format=csv \
    -o auto_completion=False \
    -o timing=True \
    -o friendly=False \
    -o header=False \
    -o rowset_size=10000 \
    -w "$SNOWFLAKE_WAREHOUSE" -d "$SNOWFLAKE_DATABASE" -s PUBLIC \
    -f "$1" > $tempdir/outsql &
  PID=$!

  cat $tempdir/outsql | {
    while read -r line ; do
      if [ "$multi" -eq 0 ] && echo "$line" | grep -q "Time Elapsed" ; then
        # example: 100 Row(s) produced. Time Elapsed: 31.334s
        echo "$line" | awk '{print substr($6, 1, length($6) - 1)}'
        break
      elif [ "$verbose" = 1 ]; then
        echo "snowsql: $line" 1>&7
      else
        echo $line >> $tempdir/output
      fi
    done
  }

  head -n15 $tempdir/output >&7

  wait $PID
  cleanup
}

if [ "$1" = "timing" ]; then
  export multi=0
  time=`runtime "$2" 7>&2`
  printf "%s,%.5f\n" "$2" "$time"
elif [ "$1" = "ddl" ]; then
  export verbose=1
  export multi=1
  runtime "$2" 7>&2
else
  runsql
fi
