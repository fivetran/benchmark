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

function runsql() {

  snowsql --noup \
    -o output_format=csv \
    -o auto_completion=False \
    -o timing=True \
    -o header=False \
    -o friendly=False \
    -o rowset_size=10000 \
    -w tpcds -d tpcds -s PUBLIC
}

function cleanup() {
  rm _outsql _query_output 2>/dev/null || :
}

function runtime() {
  cleanup

  mkfifo _outsql
  trap cleanup TERM KILL

  touch _query_output
  exec 5<> _outsql

  snowsql --noup \
    -o output_format=csv \
    -o auto_completion=False \
    -o timing=True \
    -o friendly=False \
    -o header=False \
    -o rowset_size=10000 \
    -w tpcds -d tpcds -s PUBLIC \
    -f "$1" 1>&5 &
  PID=$!

  {
    while read -r line <&5 ; do
      if echo "$line" | grep -q "Time Elapsed" ; then
        # example: 100 Row(s) produced. Time Elapsed: 31.334s
        echo "$line" | awk '{print substr($6, 1, length($6) - 1)}'
        break
      else
        echo $line >> _query_output
      fi
    done
  }
  head -n15 _query_output >&7

  wait $PID
  exec 5>&-
  cleanup
}

if [ "$1" = "timing" ]; then
  time=`runtime "$2" 7>&2`
  printf "%s,%.5f\n" "$2" "$time"
else
  runsql
fi
