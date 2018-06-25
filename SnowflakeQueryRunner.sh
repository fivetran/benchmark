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
  rm _{in,out}sql _query_output 2>/dev/null || :
}

function runtime() {
  cleanup

  mkfifo _insql
  mkfifo _outsql
  trap cleanup TERM KILL

  touch _query_output
  exec 4<> _insql
  exec 5<> _outsql

  snowsql --noup \
    -o output_format=csv \
    -o auto_completion=False \
    -o timing=True \
    -o header=False \
    -o friendly=False \
    -o rowset_size=10000 \
    -w tpcds -d tpcds -s PUBLIC <&4 1>&5 &
  PID=$!

  echo "select 1;" >&4
  while read -r line <&5 ; do
    if echo "$line" | grep -q "Time Elapsed" ; then
      break
    fi
  done

  while read -r line ; do
    echo "$line" >&4
  done
  echo "!exit" >&4
  {
    time {
      while read -r line <&5 ; do
        if echo "$line" | grep -q "Time Elapsed" ; then
          echo "snowsql: $line" >&7
          break
        else
          echo $line >> _query_output
        fi
      done
    }
  } 2>&1 \
    | awk '/^real/{split($2, arr, "[a-z]"); print (arr[1] * 60) + arr[2]}'

  head -n15 _query_output >&7

  wait $PID
  exec 4>&-
  exec 5>&-
  cleanup
}

if [ "$1" = "timing" ]; then
  {
    sqlTime=`runtime "$2" 7>&2`
    printf "%s\t%8.4f\n" "$2" "$sqlTime"
  } | tee -a timing_output.txt
else
  runsql
fi
