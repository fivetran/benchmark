#!/bin/sh

set -e

export AZURE_ACCOUNT=fivetran
export AZURE_USER=developers
if [ ! -f ~/.azure_password ]; then
  echo "No password set, do this:"
  echo "echo {mypassword} > ~/.azure_password"
  exit 1
fi
export AZURE_PWD=`cat ~/.azure_password`

if [ -z "$AZURE_WAREHOUSE" ] || \
   [ -z "$AZURE_DATABASE" ]; then
  echo "missing \$AZURE_WAREHOUSE or \$AZURE_DATABASE" 1>&2
  AZURE_WAREHOUSE=tpcds
  AZURE_DATABASE=tpcds
fi

sqlcmd -l 10 -N -b -m-1 -j -I -p \
  -S ${AZURE_WAREHOUSE}.database.windows.net \
  -U developers@tpcds \
  -P "${AZURE_PWD}" \
  -d "${AZURE_DATABASE}" \
  "$@"
