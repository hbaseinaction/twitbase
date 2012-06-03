#!/bin/sh

HBASE_CLI="$HBASE_HOME/bin/hbase"

test -n "$HBASE_HOME" || {
  echo >&2 'HBASE_HOME not set. using hbase on $PATH'
  HBASE_CLI=$(which hbase)
}

TWITS_TABLE=${TWITS_TABLE-'twits'}
TWITS_FAM=${TWITS_FAM-'t'}
USERS_TABLE=${USERS_TABLE-'users'}
USERS_FAM=${USERS_FAM-'info'}
FOLLOWERS_TABLE=${FOLLOWERS_TABLE-'followers'}
FOLLOWERS_FAM=${FOLLOWERS_FAM-'f'}

exec "$HBASE_CLI" shell <<EOF
create '$TWITS_TABLE',
  {NAME => '$TWITS_FAM', VERSIONS => 1}

create '$USERS_TABLE',
  {NAME => '$USERS_FAM'}

create '$FOLLOWERS_TABLE',
  {NAME => '$FOLLOWERS_FAM', VERSIONS => 1}
EOF
