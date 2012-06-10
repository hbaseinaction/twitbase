#!/bin/sh

HBASE_CLI="$HBASE_HOME/bin/hbase"

test -n "$HBASE_HOME" || {
  echo >&2 'HBASE_HOME not set. using hbase on $PATH'
  HBASE_CLI=$(which hbase)
}

TWITS_TABLE=${TWITS_TABLE-'twits'}
TWITS_FAM=${TWITS_FAM-'twits'}
USERS_TABLE=${USERS_TABLE-'users'}
USERS_FAM=${USERS_FAM-'info'}
FOLLOWS_TABLE=${FOLLOWS_TABLE-'follows'}
FOLLOWS_FAM=${FOLLOWS_FAM-'follows'}
FOLLOWED_TABLE=${FOLLOWED_TABLE-'followed'}
FOLLOWED_FAM=${FOLLOWED_FAM-'followed'}

exec "$HBASE_CLI" shell <<EOF
create '$TWITS_TABLE',
  {NAME => '$TWITS_FAM', VERSIONS => 1}

create '$USERS_TABLE',
  {NAME => '$USERS_FAM'}

create '$FOLLOWS_TABLE',
  {NAME => '$FOLLOWS_FAM', VERSIONS => 1}

create '$FOLLOWED_TABLE',
  {NAME => '$FOLLOWED_FAM', VERSIONS => 1}
EOF
