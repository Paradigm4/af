#!/bin/bash
#Quick restart script for dev use
iquery -aq "unload_library('af')" > /dev/null 2>&1
set -e

DBNAME="mydb"
#This is easily sym-linkable: ~/scidb
SCIDB_INSTALL="/opt/scidb/19.11"
export SCIDB_THIRDPARTY_PREFIX="/opt/scidb/19.11"

mydir=`dirname $0`
pushd $mydir
make SCIDB=$SCIDB_INSTALL

scidbctl.py stop $DBNAME
cp libaf.so ${SCIDB_INSTALL}/lib/scidb/plugins/
scidbctl.py start $DBNAME

iquery -aq "load_library('af')"
