#!/bin/bash

if [ -z $1 ]; then
    echo usage "./$0 <kudu_table_list_file>"
    exit 1
fi

WORKDIR=`readlink -f $0 | xargs dirname`
source ${WORKDIR}/../profile_kudu

TABLE_LIST=$1

cat $KUDU_COPY_BASE/meta/$TABLE_LIST | while read LINE
do
    TBL_NAME=`echo ${LINE} | awk -F"::" '{print $2}'`
    LOG_FILE=$T{TBL_NAME}_`date +%Y%m%d_%H%M%S`.log

    sh $KUDU_COPY_BASE/kudu_copy.sh ${LINE} > $WORKDIR/logs/$LOG_FILE 2>&1
    echo "Log file: $WORKDIR/logs/$LOG_FILE"
done

