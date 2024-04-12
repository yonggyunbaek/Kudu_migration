#!/bin/bash

if [ -z $1 ]; then
    echo usage "./$0 <kudu_table_name>"
    exit 1
fi

WORKDIR=`readlink -f $0 | xargs  dirname`
source ${WORKDIR}/../profile_kudu

KUDU_TABLE_NAME=$1
# KUDU_TABLE_DST_NAME=`echo $KUDU_TABLE_NAME | tr '[:upper:]' '[:lower:]'`

COPY_THREADS=`kudu table list $KUDU_MASTER_SERVER_SRC -tables $KUDU_TABLE_NAME -list_tablets | grep "^  T" | wc -l`

if [[ COPY_THREADS -ge 60 ]]; then
    COPY_THREADS=60
elif [[ COPY_THREADS == 1 ]]; then
    COPY_THREADS=2
fi


START_SEC=`date +%s`
kudu table copy \
$KUDU_MASTER_SERVER_SRC \
$KUDU_TABLE_NAME \
$KUDU_MASTER_SERVER_DST \
-nocreate_table -dst_table=$KUDU_TABLE_NAME \
-num_threads=$COPY_THREADS \
-timeout_ms=300000

EXIT_STATUS=$?
END_SEC=`date +%s`
ELAPSED_TIME=`expr $END_SEC - $START_SEC`

echo "=================================================================="
echo "kudu table copy result : $EXIT_STATUS"
echo "kudu table name : $KUDU_TABLER_NAME"
echo "Elapsed time : $ELAPSED_TIME "
echo "=================================================================="


$DST_IMPALA_SHELL -q "insert into DEFAULT.KUDU_COPY_RESULT
(TABLE_NAME, START_TS, END_TS, RUN_HOST_NAME, NUM_THREADS, ELAPSED_TIME, EXIT_STATUS) 
values ('$KUDU_TABLE_NAME',from_unixtime($START_SEC),from_unixtime($END_SEC),'$HOSTNAME','$COPY_THREADS',$ELAPSED_TIME, $EXIT_STATUS)"


