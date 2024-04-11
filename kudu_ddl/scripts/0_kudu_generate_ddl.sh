#!/bin/bash

# Set ENV.          ########################################################################################################
WORKDIR=`readlink -f $0 | xargs  dirname`
source ${WORKDIR}/../../profile_kudu


# Get Kudu table list from ASIS.
function fnc_list_table {
    mkdir -p ${KUDU_META}
    kudu table list ${KUDU_MASTER_SERVER_SRC} | grep "^impala::" | awk -F"::" '{print $2}' > ${KUDU_META}/kudu_table_list.list
}


# Generate DDL.
function fnc_gen_ddl {


    rm -rf ${KUDU_DDL}/*.sql
    mkdir -p ${KUDU_DDL}
    
    > ${KUDU_META}/show_create_table.sql
    echo "set write_delimited=true;"          >> ${KUDU_META}/show_create_table.sql
 
    cat ${KUDU_META}/kudu_table_list.list | while read LINE
    do
        echo "set OUTPUT_FILE=${KUDU_DDL}/${LINE}.sql;"
        echo "show create table ${LINE};"
    done >> ${KUDU_META}/show_create_table.sql

    ${SRC_IMPALA_SHELL} -c -f ${KUDU_META}/show_create_table.sql
}

# Remove Unused Line.
function fnc_remove_unused {
    sed -i "s/^\"CREATE/CREATE/g" ${KUDU_DDL}/*.sql
    sed -i "s/CREATE EXTERNAL TABLE/CREATE TABLE/g" ${KUDU_DDL}/*.sql
    sed -i "/^TBLPROPERTIES/d"    ${KUDU_DDL}/*.sql
}


# Modify uppercase table name
function replace_tblname_upper {
    cat ${KUDU_META}/kudu_table_list.list | while read LINE
    do
        TBL_NAME=${LINE}
        cur=`cat ${KUDU_DDL}/${TBL_NAME}.sql | grep CREATE | awk -F" " '{print $3}'`
        if [ ${cur} != ${TBL_NAME} ]; then
            sed -i "/CREATE/s/${cur}/${TBL_NAME}" ${KUDU_DDL}/${TBL_NAME}.sql
        fi
    done
}

# Create Partition type metadata.
function fnc_part_info {
    cat ${KUDU_META}/kudu_table_list.list | while read LINE
    do
        TBL_NAME=${LINE}
        if [ `grep "PARTITION BY" ${KUDU_DDL}/${LINE}.sql | wc -l` -eq 0 ]; then
          echo "${TBL_NAME}|NO"
        elif [ `grep "PARTITION BY RANGE" ${KUDU_DDL}/${LINE}.sql | wc -l` -eq 1 ]; then
           echo "${TBL_NAME}|RANGE"
        elif [ `grep "PARTITION BY HASH" ${KUDU_DDL}/${LINE}.sql | grep "RANGE" | grep "(\.\.\.)" | wc -l` -eq 1 ]; then
            echo "${TBL_NAME}|HASHRANGE"
        elif [ `grep "PARTITION BY HASH" ${KUDU_DDL}/${LINE}.sql | grep -v "RANGE" | grep -v "(\.\.\.)" | wc -l` -eq 1 ]; then
            echo "${TBL_NAME}|HASH"
        else
            echo "${TBL_NAME}|UNKNOWN"
        fi
    done > ${KUDU_META}/kudu_part.list
}

# Get Range spec using "kudu table describe" command.
function fnc_get_range {
    mkdir -p ${KUDU_META}/range
    rm -rf ${KUDU_META}/range/*.txt
    cat ${KUDU_META}/kudu_part.list | grep RANGE | awk -F"|" '{print $1}' | while read LINE
    do
        kudu table describe ${KUDU_MASTER_SERVER_SRC} impala::${LINE} > ${KUDU_META}/range/${LINE}.txt
        sed -i 's/\\000//g'  ${KUDU_META}/range/${LINE}.txt
        STRL=`grep -n "^RANGE" ${KUDU_META}/range/${LINE}.txt | awk -F":" '{print $1}'`
        ENDL=`grep -n "^OWNER" ${KUDU_META}/range/${LINE}.txt | awk -F":" '{print $1}'`
        STRL=`expr $STRL + 1`
        ENDL=`expr $ENDL - 2`
        #echo $STRL $ENDL
        sed -n ${STRL},${ENDL}p ${KUDU_META}/range/${LINE}.txt > ${KUDU_META}/range/${LINE}.out
        mv -f ${KUDU_META}/range/${LINE}.out ${KUDU_META}/range/${LINE}.txt 
    done
}

# Replacing Range spec (...) to "RANGE_PARTITION_DEF"
function fnc_set_range {
    cat ${KUDU_META}/kudu_part.list | grep RANGE | awk -F"|" '{print $1}' | while read LINE
    do
        sed -i "s|\.\.\.|\\nRANGE_PARTITION_DEF\\n|g" ${KUDU_DDL}/${LINE}.sql
        fnc_put_range "${LINE}"
    done
}

# Put Range spec
function fnc_put_range {
        TABLE_NAME=${1}
        POSITION=`grep -n RANGE_PARTITION_DEF ${KUDU_DDL}/${TABLE_NAME}.sql | awk -F":" '{print $1}'`
        HEADNO=`expr ${POSITION} - 1`
        TAILNO=`expr ${POSITION} + 1`
        head -${HEADNO}  ${KUDU_DDL}/${TABLE_NAME}.sql              >  ${KUDU_DDL}/${TABLE_NAME}_range.sql
        cat ${KUDU_META}/range/${TABLE_NAME}.txt                    >> ${KUDU_DDL}/${TABLE_NAME}_range.sql
        tail -n +${TAILNO}  ${KUDU_DDL}/${TABLE_NAME}.sql           >> ${KUDU_DDL}/${TABLE_NAME}_range.sql
        mv -f ${KUDU_DDL}/${TABLE_NAME}_range.sql ${KUDU_DDL}/${TABLE_NAME}.sql
} 

# Add quote for reserved keywords.
function fnc_rsv_word {
    sed -i "/DEFAULT_COMPRESSION/s/^  /  \`/; \
    /DEFAULT_COMPRESSION/s/ /\` /3; \
    /PRIMARY KEY/s/(/(\`/;   \
    /PRIMARY KEY/s/)/\`)/; \
    /PRIMARY KEY/s/, /\`, \`/g; \
    /PARTITION/s/(/(\`/g;   \
    /PARTITION/s/)/\`)/g; \
    /PARTITION/s/, /\`, \`/g; \
    " ${KUDU_DDL}/*.sql
    sed -i 's/`, `HASH/,_HASH/;s/`, `RANGE/, RANGE/;s/`$//' ${KUDU_DDL}/*.sql
}



#### Exec function ####
fnc_list_table
fnc_gen_ddl
fnc_remove_unused
replace_tblname_upper
fnc_part_info
fnc_get_range
fnc_set_range
# fnc_rsv_word
