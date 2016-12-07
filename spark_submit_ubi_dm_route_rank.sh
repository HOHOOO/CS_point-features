#!/bin/sh
if [ $# -lt 1 ];
    then
    #stat_date=''
    echo "please input month paras eg:201607 "
else
    stat_date=$1
fi

/opt/cloudera/parcels/SPARK/bin/spark-submit --driver-memory 20g --master spark://hadoop-namenode1:7077 /opt/data-platform/ml/py/spark_ubi_dm_route_rank.py $stat_date

