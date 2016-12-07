#!/bin/sh
if [ $# -lt 1 ];
    then
    #stat_date=''
    echo "please input month paras eg:201607"
	exit 1
else
    stat_date=$1
fi

for arg in "$@"
do
    echo last month args: $arg
done
stat_date=$1
/opt/cloudera/parcels/SPARK/bin/spark-submit --driver-memory 20g --master spark://hadoop-namenode1:7077 /opt/data-platform/ml/r/ubi_dw_cluster_point.R $stat_date
