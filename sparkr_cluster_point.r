######## WARNING: run in hadoop & hive &impala #####################################
hadoop:
hadoop fs -mkdir /user/kettle/obdbi/original/trip_stat/stat_date=20160701;
hadoop fs -put /home/houzhiwei/stat_date=20160701/trip-stat /user/kettle/obdbi/original/trip_stat/stat_date=20160701;
hive:
CREATE external TABLE if not exists trip_stat_XXXXXX (deciveid String,tid String,vid String,start INT,actual_start INT,s_end INT,dura DOUBLE,period INT,lat_st_ori DOUBLE,lon_st_ori DOUBLE,lat_en_ori DOUBLE,lon_en_ori DOUBLE,m_ori DOUBLE,lat_st_def DOUBLE,lon_st_def DOUBLE,lat_en_def DOUBLE,lon_en_def DOUBLE,m_def DOUBLE,speed_mean DOUBLE,gps_speed_sd DOUBLE,gps_acc_sd DOUBLE) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/obdbi/original/trip_stat';
impala:
impala-shell -r
ALTER TABLE trip_stat_201607 add PArtition (stat_date="20160701");

######## RUN IN SPARKR: import data #####################################
library(SparkR)
sc <- sparkR.init(appName="ubi_dw_cluster_point");sqlContext <- sparkRSQL.init(sc);hiveContext <- sparkRHive.init(sc)
connectBackend.orig <- getFromNamespace('connectBackend', pos='package:SparkR')
connectBackend.patched <- function(hostname, port, timeout = 3600*48) {
  connectBackend.orig(hostname, port, timeout)
}
assignInNamespace("connectBackend", value=connectBackend.patched, pos='package:SparkR')
args <- commandArgs(trailing = TRUE)

if (length(args) != 1) {
  print("Usage: ubi_dw_cluster_point.R <date_period>")
  q("no")
}
library(magrittr)
SparkR:::includePackage(sqlContext, 'SoDA')
trip<-sql(hiveContext,"SELECT * , LEAD(actual_start, 1, 0) OVER (PARTITION BY deviceid ORDER BY actual_start) AS start2 FROM trip_stat_XXXXXX")
trip<-withColumn(trip, "dura2", trip$start2 - trip$s_end)
trip$start2<-NULL
trip = trip %>% withColumn("sort_st", lit("0")) %>% withColumn("sort_en", lit("0"))
trip_rdd<-SparkR:::toRDD(trip)

######## zip&groupBy with keys #####################################
list_rd<-SparkR:::map(trip_rdd, function(x) {
user<-matrix(unlist(x),floor(length(unlist(x))/25),ncol=25,byrow=T)
user<-user[1,1]
})
stat_rdd<-SparkR:::map(trip_rdd, function(x) {
stat_trip<-matrix(unlist(x),floor(length(unlist(x))/25),ncol=25,byrow=T)
stat_trip
})
rdd<-SparkR:::zipRDD(list_rd,stat_rdd)
parts <- SparkR:::groupByKey(rdd,200L)
SparkR:::cache(parts)

######## main function #####################################
end_rdd<-SparkR:::mapValues(parts, function(x) {
    library('SoDA')
    user_trip<-matrix(unlist(x),floor(length(unlist(x))/25),ncol=25,byrow=T)
    if(dim(user_trip)[1]==1){
      user_trip[1,23]<-0
      user_trip[1,24]<-1
      user_trip[1,25]<-2
    }else{
      ######## Set Initial Parameters #####################################
      user_trip<-user_trip[order(user_trip[,5],decreasing=F),]
      user_trip[,24]<-1:dim(user_trip)[1]
      user_trip[,25]<--(1:dim(user_trip)[1])
      ######## Set Initial Parameters #####################################
      for(i in 1:(dim(user_trip)[1]-1)){
        for(j in (i+1):dim(user_trip)[1]){
                  x<-  geoDist(user_trip[i,9],user_trip[i,10],user_trip[j,9],user_trip[j,10])
                  if(x<1000){
                    user_trip[j,24]<-user_trip[i,24]
                  }else{
                  }              
              }
    }
    ######## Set Initial Parameters #####################################
    for(i in 1:(dim(user_trip)[1]-1)){
      for(j in (i+1):dim(user_trip)[1]){
                x<-  geoDist(user_trip[i,11],user_trip[i,12],user_trip[j,9],user_trip[j,10])
                if(x<1000){
                  user_trip[i,25]<-user_trip[j,24]
                }else{
                }              
            }
  }
    ######## Set Initial Parameters #####################################
    for(i in 1:(dim(user_trip)[1]-1)){
      for(j in (i+1):dim(user_trip)[1]){
                x<-  geoDist(user_trip[i,11],user_trip[i,12],user_trip[j,11],user_trip[j,12])
                if(x<1000 & user_trip[j,25]<0 & user_trip[i,25]>0){
                  user_trip[j,25]<-user_trip[i,25]
                }else{if(x<1000 & user_trip[j,25]<0 & user_trip[i,25]<0){
                  user_trip[i,25]<-1000*abs(user_trip[j,25])
                  user_trip[j,25]<-1000*abs(user_trip[j,25])
                }
                }              
            }
  }
  point<-as.data.frame(base::table(c(user_trip[,24],user_trip[,25])))
  point_table<-point[order(point$Freq,decreasing=T),]
  point_table$Freq<-1:dim(point_table)[1]
  user_trip<-merge(user_trip,point_table,by.x="V24",by.y="Var1",all.x=T)
  user_trip<-user_trip[,-1]
  user_trip<-merge(user_trip,point_table,by.x="V25",by.y="Var1",all.x=T)
  user_trip<-user_trip[,-1]
  user_trip<-as.matrix(user_trip)
  }
  a<-user_trip[,22]
  user_trip[,22]<-user_trip[,23]
  user_trip[,23]<-user_trip[,24]
  user_trip[,24]<-user_trip[,25]
  user_trip[,25]<-a
  user_trip
    })
######## change structure #####################################
end_rdd_rdd <- SparkR:::flatMapValues(end_rdd, function(x) {
    stat_trip <-  matrix(unlist(x),floor(length(unlist(x))/25),ncol=25)
    stat_trip <- split(stat_trip, row(stat_trip))
    stat_trip
})
end_rdd_value<-SparkR:::values(end_rdd_rdd)
SparkR:::cache(end_rdd_value)

SparkR:::saveAsTextFile(end_rdd_value, paste0("/user/kettle/ubi/dw/cluster_point/stat_date=",date_period))
sql(hiveContext,"drop TABLE ubi_dw_cluster_point")
sql(hiveContext,"CREATE external TABLE ubi_dw_cluster_point (deviceid String,tid String,vid String,start String,actual_start String,s_end String,dura DOUBLE,period String,lat_st_ori DOUBLE,lon_st_ori DOUBLE,lat_en_ori DOUBLE,lon_en_ori DOUBLE,m_ori DOUBLE,lat_st_def DOUBLE,lon_st_def DOUBLE,lat_en_def DOUBLE,lon_en_def DOUBLE,m_def DOUBLE,speed_mean DOUBLE,gps_speed_sd DOUBLE,gps_acc_sd DOUBLE,dura2 String,sort_st String,sort_en String,dim_month string) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dw/cluster_point/'")
sql(hiveContext,paste0("ALTER TABLE ubi_dw_cluster_point add PArtition (stat_date='",date_period,"')"))
sql(hiveContext,"insert overwrite table ubi_dw_cluster_point select trim(deviceid) as deviceid,trim(tid) as tid,trim(vid) as vid,trim(start) as start,trim(actual_start) as actual_start,trim(s_end) as s_end,dura,trim(period) as period,lat_st_ori,lon_st_ori,lat_en_ori,lon_en_ori,m_ori,lat_st_def,lon_st_def,lat_en_def,lon_en_def,m_def,speed_mean,gps_speed_sd,gps_acc_sd,trim(dura2) as dura2,trim(sort_st) as sort_st,trim(sort_en) as sort_en,trim(stat_date) as stat_date from ubi_dw_cluster_point")
sql(hiveContext,"drop TABLE ubi_dw_cluster_point")
sql(hiveContext,"CREATE external TABLE ubi_dw_cluster_point (deviceid String,tid String,vid String,start String,actual_start String,s_end String,dura DOUBLE,period String,lat_st_ori DOUBLE,lon_st_ori DOUBLE,lat_en_ori DOUBLE,lon_en_ori DOUBLE,m_ori DOUBLE,lat_st_def DOUBLE,lon_st_def DOUBLE,lat_en_def DOUBLE,lon_en_def DOUBLE,m_def DOUBLE,speed_mean DOUBLE,gps_speed_sd DOUBLE,gps_acc_sd DOUBLE,dura2 String,sort_st String,sort_en String,dim_month string) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dw/cluster_point/'")
sql(hiveContext,paste0("ALTER TABLE ubi_dw_cluster_point add PArtition (stat_date='",date_period,"')"))

sparkR.stop();
