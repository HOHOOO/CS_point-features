trip<-sql(hiveContext,"select * from trip_stat")
library(magrittr)
SparkR:::includePackage(sqlContext, 'SoDA')
trip = trip %>% withColumn("dura2", lit("0")) %>% withColumn("sort_st", lit("0")) %>% withColumn("sort_en", lit("0"))
trip_rdd<-SparkR:::toRDD(trip)
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
      user_trip[(dim(user_trip)[1]),23]<-0
      user_trip[,24]<-1:dim(user_trip)[1]
      user_trip[,25]<--(1:dim(user_trip)[1])
      ######## Set Initial Parameters #####################################
      for(i in 1:(dim(user_trip)[1]-1)){
        for(j in (i+1):dim(user_trip)[1]){
          user_trip[i,23]<-(as.numeric(user_trip[j,5])-as.numeric(user_trip[i,6]))/60
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

    user_trip
    })
end_rdd_value<-SparkR:::values(end_rdd)
flat_end_rdd<-SparkR:::flatMap(end_rdd_value,function(x){
  end_trip<-matrix(unlist(x),floor(length(unlist(x))/25),ncol=25,byrow=T)
  end_trip
  })
end_end_rdd<-SparkR:::toDF(flat_end_rdd,list('deciveid','tid','vid','start','actual_start','s_end','dura','period','lat_st_ori','lon_st_ori','lat_en_ori','lon_en_ori','m_ori','lat_st_def','lon_st_def','lat_en_def','lon_en_def','m_def','speed_mean','gps_speed_sd','gps_acc_sd','stat_date','dura2','sort_st','sort_en'))
registerTempTable(end_end_rdd,"cluster_point")
sql(hiveContext,"set hive.exec.dynamic.partition.mode=nostrick")
sql(hiveContext,"set hive.exec.dynamic.partition=true")
sql(hiveContext,"set hive.exec.max.dynamic.partitions.pernode = 2000000000")
sql(hiveContext,"set hive.exec.max.dynamic.partitions = 2000000000")
sql(hiveContext,"set hive.exec.max.created.files = 2000000000")
sql(hiveContext,"insert overwrite table ubi_dm_cluster_point partition (stat_date) select * from cluster_point")
