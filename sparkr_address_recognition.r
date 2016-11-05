##########################
### AddressRecognition ###
##########################
SparkR:::includePackage(sqlContext, 'data.table')

######## test1：MODEL #####################################
trip22<-sql(hiveContext,"select * from ubi_dm_cluster_point limit 5")
library('magrittr')
trip = trip %>% withColumn("WEEKDAY", lit("0")) %>% withColumn("start_adj", lit("0")) %>% withColumn("end_adj", lit("0")) %>% withColumn("Start_Floor", lit("0"))  %>% withColumn("End_Floor", lit("0")) %>% withColumn("Is_First_St", lit("0")) %>% withColumn("Is_Last_St", lit("0")) %>% withColumn("Is_First_long_trip", lit("0"))
trip_rdd<-SparkR:::toRDD(trip)
######## zip&groupBy with keys #####################################
list_rd<-SparkR:::map(trip_rdd, function(x) {
  user<-matrix(unlist(x),floor(length(unlist(x))/33),ncol=33,byrow=T)
  user<-user[1,1]
})
######## test #####################################
list_rd<-SparkR:::map(trip_rdd, function(x) {
  user<-matrix(unlist(x),floor(length(unlist(x))/33),ncol=33,byrow=T)
  user<-1
})

stat_rdd<-SparkR:::map(trip_rdd, function(x) {
  stat_trip<-matrix(unlist(x),floor(length(unlist(x))/33),ncol=33,byrow=T)
  stat_trip
})
rdd<-SparkR:::zipRDD(list_rd,stat_rdd)
parts <- SparkR:::groupByKey(rdd,200L)
SparkR:::cache(parts)
######## main function #####################################
end_rdd<-SparkR:::mapValues(parts, function(x) {
  user_trip<-matrix(unlist(x),floor(length(unlist(x))/33),ncol=33,byrow=T)

  Users<-data.frame(matrix(0,ncol=31,nrow=1))
  names(Users)<-c('ID','tid','vid','home1','home1_lat','home1_lon','home2','home2_lat','home2_lon','company1','company1_lat','company1_lon','company2','company2_lat','company2_lon',
                  'home1_Freq','home1_Aarive_time','home1_Leave_time','home1_time_active_perday',
                  'home2_Freq','home2_Aarive_time','home2_Leave_time','home2_time_active_perday',
                  'company1_Freq','company1_Aarive_time_workday','company1_time_perWorkday','company1_times_perWorkday',
                  'company2_Freq','company2_Aarive_time_workday','company2_time_perWorkday','company2_times_perWorkday')
  testdata<-as.data.frame(user_trip)
  testdata$V5 <- as.vector(unlist(testdata$V5))
  mode(testdata$V5) <- "numeric"
  testdata$V6 <- as.vector(unlist(testdata$V6))
  mode(testdata$V6) <- "numeric"
  testdata$V1 <- as.vector(unlist(testdata$V1))
  mode(testdata$V1) <- "numeric"
  testdata$V21 <- as.vector(unlist(testdata$V21))
  mode(testdata$V21) <- "numeric"
  testdata$V22 <- as.vector(unlist(testdata$V22))
  mode(testdata$V22) <- "numeric"
  testdata$V23 <- as.vector(unlist(testdata$V23))
  mode(testdata$V23) <- "numeric"
  testdata$V24 <- as.vector(unlist(testdata$V24))
  mode(testdata$V24) <- "numeric"
  testdata$V29 <- as.vector(unlist(testdata$V29))
  mode(testdata$V29) <- "numeric"
  testdata$V30 <- as.vector(unlist(testdata$V30))
  mode(testdata$V30) <- "numeric"
  testdata$V31 <- as.vector(unlist(testdata$V31))
  mode(testdata$V31) <- "numeric"
  testdata$V32 <- as.vector(unlist(testdata$V32))
  mode(testdata$V32) <- "numeric"
  testdata$V33 <- as.vector(unlist(testdata$V33))
  mode(testdata$V33) <- "numeric"
  testdata$V26<-wday(as.POSIXlt(testdata$V5,origin='1970-01-01 00:00:00',format='%Y-%m-%d %H:%M:%S','UTC'))

if (dim(testdata)[1]<10) {
    Users$ID<-testdata$V1[1]
    Users$tid<-testdata$V2[1]
    Users$vid<-testdata$V3[1]
    Users$home1<-0
    Users$home1_lat<-0
    Users$home1_lon<-0
    Users$home2<-0
    Users$home2_lat<-0
    Users$home2_lon<-0
    Users$company1<-0
    Users$company1_lat<-0
    Users$company1_lon<-0
    Users$company2<-0
    Users$company2_lat<-0
    Users$company2_lon<-0
  }else{
Users$ID<-testdata$V1[1]
Users$tid<-testdata$V2[1]
Users$vid<-testdata$V3[1]
testdata$V5<-(testdata$V5+8*3600)/60/60/24+70*365+19
testdata$V6<-(testdata$V6+8*3600)/60/60/24+70*365+19
testdata$V27<-testdata$V5-4/24
testdata$V28<-testdata$V6-4/24
testdata$V29<-floor((testdata$V5-floor(testdata$V5))*24)
testdata$V30<-floor((testdata$V6-floor(testdata$V6))*24)
testdata$V31[1]<-1
testdata$V32[length(testdata$V1)]<-1
  }
  testdata})

    for (l in 2:(length(testdata$V1)[1]))
    {
      if (floor(testdata$V27[l]) - floor(testdata$V27[l-1]) == 1)
      {
        testdata$V31[l]<-1
        testdata$V32[l-1]<-1
      }else{
        testdata$V31[l]<-0
        testdata$V32[l-1]<-0
      }
      if (testdata$V22[l]>2/24)
      {
        if (testdata$V31[l]==1){
          testdata$V33[l]<-1
        }else{
          for (p in 1:10000){
            if (testdata$V32[l-p]!=1){
              if (testdata$V33[l-p]!=1){
                if (testdata$V31[l-p]!=1){
                }else{
                  testdata$V33[l] <- 1
                  break
                }}else{
                  testdata$V33[l] <- 0
                  break
                }}else{
                  testdata$V33[l] <- 0
                  break
                }}}
      }else{}}

    Point_List<-as.data.frame(base::table(testdata$V23))
    Point_List_adj<-subset(Point_List,subset=(Point_List$Freq!=1&Point_List$Freq!=2))
    if(dim(Point_List_adj)[1]==0)
    {
      Point_List_adj<-0
    }
    else{
    Point_List_adj$Aarive_time<-0
    }
    
    First_St <- subset(testdata, subset = (testdata$V31 == 1))
    First_St_sort <-as.data.frame(sort(base::table(First_St$V23), decreasing = T))
    First_St_sort <- First_St_sort$Var1[1:3]
    
    Last_En <- subset(testdata, subset = (testdata$V32 == 1))
    Last_En_sort <- as.data.frame(sort(base::table(Last_En$V24), decreasing = T))
    Last_En_sort <- Last_En_sort$Var1[1:3]
    
    first_longtrip_En <- subset(testdata, subset = (testdata$V33 == 1))
    first_longtrip_sort <- as.data.frame(sort(base::table(first_longtrip_En$V24), decreasing = T))
    first_longtrip_sort <- first_longtrip_sort$Var1[1:3]
    for (m in 1:length(Point_List_adj$Var1))
    {
      a <- as.numeric(Point_List_adj$Var1[m])
      Pointdata_En <- subset(testdata, subset = (testdata$V24 == a))
      Pointdata_St <- subset(testdata, subset = (testdata$V23 == a))
      if (length(base::table(floor(Pointdata_En$V28))) == 0)
      {
        Point_List_adj$time_active_perday[m] <- 0
      }else{
        Point_List_adj$time_active_perday[m] <-sum(Pointdata_En$V22) / length(base::table(floor(Pointdata_En$V28)))
      }
      b <- which.max(base::table(Pointdata_En$V30))
      g <- as.data.frame(base::table(Pointdata_En$V30))
      e <- g$Freq[b]
      d <- subset(g, subset = (g$Freq == e))
      if (length(as.numeric(as.vector(unlist(g[which.max(base::table(last(d)[1])), "Var1"])))) ==0)
      {
        Point_List_adj$Aarive_time[m] <- 0
      } else{
        h <- last(d$Var1)
        v <- as.vector(unlist(h))
        mode(v) <- "numeric"
        Point_List_adj$Aarive_time[m] <- v
      }
      bb <- which.max(base::table(Pointdata_St$V29))
      cc <- as.data.frame(base::table(Pointdata_St$V29))
      ee <- cc$Freq[bb]
      dd <- subset(cc, subset = (cc$Freq == ee))
      if (length(as.numeric(as.vector(unlist(cc[which.max(base::table(last(dd)[1])), "Var1"])))) ==0)
      {
        Point_List_adj$Leave_time[m] <- 0
      } else
      {
        hh <- last(dd$Var1)
        vv <- as.vector(unlist(hh))
        mode(vv) <- "numeric"
        Point_List_adj$Leave_time[m] <-  vv
      }
      Pointdata_En_Workday <-subset(Pointdata_En,subset = (Pointdata_En$V26 != 6 & Pointdata_En$V26 != 7))
      bbb <- which.max(base::table(Pointdata_En_Workday$V30))
      ccc <- as.data.frame(base::table(Pointdata_En_Workday$V30))
      eee <- ccc$Freq[bbb]
      ddd <- subset(ccc, subset = (ccc$Freq == eee))
      if (length(as.numeric(as.vector(unlist(ccc[which.max(base::table(last(ddd)[1])), "Var1"]))))==0)
      {
        Point_List_adj$Aarive_time_workday[m] <- 0
      } else
      {
        hhh <- last(ddd$Var1)
        vvv <- as.vector(unlist(hhh))
        mode(vvv) <- "numeric"
        Point_List_adj$Aarive_time_workday[m] <- vvv
      }
      if (sum(Pointdata_En_Workday$V1) == 0)
      {
        Point_List_adj$times_perWorkday[m] <- 0
      } else
      {
        Point_List_adj$times_perWorkday[m]<-length(Pointdata_En_Workday$V1)/(last(floor(Pointdata_En_Workday$V28))-first(floor(Pointdata_En_Workday$V5_adj))+1)
      }
      if (sum(Pointdata_En_Workday$V22) == 0)
      {
        Point_List_adj$time_perWorkday[m] <- 0
      } else
      {
        Point_List_adj$time_perWorkday[m] <-sum(Pointdata_En_Workday$V22)/length(base::table(floor(Pointdata_En_Workday$V28)))
      }
Point_List_adj$check1[m]<-if(base::match(Point_List_adj$Aarive_time[m],c(c(0:2),c(17:24)),nomatch=0)>0){1}else{0}
Point_List_adj$check2[m]<-if(base::match(Point_List_adj$Leave_time[m],c(4:22),nomatch=0)>0){1}else{0}
Point_List_adj$check3[m]<-if(Point_List_adj$time_active_perday[m]>=6/24){1}else{0}
Point_List_adj$check4[m]<-if((base::match(Point_List_adj$Var1[m],First_St_sort,nomatch=0)>0)&(base::match(Point_List_adj$Var1[m],Last_En_sort,nomatch=0)>0)){1}else{0}
Point_List_adj$check5[m]<-if(base::match(Point_List_adj$Aarive_time_workday[m],c(7:15),nomatch=0)>0){1}else{0}
Point_List_adj$check6[m]<-if(Point_List_adj$time_perWorkday[m]>2/24&Point_List_adj$time_perWorkday[m]<18/24){1}else{0}
Point_List_adj$check7[m]<-if(Point_List_adj$times_perWorkday[m]>4/22.75){1}else{0}
Point_List_adj$check8[m]<-if(base::match(Point_List_adj$Var1[m],first_longtrip_sort,nomatch=0)>0){1}else{0}
    }
  }
  Point_List_adj
}) 

    if (Point_List_adj$check1[m] + Point_List_adj$check2[m] + Point_List_adj$check3[m] +Point_List_adj$check4[m] == 4)
    {
      Point_List_adj$check9[m] <- 1
    } else
    {
      Point_List_adj$check9[m] <- 0
    }
    
    if (Point_List_adj$check5[m] + Point_List_adj$check6[m] + Point_List_adj$check7[m] + Point_List_adj$check8[m] == 4)
    {
      Point_List_adj$check10[m] <- 1
    } else
    {
      Point_List_adj$check10[m] <- 0
    }

  Point_List_adj_home <- subset(Point_List_adj, subset = (Point_List_adj$check9 == 1))
  if (length(Point_List_adj_home$Var1) == 0)
  {
    Users$home1 <- 0
  }else{
    if (length(Point_List_adj_home$Var1) == 1)
    {
      Users$home1 <-Point_List_adj_home$Var1[1]
      Users$home1_Freq<-Point_List_adj_home$Freq[1]
      Users$home1_Aarive_time <-Point_List_adj_home$Aarive_time[1]
      Users$home1_Leave_time<-Point_List_adj_home$Leave_time[1]
      Users$home1_time_active_perday <-Point_List_adj_home$time_active_perday[1]
    }else{
      Users$home1 <-Point_List_adj_home$Var1[1]
      Users$home1_Freq<-Point_List_adj_home$Freq[1]
      Users$home1_Aarive_time <-Point_List_adj_home$Aarive_time[1]
      Users$home1_Leave_time<-Point_List_adj_home$Leave_time[1]
      Users$home1_time_active_perday <-Point_List_adj_home$time_active_perday[1]
      Users$home2 <-Point_List_adj_home$Var1[2]
      Users$home2_Freq<-Point_List_adj_home$Freq[2]
      Users$home2_Aarive_time <-Point_List_adj_home$Aarive_time[2]
      Users$home2_Leave_time<-Point_List_adj_home$Leave_time[2]
      Users$home2_time_active_perday <-Point_List_adj_home$time_active_perday[2]
    }
  }
  
  
  Point_List_adj_company <- subset(Point_List_adj,subset = (Point_List_adj$check10==1))
  if (length(Point_List_adj_company$Var1) == 0)
  {
    Users$company1 <- 0
  }else{
    if (length(Point_List_adj_company$Var1) == 1)
    {
      Users$company1 <-Point_List_adj_company$Var1[1]
      Users$company1_Freq<-Point_List_adj_company$Freq[1]
      Users$company1_Aarive_time_workday <-Point_List_adj_company$Aarive_time_workday[1]
      Users$company1_time_perWorkday<-Point_List_adj_company$time_perWorkday[1]
      Users$company1_times_perWorkday <-Point_List_adj_company$times_perWorkday[1]
    }else{
      Users$company1 <-Point_List_adj_company$Var1[1]
      Users$company1_Freq<-Point_List_adj_company$Freq[1]
      Users$company1_Aarive_time_workday <-Point_List_adj_company$Aarive_time[1]
      Users$company1_time_perWorkday<-Point_List_adj_company$Leave_time[1]
      Users$company1_times_perWorkday <-Point_List_adj_company$time_active_perday[1]
      Users$company2 <-Point_List_adj_company$Var1[2]
      Users$company2_Freq<-Point_List_adj_company$Freq[2]
      Users$company2_Aarive_time_workday <-Point_List_adj_company$Aarive_time[2]
      Users$company2_time_perWorkday<-Point_List_adj_company$Leave_time[2]
      Users$company2_times_perWorkday <-Point_List_adj_company$time_active_perday[2]
    }
  }
}
  Users
})


end_rdd_value<-SparkR:::values(end_rdd)
end_end_rdd<-SparkR:::toDF(end_rdd_value,list('deciveid','tid','vid','start','actual_start','s_end','dura','period','lat_st_ori','lon_st_ori','lat_en_ori','lon_en_ori','m_ori','lat_st_def','lon_st_def','lat_en_def','lon_en_def','m_def','speed_mean','gps_speed_sd','gps_acc_sd','stat_date','dura2','sort_st','sort_en'))
registerTempTable(end_end_rdd,"cluster_point")
sql(sqlContext,"insert overwrite table ubi_dm_cluster_point partition(stat_date=) select * from cluster_point")
CREATE external base::table ubi_dm_cluster_point (deviceid String,tid String,vid String,start INT,actual_start INT,s_end INT,dura DOUBLE,period INT,lat_st_ori DOUBLE,lon_st_ori DOUBLE,lat_en_ori DOUBLE,lon_en_ori DOUBLE,m_ori DOUBLE,lat_st_def DOUBLE,lon_st_def DOUBLE,lat_en_def DOUBLE,lon_en_def DOUBLE,m_def DOUBLE,speed_mean DOUBLE,gps_speed_sd DOUBLE,gps_acc_sd DOUBLE,dura2 INT,sort_st String,sort_en String) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dm/ubi_dm_cluster_point';
