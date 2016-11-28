######## RUN IN SPARKR: import data #####################################
library(SparkR)
sc <- sparkR.init(appName="ubi_dm_address_recognition");sqlContext <- sparkRSQL.init(sc);hiveContext <- sparkRHive.init(sc)
connectBackend.orig <- getFromNamespace('connectBackend', pos='package:SparkR')
connectBackend.patched <- function(hostname, port, timeout = 3600*48) {
  connectBackend.orig(hostname, port, timeout)
}
assignInNamespace("connectBackend", value=connectBackend.patched, pos='package:SparkR')
args <- commandArgs(trailing = TRUE)

if (length(args) != 1) {
  print("Usage: address_recognition.R <date_period>")
  q("no")
}

# Initialize SparkContext and SQLContext
SparkR:::includePackage(sqlContext, 'data.table')
SparkR:::includePackage(sqlContext, 'gdata')
date_period <- args[[1]]
trip<-sql(hiveContext,"select * from ubi_dw_cluster_point")


library('magrittr')
trip = trip %>% withColumn("WEEKDAY", lit("0")) %>% withColumn("start_adj", lit("0")) %>% withColumn("end_adj", lit("0")) %>% withColumn("Start_Floor", lit("0"))  %>% withColumn("End_Floor", lit("0")) %>% withColumn("Is_First_St", lit("0")) %>% withColumn("Is_Last_St", lit("0")) %>% withColumn("Is_First_long_trip", lit("0"))
trip_rdd<-SparkR:::toRDD(trip)

######## zip&groupBy with keys #####################################
list_rd<-SparkR:::map(trip_rdd, function(x) {
  user<-matrix(unlist(x),floor(base::length(unlist(x))/33),ncol=33,byrow=T)
  user<-user[1,1]
})
stat_rdd<-SparkR:::map(trip_rdd, function(x) {
  stat_trip<-matrix(unlist(x),floor(base::length(unlist(x))/33),ncol=33,byrow=T)
  stat_trip
})
rdd<-SparkR:::zipRDD(list_rd,stat_rdd)
parts <- SparkR:::groupByKey(rdd,200L)
SparkR:::cache(parts)

######## main function not including autonavi api part #####################################
end_rdd<-SparkR:::mapValues(parts, function(x) {
  user_trip<-matrix(unlist(x),floor(base::length(unlist(x))/33),ncol=33,byrow=T)
  users<-data.frame(matrix(0,ncol=31,nrow=1))
  names(users)<-c('ID','tid','vid','home1','home1_lat','home1_lon','home2','home2_lat','home2_lon','company1','company1_lat','company1_lon','company2','company2_lat','company2_lon',
                  'home1_Freq','home1_Aarive_time','home1_Leave_time','home1_time_active_perday',
                  'home2_Freq','home2_Aarive_time','home2_Leave_time','home2_time_active_perday',
                  'company1_Freq','company1_Aarive_time_workday','company1_time_perWorkday','company1_times_perWorkday',
                  'company2_Freq','company2_Aarive_time_workday','company2_time_perWorkday','company2_times_perWorkday')
  testdata<-as.data.frame(user_trip)
  testdata$V2 <- as.vector(unlist(testdata$V2))
  mode(testdata$V2) <- "numeric"
  testdata$V3 <- as.vector(unlist(testdata$V3))
  mode(testdata$V3) <- "numeric"
  testdata$V5 <- as.vector(unlist(testdata$V5))
  mode(testdata$V5) <- "numeric"
  testdata$V6 <- as.vector(unlist(testdata$V6))
  mode(testdata$V6) <- "numeric"
  testdata$V1 <- as.vector(unlist(testdata$V1))
  mode(testdata$V1) <- "numeric"
  testdata<-testdata[order(testdata$V5),]
  testdata$V16 <- as.vector(unlist(testdata$V16))
  mode(testdata$V16) <- "numeric"
  testdata$V17 <- as.vector(unlist(testdata$V17))
  mode(testdata$V17) <- "numeric"
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
  testdata$V26<-wday(as.POSIXlt(testdata$V5,origin='1970-01-01 00:00:00',format='%Y-%m-%d %H:%M:%S','uTC'))
  
    users$ID<-testdata$V1[1]
    users$tid<-testdata$V2[1]
    users$vid<-testdata$V3[1]
    testdata$V5<-(testdata$V5+8*3600)/60/60/24+70*365+19
    testdata$V6<-(testdata$V6+8*3600)/60/60/24+70*365+19
    testdata$V27<-testdata$V5-4/24
    testdata$V28<-testdata$V6-4/24
    testdata$V29<-floor((testdata$V5-floor(testdata$V5))*24)
    testdata$V30<-floor((testdata$V6-floor(testdata$V6))*24)
    testdata$V31[1]<-1
    testdata$V32[base::length(testdata$V1)]<-1
    
    for (g in 1:(base::length(testdata$V1)[1]))
    {
    testdata$V22[g]<-if(testdata$V22[g]<0){0}else{testdata$V22[g]/3600/24}}
    Point_List<-as.data.frame(base::table(testdata$V23))
    Point_List_adj<-subset(Point_List,subset=(Point_List$Freq!=1&Point_List$Freq!=2))

    if (base::length(Point_List_adj$Freq)==0 | base::length(testdata$V1)[1]<5){
      users$ID<-testdata$V1[1]
      users$tid<-testdata$V2[1]
      users$vid<-testdata$V3[1]
    }else{
      for (l in 2:(base::length(testdata$V1)[1]))
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
    Point_List_adj$Aarive_time<-0
    First_St <- subset(testdata, subset = (testdata$V31 == 1))
    if(dim(First_St)[1]==0)
      {
      First_St_sort <- c("0")
    }else{
      First_St_sort1 <-as.data.frame(base::table(First_St$V23))
      names(First_St_sort1)<-c("Var1","Freq")
      First_St_sort1<-First_St_sort1[order(-First_St_sort1$Freq),]
      if(base::length(First_St_sort1$Var1) < 3){
      First_St_sort <- c(unlist(First_St_sort1$Var1))
    }else{
      First_St_sort <- c(unlist(First_St_sort1$Var1[1:3]))
    }}

    Last_En <- subset(testdata, subset = (testdata$V32 == 1))
    Last_En_sort1 <-as.data.frame(base::table(Last_En$V24))
    names(Last_En_sort1)<-c("Var1","Freq")
    Last_En_sort1<-Last_En_sort1[order(-Last_En_sort1$Freq),]
    if(base::length(Last_En_sort1$Var1) < 3){
      Last_En_sort <- c(unlist(Last_En_sort1$Var1))
    }else{
      Last_En_sort <- c(unlist(Last_En_sort1$Var1[1:3]))
    }

    first_longtrip_En <- subset(testdata, subset = (testdata$V33 == 1))
    if(dim(first_longtrip_En)[1]==0)
    {
      first_longtrip_sort <- c("0")
    }else{
    first_longtrip_sort1 <-as.data.frame(base::table(first_longtrip_En$V24))
    names(first_longtrip_sort1)<-c("Var1","Freq")
    first_longtrip_sort1<-first_longtrip_sort1[order(-first_longtrip_sort1$Freq),]
    if(base::length(first_longtrip_sort1$Var1) < 3){
      first_longtrip_sort <- c(unlist(first_longtrip_sort1$Var1))
    }else{
      first_longtrip_sort <- c(unlist(first_longtrip_sort1$Var1[1:3]))
    }}
 
    for (m in 1:base::length(Point_List_adj$Var1))
    {
      a <- as.numeric(Point_List_adj$Var1[m])
      Pointdata_En <- subset(testdata, subset = (testdata$V24 == a))
      Pointdata_St <- subset(testdata, subset = (testdata$V23 == a))
      if (base::length(base::table(floor(Pointdata_En$V28))) == 0)
      {
        Point_List_adj$time_active_perday[m] <- 0
      }else{
        Point_List_adj$time_active_perday[m] <-sum(Pointdata_En$V22)/base::length(base::table(floor(Pointdata_En$V28)))
      }
      
      b <- which.max(base::table(Pointdata_En$V30))
      g <- as.data.frame(base::table(Pointdata_En$V30))
      e <- g$Freq[b]
      d <- subset(g, subset = (g$Freq == e))
      if (base::length(as.numeric(as.vector(unlist(g[which.max(base::table(gdata::last(d)[1])), "Var1"]))))==0)
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
      if (base::length(as.numeric(as.vector(unlist(cc[which.max(base::table(gdata::last(dd)[1])), "Var1"]))))==0)
      {
        Point_List_adj$Leave_time[m] <- 0
      }else{
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
      
      if (base::length(as.numeric(as.vector(unlist(ccc[which.max(base::table(gdata::last(ddd)[1])), "Var1"]))))==0)
      {
        Point_List_adj$Aarive_time_workday[m] <- 0
      }else{
        hhh <- last(ddd$Var1)
        vvv <- as.vector(unlist(hhh))
        mode(vvv) <- "numeric"
        Point_List_adj$Aarive_time_workday[m] <- vvv
      }

      if (base::length(Pointdata_En_Workday$V1) == 0)
      {
        Point_List_adj$times_perWorkday[m]<-0
      }else{
        Point_List_adj$times_perWorkday[m]<-base::length(Pointdata_En_Workday$V1)/(gdata::last(Pointdata_En_Workday$V28)-gdata::first(Pointdata_En_Workday$V27)+1)
      }
      
      if (base::length(base::unique(Pointdata_En_Workday$V28))==0)
      {
        Point_List_adj$time_perWorkday[m]<-0
      }else
      {
        Point_List_adj$time_perWorkday[m] <-base::sum(Pointdata_En_Workday$V22)/(base::length(base::unique(Pointdata_En_Workday$V28)))
      }
      
      Point_List_adj$check1[m]<-if(base::match(Point_List_adj$Aarive_time[m],c(c(0:2),c(17:24)),nomatch=0)>0){1}else{0}
      Point_List_adj$check2[m]<-if(base::match(Point_List_adj$Leave_time[m],c(4:22),nomatch=0)>0){1}else{0}
      Point_List_adj$check3[m]<-if(Point_List_adj$time_active_perday[m]>=6/24){1}else{0}
      Point_List_adj$check4[m]<-if((base::match(Point_List_adj$Var1[m],First_St_sort,nomatch=0)>0) & (base::match(Point_List_adj$Var1[m],Last_En_sort,nomatch=0)>0)){1}else{0}
      Point_List_adj$check5[m]<-if(base::match(Point_List_adj$Aarive_time_workday[m],c(7:15),nomatch=0)>0){1}else{0}
      Point_List_adj$check6[m]<-if((Point_List_adj$time_perWorkday[m]>2/24) & (Point_List_adj$time_perWorkday[m]<18/24)){1}else{0}
      Point_List_adj$check7[m]<-if(Point_List_adj$times_perWorkday[m]>4/22.75){1}else{0}
      Point_List_adj$check8[m]<-if(base::match(Point_List_adj$Var1[m],first_longtrip_sort,nomatch=0)>0){1}else{0}

      if((Point_List_adj$check1[m]+Point_List_adj$check2[m]+Point_List_adj$check3[m]+Point_List_adj$check4[m])==4)
      {
        Point_List_adj$check9[m] <- 1
      }else
      {
        Point_List_adj$check9[m] <- 0
      }

      if((Point_List_adj$check5[m]+Point_List_adj$check6[m]+Point_List_adj$check7[m]+Point_List_adj$check8[m])==4)
      {
        Point_List_adj$check10[m] <- 1
      }else
      {
        Point_List_adj$check10[m] <- 0
      }
      
    }

Point_List_adj_home<-subset(Point_List_adj, subset=(Point_List_adj$check9==1))
if (base::length(Point_List_adj_home$Var1) == 0)
{
}else{
  if (base::length(Point_List_adj_home$Var1) == 1)
  { users$home1 <-Point_List_adj_home$Var1[1]
    users$home1_lat<-base::mean(subset(testdata$V16,subset = (testdata$V24==unlist(users$home1))))
    users$home1_lon<-base::mean(subset(testdata$V17,subset = (testdata$V24==unlist(users$home1))))
    users$home1_Freq<-Point_List_adj_home$Freq[1]
    users$home1_Aarive_time <-Point_List_adj_home$Aarive_time[1]
    users$home1_Leave_time<-Point_List_adj_home$Leave_time[1]
    users$home1_time_active_perday <-Point_List_adj_home$time_active_perday[1]
  }else{
    users$home1 <-Point_List_adj_home$Var1[1]
    users$home1_Freq<-Point_List_adj_home$Freq[1]
    users$home1_Aarive_time <-Point_List_adj_home$Aarive_time[1]
    users$home1_Leave_time<-Point_List_adj_home$Leave_time[1]
    users$home1_time_active_perday <-Point_List_adj_home$time_active_perday[1]
    users$home2 <-Point_List_adj_home$Var1[2]
    users$home2_Freq<-Point_List_adj_home$Freq[2]
    users$home2_Aarive_time <-Point_List_adj_home$Aarive_time[2]
    users$home2_Leave_time<-Point_List_adj_home$Leave_time[2]
    users$home2_time_active_perday <-Point_List_adj_home$time_active_perday[2]
    users$home1_lat<-base::mean(subset(testdata$V16,subset = (testdata$V24==users$home1)))
    users$home1_lon<-base::mean(subset(testdata$V17,subset = (testdata$V24==users$home1)))
    users$home2_lat<-base::mean(subset(testdata$V16,subset = (testdata$V24==users$home2)))
    users$home2_lon<-base::mean(subset(testdata$V17,subset = (testdata$V24==users$home2)))
    }}

Point_List_adj_company<-subset(Point_List_adj,subset=(Point_List_adj$check10==1))
if (base::length(Point_List_adj_company$Var1) == 0)
{
}else{
  if (base::length(Point_List_adj_company$Var1) == 1)
  {
    users$company1 <-Point_List_adj_company$Var1[1]
    users$company1_Freq<-Point_List_adj_company$Freq[1]
    users$company1_Aarive_time_workday <-Point_List_adj_company$Aarive_time_workday[1]
    users$company1_time_perWorkday<-Point_List_adj_company$time_perWorkday[1]
    users$company1_times_perWorkday <-Point_List_adj_company$times_perWorkday[1]
    users$company1_lat<-base::mean(subset(testdata$V16,subset = (testdata$V24==Point_List_adj_company$Var1[1])))
    users$company1_lon<-base::mean(subset(testdata$V17,subset = (testdata$V24==Point_List_adj_company$Var1[1])))
  }else{
    users$company1 <-Point_List_adj_company$Var1[1]
    users$company1_Freq<-Point_List_adj_company$Freq[1]
    users$company1_Aarive_time_workday <-Point_List_adj_company$Aarive_time[1]
    users$company1_time_perWorkday<-Point_List_adj_company$Leave_time[1]
    users$company1_times_perWorkday <-Point_List_adj_company$time_active_perday[1]
    users$company2 <-Point_List_adj_company$Var1[2]
    users$company2_Freq<-Point_List_adj_company$Freq[2]
    users$company2_Aarive_time_workday <-Point_List_adj_company$Aarive_time[2]
    users$company2_time_perWorkday<-Point_List_adj_company$Leave_time[2]
    users$company2_times_perWorkday <-Point_List_adj_company$time_active_perday[2]
    users$company1_lat<-base::mean(subset(testdata$V16,subset = (testdata$V24==Point_List_adj_company$Var1[1])))
    users$company1_lon<-base::mean(subset(testdata$V17,subset = (testdata$V24==Point_List_adj_company$Var1[1])))
    users$company2_lat<-base::mean(subset(testdata$V16,subset = (testdata$V24==Point_List_adj_company$Var1[2])))
    users$company2_lon<-base::mean(subset(testdata$V17,subset = (testdata$V24==Point_List_adj_company$Var1[2])))
 }}}
    users
})
######## change structure & output data #####################################
end_rdd_value<-SparkR:::values(end_rdd)
SparkR:::saveAsTextFile(end_rdd_value, paste0("/user/kettle/ubi/dw/ubi_dw_address_recognition/stat_date='",date_period,"01'"))
sql(hiveContext,"drop TABLE ubi_dw_address_recognition")
sql(hiveContext,"CREATE external TABLE ubi_dw_address_recognition (deviceid String,tid String,vid String,home1 String,home1_lat double,home1_lon double,home2 String,home2_lat double,home2_lon double,company1 String,company1_lat double,company1_lon double,company2 String,company2_lat double,company2_lon double,home1_Freq double,home1_Aarive_time string,home1_Leave_time string,home1_time_active_perday double,home2_Freq double,home2_Aarive_time string,home2_Leave_time string,home2_time_active_perday double,company1_Freq double,company1_Aarive_time_workday string,company1_time_perWorkday double,company1_times_perWorkday double,company2_Freq double,company2_Aarive_time_workday string,company2_time_perWorkday double,company2_times_perWorkday double) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dw/ubi_dw_address_recognition/'")
sql(hiveContext,paste0("ALTER TABLE ubi_dw_address_recognition add PArtition (stat_date='",date_period,"01')"))
sparkR.stop()
