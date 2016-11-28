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
date_period <- args[[1]]

SparkR:::includePackage(sqlContext, 'RCurl')
trip<-sql(hiveContext,"select * from ubi_dm_address_recognition")
library('magrittr')
trip$stat_data<-NULL
trip = trip %>% withColumn("home1_adress", lit("0")) %>% withColumn("home2_adress", lit("0")) %>% withColumn("company1_adress", lit("0")) %>% withColumn("company1_adress", lit("0"))  %>% withColumn("dim_month", lit(date_period))
trip_rdd<-SparkR:::toRDD(trip)
list_rd<-SparkR:::map(trip_rdd, function(x) {
  library('RCurl')
  user<-matrix(unlist(x),floor(base::length(unlist(x))/36),ncol=36,byrow=T)
  regeo <- function(lon,lat)
  {
    output='xml'
    radius= 10
    extensions="all"
    batch='false'
    homeorcorp= 0
    map_ak = '机密'
    lon <- base::as.numeric(lon)
    lat <- base::as.numeric(lat)
    if(lon==0)
    {
      geo<-"0"
      }else{
        url_head = base::paste0("http://restapi.amap.com/v3/geocode/regeo?key=", map_ak, "&location=")
        url_tail = base::paste0("&poitype=&radius=",radius,"&extensions=",extensions,"&batch=", batch,"&homeorcorp=",homeorcorp, "&output=",output, "&roadlevel=1")
        url = base::paste0(url_head, lon, ",", lat, url_tail)
        geo1 <- base::gsub(".*<province>(.*?)</province>.*", '\\1', RCurl::getURL(url))
        geo2 <- base::gsub(".*<city>(.*?)</city>.*", '\\1', RCurl::getURL(url))
        geo3 <- base::gsub(".*<district>(.*?)</district>.*", '\\1', RCurl::getURL(url))
        geo4 <- base::gsub(".*<township>(.*?)</township>.*", '\\1', RCurl::getURL(url))
        geo<-paste0(geo1,geo2,geo3,geo4)
      }
    return(geo)
  }
  for (i in 1:(base::dim(user)[1]))
  {     
   user[i,32]<-regeo(user[i,6],user[i,5])
   user[i,33]<-regeo(user[i,9],user[i,8])
   user[i,34]<-regeo(user[i,12],user[i,11])
   user[i,35]<-regeo(user[i,15],user[i,14])
  }
})
SparkR:::saveAsTextFile(list_rd, paste0("/user/kettle/ubi/dm/ubi_dm_address_recognition/stat_date='",date_period,"01'"))
sql(hiveContext,"drop TABLE ubi_dm_address_recognition")
sql(hiveContext,"CREATE external TABLE ubi_dm_address_recognition (deviceid String,tid String,vid String,home1 String,home1_lat double,home1_lon double,home2 String,home2_lat double,home2_lon double,company1 String,company1_lat double,company1_lon double,company2 String,company2_lat double,company2_lon double,home1_Freq double,home1_Aarive_time string,home1_Leave_time string,home1_time_active_perday double,home2_Freq double,home2_Aarive_time string,home2_Leave_time string,home2_time_active_perday double,company1_Freq double,company1_Aarive_time_workday string,company1_time_perWorkday double,company1_times_perWorkday double,company2_Freq double,company2_Aarive_time_workday string,company2_time_perWorkday double,company2_times_perWorkday double,home1_adress string,home2_adress string,company1_adress string,company2_adress string,dim_month string) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dm/ubi_dm_address_recognition/'")
sql(hiveContext,paste0("ALTER TABLE ubi_dm_address_recognition add PArtition (stat_date='",date_period,"01')"))
sql(hiveContext,"insert overwrite table ubi_dm_address_recognition select trim(deviceid) as deviceid, trim(tid) as tid, trim(vid) as vid, trim(home1) as home1, trim(home1_lat) as home1_lat, trim(home1_lon) as home1_lon, trim(home2) as home2, trim(home2_lat) as home2_lat, trim(home2_lon) as home2_lon, trim(company1) as company1, trim(company1_lat) as company1_lat, trim(company1_lon) as company1_lon, trim(company2) as company2, trim(company2_lat) as company2_lat, trim(company2_lon) as company2_lon, trim(home1_Freq) as home1_Freq, trim(home1_Aarive_time) as home1_Aarive_time, trim(home1_Leave_time) as home1_Leave_time, trim(home1_time_active_perday) as home1_time_active_perday, trim(home2_Freq) as home2_Freq, trim(home2_Aarive_time) as home2_Aarive_time, trim(home2_Leave_time) as home2_Leave_time, trim(home2_time_active_perday) as home2_time_active_perday, trim(company1_Freq) as company1_Freq, trim(company1_Aarive_time_workday) as company1_Aarive_time_workday, trim(company1_time_perWorkday) as company1_time_perWorkday, trim(company1_times_perWorkday) as company1_times_perWorkday, trim(company2_Freq) as company2_Freq, trim(company2_Aarive_time_workday) as company2_Aarive_time_workday, trim(company2_time_perWorkday) as company2_time_perWorkday, trim(company2_times_perWorkday) as company2_times_perWorkday,trim(home1_adress) as home1_adress,trim(home2_adress) as home2_adress,trim(company1_adress) as company1_adress,trim(company2_adress) as company2_adress,trim(dim_month) as dim_month from ubi_dm_address_recognition")
sql(hiveContext,"drop TABLE ubi_dm_address_recognition")
sql(hiveContext,"CREATE external TABLE ubi_dm_address_recognition (deviceid String,tid int,vid int,home1 String,home1_lat double,home1_lon double,home2 String,home2_lat double,home2_lon double,company1 String,company1_lat double,company1_lon double,company2 String,company2_lat double,company2_lon double,home1_Freq double,home1_Aarive_time string,home1_Leave_time string,home1_time_active_perday double,home2_Freq double,home2_Aarive_time string,home2_Leave_time string,home2_time_active_perday double,company1_Freq double,company1_Aarive_time_workday string,company1_time_perWorkday double,company1_times_perWorkday double,company2_Freq double,company2_Aarive_time_workday string,company2_time_perWorkday double,company2_times_perWorkday double,home1_adress string,home2_adress string,company1_adress string,company2_adress string,dim_month string) partitioned BY (stat_date string) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dm/ubi_dm_address_recognition/'")
sql(hiveContext,paste0("ALTER TABLE ubi_dm_address_recognition add PArtition (stat_date='",date_period,"01')"))
sparkR.stop()
