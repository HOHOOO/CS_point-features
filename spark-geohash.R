library(magrittr)
SparkR:::includePackage(sqlContext, 'geohash')
SparkR:::includePackage(sqlContext, 'Rcpp')
arg<-2
trip<-sql(hiveContext,"SELECT lat,lon,speed FROM obd_gps_second")
trip = trip %>% withColumn("hash8", lit("0")) %>% withColumn("hash7", lit("0")) %>% withColumn("hash6", lit("0"))
rdd<-SparkR:::toRDD(trip)
rdd<-SparkR:::map(rdd, function(x) {
  user<-matrix(unlist(x),floor(length(unlist(x))/6),ncol=6,byrow=T)
  library('geohash')
  user[,2] <- as.vector(user[,2])
  user[,3] <- as.vector(user[,3])
  a <- user[,2]
  mode(a) <- "numeric"
  b <- user[,3]
  mode(b) <- "numeric"
  user[1,6] <-geohash::gh_encode(a,b,6)
  user[1,5] <-geohash::gh_encode(a,b,7)
  user[1,4] <-geohash::gh_encode(a,b,8)
  user
})

SparkR:::saveAsTextFile(rdd, paste0("/user/yyl/geohash/test",arg))
sql(hiveContext,"drop TABLE hoho_test_geohash")
sql(hiveContext,paste0("CREATE external TABLE hoho_test_geohash (lat DOUBLE,lon DOUBLE,speed DOUBLE,hash8 String,hash7 String,hash6 String) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dw/cluster_point/stat_date=",date_period,"01'"))
sql(hiveContext,"insert overwrite table TABLE hoho_test_geohash select trim(lat) as lat,trim(lon) as lon,trim(speed) as speed,trim(hash8) as hash8,trim(hash7) as hash7,trim(hash6) as hash6 from hoho_test_geohash")


sql(hiveContext,paste0("CREATE external TABLE hoho_test_geohash (lat DOUBLE,lon DOUBLE,speed DOUBLE,hash8 String,hash7 String,hash6 String) ROW format delimited FIELDS TERMINATED BY ',' LOCATION '/user/kettle/ubi/dw/time_distribution/stat_date=2016'"))
