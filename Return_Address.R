rm(list=ls())
# input -------------------------------------------------------------------
path<-"E:\\Data/1BEIJING"
setwd(path)
library(SoDA)
library(gdata)
library(plyr)
library(RCurl)
Users <- read.csv("Users.CSV",header=TRUE,sep=",")
all_towncode <- read.csv("2015_all_towncode.csv",header=TRUE,sep=",")
Users <- as.data.frame(Users)
for (k in 1:length(Users$home)) {
  if(!is.na(strsplit(as.character(Users$home[k]),split="_")[[1]][6])){Users$home2[k] <- strsplit(as.character(Users$home[k]),split="_")[[1]][6]}else{Users$home2[k] <- 0}
  if(!is.na(strsplit(as.character(Users$company[k]),split="_")[[1]][6])){Users$company2[k] <- strsplit(as.character(Users$company[k]),split="_")[[1]][6]}else{Users$company2[k] <- 0}
}
# library(parallel)
# cl <- makeCluster(detectCores(logical = F))
# clusterExport(cl, "vector.origin.xyz")
# vector.frequency <- parApply(cl=cl,vector.origin.unique,MARGIN = 1,
#                              function(x) 
#                                sum(vector.origin.xyz$Acc.x==x[1] & 
#                                      vector.origin.xyz$Acc.y==x[2] & 
#                                      vector.origin.xyz$Acc.z==x[3]))
# stopCluster(cl)

# some fuction ------------------------------------------------------------

regeo <- function(location)
{
  output='xml'
  radius= 1000
  extensions="all"
  batch='false'
  homeorcorp= 0
  map_ak = '196506b424d4b7983d6c6a0a358165e8'
  location <- as.matrix(location)
  lon = location[2]
  lat = location[1]
  location = paste0(lon, ",", lat)
  url_head1 = paste0("http://restapi.amap.com/v3/assistant/coordinate/convert?locations=")
  url_tail1 = paste0("&coordsys=gps&output=xml&key=196506b424d4b7983d6c6a0a358165e8")
  url1 = paste0(url_head1, location, url_tail1)
  str1 <- getURL(url1)
  location <- gsub(".*<locations>(.*?)</locations>.*", '\\1', str1)
  lon <- as.numeric(strsplit(as.character(location),split=",")[[1]][1])
  lat <- as.numeric(strsplit(as.character(location),split=",")[[1]][2])
  
  url_head = paste0("http://restapi.amap.com/v3/geocode/regeo?key=", map_ak, "&location=")
  url_tail = paste0("&poitype=&radius=",radius,"&extensions=",extensions,"&batch=", batch,"&homeorcorp=",homeorcorp, "&output=",output, "&roadlevel=1")
  url = paste0(url_head, lon, ",", lat, url_tail)
  geo <- gsub(".*<formatted_address>(.*?)</formatted_address>.*", '\\1', getURL(url))
  return(geo)
}

re_district <- function(location)
{
  output='xml'
  radius= 1000
  extensions="all"
  batch='false'
  homeorcorp= 0
  map_ak = '196506b424d4b7983d6c6a0a358165e8'
  location <- as.matrix(location)
  lon = location[2]
  lat = location[1]
  location = paste0(lon, ",", lat)
  url_head1 = paste0("http://restapi.amap.com/v3/assistant/coordinate/convert?locations=")
  url_tail1 = paste0("&coordsys=gps&output=xml&key=196506b424d4b7983d6c6a0a358165e8")
  url1 = paste0(url_head1, location, url_tail1)
  str1 <- getURL(url1)
  location <- gsub(".*<locations>(.*?)</locations>.*", '\\1', str1)
  lon <- as.numeric(strsplit(as.character(location),split=",")[[1]][1])
  lat <- as.numeric(strsplit(as.character(location),split=",")[[1]][2])
  
  url_head = paste0("http://restapi.amap.com/v3/geocode/regeo?key=", map_ak, "&location=")
  url_tail = paste0("&poitype=&radius=",radius,"&extensions=",extensions,"&batch=", batch,"&homeorcorp=",homeorcorp, "&output=",output, "&roadlevel=1")
  url = paste0(url_head, lon, ",", lat, url_tail)
  geo <- gsub(".*<township>(.*?)</township>.*", '\\1', getURL(url))
  return(geo)
}

return_all_district <- function()
{
  result <- getURL("http://restapi.amap.com/v3/config/district?key=196506b424d4b7983d6c6a0a358165e8&keywords=%E5%8C%97%E4%BA%AC&level=city&subdistrict=3&extensions=base")
  rule<-gregexpr("name\":\"(.*?)\"", result)
  value<-as.data.frame(unlist(regmatches(result, rule)))
  value <- substr(value$`unlist(regmatches(result, rule))`,8,(length(value$`unlist(regmatches(result, rule))`)))
  rule2 <- gregexpr("center\":\"(.*?)\"", result)
  value2<-as.data.frame(unlist(regmatches(result, rule2)))

  value2 <- substr(value2$`unlist(regmatches(result, rule2))`,10,(length(value2$`unlist(regmatches(result, rule2))`)))
  name <- as.data.frame(gsub("(.*?)\"", '\\1', value))
  GPS <- as.data.frame(gsub("(.*?)\"", '\\1', value2))
  all_district <- cbind(name,GPS)
  names(all_district) <- c("name","GPS")
  return(all_district)
}

re_towncode <- function(location)
{
  output='xml'
  radius= 1000
  extensions="all"
  batch='false'
  homeorcorp= 0
  map_ak = '196506b424d4b7983d6c6a0a358165e8'
  location <- as.matrix(location)
  lon = location[2]
  lat = location[1]
  location = paste0(lon, ",", lat)
  url_head1 = paste0("http://restapi.amap.com/v3/assistant/coordinate/convert?locations=")
  url_tail1 = paste0("&coordsys=gps&output=xml&key=196506b424d4b7983d6c6a0a358165e8")
  url1 = paste0(url_head1, location, url_tail1)
  str1 <- getURL(url1)
  location <- gsub(".*<locations>(.*?)</locations>.*", '\\1', str1)
  lon <- as.numeric(strsplit(as.character(location),split=",")[[1]][1])
  lat <- as.numeric(strsplit(as.character(location),split=",")[[1]][2])
  
  url_head = paste0("http://restapi.amap.com/v3/geocode/regeo?key=", map_ak, "&location=")
  url_tail = paste0("&poitype=&radius=",radius,"&extensions=",extensions,"&batch=", batch,"&homeorcorp=",homeorcorp, "&output=",output, "&roadlevel=1")
  url = paste0(url_head, lon, ",", lat, url_tail)
  geo <- gsub(".*<towncode>(.*?)</towncode>.*", '\\1', getURL(url))
  geo <- substr(geo,1,9)
  return(geo)
}

# compute -----------------------------------------------------------------

for(k in 1:length(Users$home))
{
  if(strsplit(as.character(Users$home[k]),split="_")[[1]][1]!=0 |strsplit(as.character(Users$company[k]),split="_")[[1]][1]!=0){
    First_data<-read.table(paste(k,"user.csv",sep = " "),header=TRUE,sep=",")
    if(strsplit(as.character(Users$home[k]),split="_")[[1]][1]!=0)
    {
      home_locations <- subset(First_data,subset = (First_data$Sort.En==strsplit(as.character(Users$home[k]),split="_")[[1]][1]))[c('Lat.En','Lon.En')]
      home_location <- c(mean(home_locations$Lat.En),mean(home_locations$Lon.En))
      Users$home_address[k] <- regeo(home_location)
      Users$home_district[k] <- re_district(home_location)
      Users$home_address_lat[k] <- home_location[1]
      Users$home_address_lon[k] <- home_location[2]
      Users$home_towncode[k] <- re_towncode(home_location)
    }else{
      Users$home_address[k] <- 0
      Users$home_district[k] <- 0
      Users$home_towncode[k] <- 0
      Users$home_address_lat[k] <- 0
      Users$home_address_lon[k] <- 0
    }

    if(strsplit(as.character(Users$home2[k]),split="_")[[1]][1]!=0)
    {
      home2_locations <- subset(First_data,subset = (First_data$Sort.En==strsplit(as.character(Users$home2[k]),split="_")[[1]][1]))[c('Lat.En','Lon.En')]
      home2_location <- c(mean(home2_locations$Lat.En),mean(home2_locations$Lon.En))
      Users$home2_address[k] <- regeo(home2_location)
      Users$home2_towncode[k] <- re_towncode(home2_location)
      Users$home2_district[k] <- re_district(home2_location)
      Users$home2_address_lat[k] <- home2_location[1]
      Users$home2_address_lon[k] <- home2_location[2]
    }else{
      Users$home2_address[k] <- 0
      Users$home2_district[k] <- 0
      Users$home2_address_lat[k] <- 0
      Users$home2_address_lon[k] <- 0
      Users$home2_towncode[k] <- 0
    }
    
    if(strsplit(as.character(Users$company[k]),split="_")[[1]][1]!=0)
    {
      company_locations <- subset(First_data,subset = (First_data$Sort.En==strsplit(as.character(Users$company[k]),split="_")[[1]][1]))[c('Lat.En','Lon.En')]
      company_location <- c(mean(company_locations$Lat.En),mean(company_locations$Lon.En))
      Users$company_address[k] <- regeo(company_location)
      Users$company_district[k] <- re_district(company_location)
      Users$company_towncode[k] <- re_towncode(company_location)
      Users$company_address_lat[k] <- company_location[1]
      Users$company_address_lon[k] <- company_location[2]
    }else{
      Users$company_address[k] <- 0
      Users$company_district[k] <- 0
      Users$company_towncode[k] <- 0
      Users$company_address_lat[k] <- 0
      Users$company_address_lon[k] <- 0
    }
    
    if(strsplit(as.character(Users$company2[k]),split="_")[[1]][1]!=0)
    {
      company2_locations <- subset(First_data,subset = (First_data$Sort.En==strsplit(as.character(Users$company2[k]),split="_")[[1]][1]))[c('Lat.En','Lon.En')]
      company2_location <- c(mean(company2_locations$Lat.En),mean(company2_locations$Lon.En))
      Users$company2_address[k] <- regeo(company2_location)
      Users$company2_district[k] <- re_district(company2_location)
      Users$company2_towncode[k] <- re_towncode(company2_location)
      Users$company2_address_lat[k] <- company2_location[1]
      Users$company2_address_lon[k] <- company2_location[2]
    }else{
      Users$company2_address[k] <- 0
      Users$company2_district[k] <- 0
      Users$company2_towncode[k] <- 0
      Users$company2_address_lat[k] <- 0
      Users$company2_address_lon[k] <- 0
    }
  }else{
    Users$home_address[k] <- 0
    Users$company_address[k] <- 0
    Users$home2_address[k] <- 0
    Users$company2_address[k] <- 0
    Users$company2_address_lat[k] <- 0
    Users$company2_address_lon[k] <- 0
    Users$company_address_lat[k] <- 0
    Users$company_address_lon[k] <- 0
    Users$home2_address_lat[k] <- 0
    Users$home2_address_lon[k] <- 0
    Users$home_address_lat[k] <- 0
    Users$home_address_lon[k] <- 0
    Users$home_district[k] <- 0
    Users$company_district[k] <- 0
    Users$home2_district[k] <- 0
    Users$company2_district[k] <- 0
    Users$home_towncode[k] <- 0
    Users$company_towncode[k] <- 0
    Users$home2_towncode[k] <- 0
    Users$company2_towncode[k] <- 0
  }

}

# merge table -------------------------------------------------------------
all_district <- return_all_district()
Users <- merge(Users,all_district,by.x="home_district",by.y="name",all.x =T)
Users$home_district_GPS <- Users$GPS
Users <- Users[,-(dim(Users)[2]-1)]
Users <- merge(Users,all_district,by.x="company_district",by.y="name",all.x =T)
Users$company_district_GPS <- Users$GPS
Users <- Users[,-(dim(Users)[2]-1)]
Users <- merge(Users,all_district,by.x="home2_district",by.y="name",all.x =T)
Users$home2_district_GPS <- Users$GPS
Users <- Users[,-(dim(Users)[2]-1)]
Users <- merge(Users,all_district,by.x="company2_district",by.y="name",all.x =T)
Users$company2_district_GPS <- Users$GPS
Users <- Users[,-(dim(Users)[2]-1)]
