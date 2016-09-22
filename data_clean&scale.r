
########################
### ???ֻ?ͼ????2.0  ###
########################

rm(list=ls())
if (!require("RCurl"))install.packages('RCurl')
library(RCurl)
if (!require("stringr"))install.packages('stringr')
library(stringr)
if (!require("tidyr"))install.packages('tidyr')
library(tidyr)
if (!require("tcltk"))install.packages('tcltk')
library(tcltk)
if (!require("DT"))install.packages('DT')
library(DT)
library(dplyr)
library(leaflet)
library(plotly)
alldata <- read.csv("E:/Data/3JIANGXI/trip_statistics_20160821.csv",header = T)
alldata$deviceid <- as.character(alldata$deviceid)
alldata <- subset(alldata,subset = (alldata$deviceid!=0&!is.na(alldata$deviceid)&!is.na(alldata$End)))

########################
### Set Dura2 & ID2  ###
########################
user_table <- as.data.frame(sort(table(alldata$deviceid)))
names(user_data) <- c("ID","frequency")
user_table$ID2 <- c(1:length(user_table$Var1))
# save(user_table,file = "user_table.rda")
alltable <- merge(alldata,user_table,by.x = "deviceid",by.y = "Var1",all.x = T)
alldata <- alldata[order(alldata$ID2,alldata$Start,decreasing=F),]
for(i in 1:(length(alldata$ID2)-1))
{
  if(alldata$ID2[i]==alldata$ID2[i+1])
  {alldata$Dura2[i] <- alldata$Start[i+1]-alldata$End[i]}else{
    alldata$Dura2[i] <- 0
  }
}
alldata$Dura2 <- alldata$Dura2/3600/24
# save(alldata,file = "alldata_Dura2_NA.rda")

names(alldata) <- c('deviceid','tid','vin','Start','End','Dura','Period','Lat.St','Lon.St','Lat.En','Lon.En','m','Lat.St','Lon.St','Lat.En','Lon.En','m','Speed.Mean','GPS.Speed.Sd','Speed.Sd','Freq','ID2','Dura2','Dura_flag')
# names(alldata) <- c('deviceid','tid','vin','Start','End','Dura','Period','Lat.St','Lon.St','Lat.En','Lon.En','m','Lat.St.def','Lon.St.def','Lat.En.def','Lon.En.def','m.def','Speed.Mean','GPS.Speed.Sd','Speed.Sd','Freq','ID2','Dura2')


# combine_trip ------------------------------------------------------------
########################
### Set User_table   ###
########################
Dura<-0.003
for(i in 1:dim(alldata)[1])
{
  if(alldata$Dura2[i]>=Dura)
  {alldata$Dura_flag[i]<-1}
  else
  {alldata$Dura_flag[i]<-0}
}
flag_data<-subset(alldata,subset=(Dura_flag==1))
save(alldata,file = "alldata_flag.rda")

########################
### Set combine table###
########################
journey_combine<-data.frame(matrix(0,dim(flag_data)[1],ncol=15))
names(journey_combine)<-c('ID2','Start','End','Dura','Period','Lat.St','Lon.St','Lat.En','Lon.En','m','Speed.Mean','GPS.Speed.Sd','Speed.Sd','Dura2','Dura_flag')


pb <- tkProgressBar("????","?????? %",  0, 100)
########################
### Combine Table  m ###
########################
m<-1
while(m<=dim(flag_data)[1])
{
  ########################
  ### User Loop & dura2###
  ########################  
  for(k in 1:max(alldata$ID2))
  {
    userk<-subset(alldata,subset=(ID2==k))
    if(length(userk$deviceid)==0){      
    journey_combine$ID2[m]<-k
    journey_combine$Start[m]<-0
    journey_combine$End[m]<-0
    journey_combine$Dura[m]<-0
    journey_combine$Period[m]<-0
    journey_combine$Lat.St[m]<-0
    journey_combine$Lon.St[m]<-0
    journey_combine$Lat.En[m]<-0
    journey_combine$Lon.En[m]<-0
    journey_combine$m[m]<-0
    journey_combine$Speed.Mean[m]<-0
    journey_combine$Speed.Sd[m]<-0
    journey_combine$GPS.Speed.Sd[m]<-0
    journey_combine$Dura2[m]<-0
    journey_combine$Dura_flag[m]<-0
    next}else{
    if(length(userk$deviceid)==1){
      journey_combine$ID2[m]<-userk$ID2[1]
      journey_combine$Start[m]<-userk$Start[i]
      journey_combine$End[m]<-userk$End[i]
      journey_combine$Dura[m]<-sum(userk$Dura[i:i])
      journey_combine$Period[m]<-mean(userk$Period[i:i])
      journey_combine$Lat.St[m]<-userk$Lat.St[i]
      journey_combine$Lon.St[m]<-userk$Lon.St[i]
      journey_combine$Lat.En[m]<-userk$Lat.En[i]
      journey_combine$Lon.En[m]<-userk$Lon.En[i]
      journey_combine$m[m]<-sum(userk$m[i:i])
      journey_combine$Speed.Mean[m]<-userk$Speed.Mean[i]
      journey_combine$Speed.Sd[m]<-userk$Speed.Sd[i]
      journey_combine$GPS.Speed.Sd[m]<-userk$GPS.Speed.Sd[i]
      journey_combine$Dura2[m]<-userk$Dura2[i]
      journey_combine$Dura_flag[m]<-userk$Dura_flag[i]
      next
    }else{
    for(i in 1:(length(userk$deviceid)-1))
    {
      userk$Dura_flag[length(userk$deviceid)+1-i] <- userk$Dura_flag[length(userk$deviceid)-i]
    }
    userk$Dura_flag[1] <- 1
    ########################
    ### User[k] Loop   i ###
    ########################
    for(i in 1:dim(userk)[1])
    {
      if(userk$Dura_flag[i]==1 && (sum(userk$Dura_flag[i:(i+1)])==2 ||i==dim(userk)[1]))
      {
        journey_combine$ID2[m]<-userk$ID2[1]
        journey_combine$Start[m]<-userk$Start[i]
        journey_combine$End[m]<-userk$End[i]
        journey_combine$Dura[m]<-sum(userk$Dura[i:i])
        journey_combine$Period[m]<-mean(userk$Period[i:i])
        journey_combine$Lat.St[m]<-userk$Lat.St[i]
        journey_combine$Lon.St[m]<-userk$Lon.St[i]
        journey_combine$Lat.En[m]<-userk$Lat.En[i]
        journey_combine$Lon.En[m]<-userk$Lon.En[i]
        journey_combine$m[m]<-sum(userk$m[i:i])
        journey_combine$Speed.Mean[m]<-userk$Speed.Mean[i]
        journey_combine$Speed.Sd[m]<-userk$Speed.Sd[i]
        journey_combine$GPS.Speed.Sd[m]<-userk$GPS.Speed.Sd[i]
        journey_combine$Dura2[m]<-userk$Dura2[i]
        journey_combine$Dura_flag[m]<-userk$Dura_flag[i]
        m<-m+1
      }
      else
      {
        if(userk$Dura_flag[i]==1 && (sum(userk$Dura_flag[i:(i+1)])!=2 && i!=dim(userk)[1]))
        {
          ########################
          ### Combined journey j ###
          ########################
          for(j in 1:(length(userk$Dura_flag)-i))
          {
            if(userk$Dura_flag[i]==1 && (sum(userk$Dura_flag[i:(i+j-1)])==2 || i==dim(userk)[1]))
            { 
              
              journey_combine$ID2[m]<-userk$ID2[1]
              journey_combine$Start[m]<-userk$Start[i]
              journey_combine$End[m]<-userk$End[i+j-1]
              journey_combine$Dura[m]<-(userk$End[i+j-1]-userk$Start[i])/60
              journey_combine$Period[m]<-mean(userk$Period[i:(i+j-1)])
              journey_combine$Lat.St[m]<-userk$Lat.St[i]
              journey_combine$Lon.St[m]<-userk$Lon.St[i]
              journey_combine$Lat.En[m]<-userk$Lat.En[i+j-1]
              journey_combine$Lon.En[m]<-userk$Lon.En[i+j-1]
              journey_combine$m[m]<-sum(userk$m[i:(i+j-1)])
              journey_combine$Speed.Mean[m]<-journey_combine$m[m]/journey_combine$Dura[m]/60
              Speed.Sd<-sqrt((userk$Speed.Sd[i]^2)+(userk$Speed.Sd[i+1]^2)-userk$Speed.Sd[i]*userk$Speed.Sd[i+1])
              GPS.Speed.Sd<-sqrt((userk$GPS.Speed.Sd[i]^2)+(userk$GPS.Speed.Sd[i+1]^2)-userk$GPS.Speed.Sd[i]*userk$GPS.Speed.Sd[i+1])
              for(l in 2:(j-1))
              {
                Speed.Sd<-sqrt(Speed.Sd^2+userk$Speed.Sd[i+l]^2-Speed.Sd*userk$Speed.Sd[i+l])
                GPS.Speed.Sd<-sqrt(GPS.Speed.Sd^2+userk$GPS.Speed.Sd[i+l]^2-GPS.Speed.Sd*userk$GPS.Speed.Sd[i+l])
              }
              journey_combine$GPS.Speed.Sd[m]<-GPS.Speed.Sd
              journey_combine$Speed.Sd[m]<-Speed.Sd
              journey_combine$Dura2[m]<-userk$Dura2[i+j-1]
              journey_combine$Dura_flag[m]<-userk$Dura_flag[i+j-1]
              m<-m+1
              break
            }else{next}
          }
        }else{next}
      }}
    }}
    info <- sprintf("?????????? %d%%", round(k*100/max(alldata$ID2)))
    setTkProgressBar(pb, k*100/max(alldata$ID2), sprintf("???? (%s)", info), info)
  }
}
close(pb)
alldata <- journey_combine
save(alldata,file = "alldata_combine.rda")
# is_ture & is_district ---------------------------------------------------
alldata <- subset(alldata,subset=(alldata$m/alldata$Dura/alldata$Speed.Mean<=1.5))
alldata <- subset(alldata,subset = (alldata$Lat.St>=28.513548 & alldata$Lat.St<=28.802762
                              & alldata$Lat.En>=28.513548 & alldata$Lat.En<=28.802762
                              & alldata$Lon.St>=115.636345 & alldata$Lon.St<=116.078888
                              & alldata$Lon.En>=115.636345 & alldata$Lon.En<=116.078888))
save(alldata,file = "alldata_nanchang.rda")
# save(alldata,file = "alldata_nanchang_pre_rating.rda")
# rating_scale ------------------------------------------------------------
alldata$Speed.Mean<-floor(alldata$Speed.Mean)
alldata$Dura<-floor(alldata$Dura)
alldata$Speed.Sd<-floor(alldata$Speed.Sd*1000)
########################
### speed scale      ###
########################
#hist(alldata$Speed.Mean,breaks=max(alldata$Speed.Mean),xlim=c(0,200),col="red",xlab="Speed.Mean")
speed_scale <- as.data.frame(sort(table(alldata$Speed.Mean)))
speed_scale$scale<-speed_scale$Freq/max(speed_scale$Freq)*100
speed_scale <- order(peed_scale[,Var1],decreasing = F)
names(speed_scale) <- c("speed","Freq","speed_scale")
speed_scale<-speed_scale[1:80,c('speed','speed_scale')]
########################
### period   scale   ###
########################
# Period_scale<-read.table("period.csv",header=TRUE,sep=",")
########################
### Dura scale       ###
########################
Dura_scale <- as.data.frame(sort(table(alldata$Dura.Mean)))
Dura_scale$scale<-Dura_scale$Freq/max(Dura_scale$Freq)*100
Dura_scale <- order(peed_scale[,Var1],decreasing = F)
names(Dura_scale) <- c("Dura","Freq","scale")
Dura_scale<-Dura_scale[1:120,c('Dura','Dura_scale')]
########################
### ACC Scale        ###
########################
#hist(alldata$Speed.Sd,breaks=max(alldata$Speed.Sd),col="red",xlab="Speed.Sd")
ACC_scale <- as.data.frame(sort(table(alldata$ACC.Mean)))
ACC_sum <- sum(ACC_scale$Freq)
ACC_max <- max(ACC_scale$Freq)/ACC_sum
ACC_scale$Freq <- ACC_max-ACC_scale$Freq/ACC_sum
ACC_scale$Freq <- ACC_scale$Freq/sum(ACC_scale$Freq)*100
ACC_scale <- order(peed_scale[,Var1],decreasing = F)
ACC_scale$scale[1]<-100
for(i in 2:max(ACC_scale$Freq))
{
  ACC_scale$scale[i]<-ACC_scale$scale[i-1]-ACC_scale$Freq[i]
}
names(ACC_scale) <- c("ACC","Freq","ACC_scale")
ACC_scale<-ACC_scale[1:120,c('ACC','ACC_scale')]

# show_scale --------------------------------------------------------------
write.csv(speed_scale,file = "C:\\Users\\HOHO\\Desktop\\speed_scale.csv")
write.csv(Dura_scale,file = "C:\\Users\\HOHO\\Desktop\\Dura_scale.csv")
# write.csv(Period_scale,file = "C:\\Users\\HOHO\\Desktop\\Period_scale.csv")
write.csv(ACC_scale,file = "C:\\Users\\HOHO\\Desktop\\ACC_scale.csv")



# alldata$Speed.Mean<-floor(alldata$Speed.Mean)
# alldata$Dura<-floor(alldata$Dura)
# alldata$Speed.Sd<-floor(alldata$Speed.Sd*1000)

alldata <- merge(alldata,speed_scale,by.x = "Speed.Sd",by.y = "speed_scale",all.x = T)
alldata <- alldata[,-(dim(alldata)[2]-1)]

alldata <- merge(alldata,Dura_scale,by.x = "Dura",by.y = "Dura_scale",all.x = T)
alldata <- alldata[,-(dim(alldata)[2]-1)]

alldata <- merge(alldata,Period_scale,by.x = "Period",by.y = "Period_scale",all.x = T)
alldata <- alldata[,-(dim(alldata)[2]-1)]

alldata <- merge(alldata,ACC_scale,by.x = "Speed.Mean",by.y = "ACC_scale",all.x = T)
alldata <- alldata[,-(dim(alldata)[2]-1)]

alldata[is.na(alldata)] <- 0

alldata$rating <- (alldata$speed_scale+alldata$Dura_scalet+alldata$Period_scale+alldata$ACC_scale)/4

# alldata <- subset(alldata,subset=(alldata$rating!=0))
# save(alldata,file = "alldata_nanchang_rating.rda")
# save(alldata,file = "alldata_nanchang_combine_rating.rda")
alldata <- alldata[order(alldata$ID2,alldata$Start,decreasing=F),]
demo_data <- alldata[1:30000,]
p <- plot_ly(demo_data, x = ID2, y = rating, type = "box",showlegend = FALSE)
plot_ly(demo_data, x = ID2, y = rating, mode = "markers")
plot_ly(demo_data, x = Speed.Mean, y =rating , size = Dura,color = Speed.Sd, opacity=Period.point, mode = "markers")

alldata$rating_floor <- as.character(floor(alldata$rating))

alldata %>% count(rating_floor) %>%
  plot_ly(x = rating_floor, y = n, type = "bar")
plot_ly(alldata,x = alldata$rating_floor, y = n, type = "bar", color = rating_floor)

demo_data$rating_floor <- as.character(floor(demo_data$rating_floor/20))
demo_data %>% count(ID2, rating_floor) %>%
  plot_ly(x = ID2, y = n, type = "bar", color = rating_floor)

