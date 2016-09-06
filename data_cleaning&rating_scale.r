map_str <- leaflet() %>%
  addTiles() %>% addCircles(lng = demo_data$Lon.En.ori, lat = demo_data$Lat.En.ori,radius = 10, layerId = NULL, group = NULL, 
                            weight = 5, opacity = 0.5, fill = TRUE, 
                            fillOpacity = 0.2, dashArray = NULL, popup = NULL, 
                            options = pathOptions(), data = getMapData())

rm(list=ls())
if (!require("RCurl"))install.packages('RCurl')
library(RCurl)
if (!require("stringr"))install.packages('stringr')
library(stringr)
if (!require("tidyr"))install.packages('tidyr')
library(tidyr)
if (!require("tcltk"))install.packages('tcltk')
library(tcltk)
  
alldata <- read.csv("E:/Data/3JIANGXI/trip_statistics_20160821.csv",header = T)
alldata$deviceid <- as.character(alldata$deviceid)
alldata <- subset(alldata,subset = (alldata$deviceid!=0&!is.na(alldata$deviceid)&!is.na(alldata$End)))

########################
### Set Dura2 & ID2  ###
########################
user_table <- as.data.frame(sort(table(alldata$deviceid)))
names(user_data) <- c("ID","frequency")
user_table$ID2 <- c(1:length(user_table$Var1))
# save(user_data,file = "user_table.rda")
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

# change_name -------------------------------------------------------------
# all_var <- as.data.frame(t(names(alldata)))
# temp_name <- "V1"
# for(i in 1:dim(all_var)[2]){temp_name <- paste0(temp_name,",V",i)}
# b <- eval(parse(text=(paste("unite(all_var,",temp_name,",sep = ',')"))))
# b <- as.matrix(unlist(strsplit(as.character(b[1]),split=",")))
# changed_name <- paste0("'",b[1],"'")
# for(i in 2:(dim(all_var)[2])){changed_name <- paste0(changed_name,",'",b[i],"'")}
# changed_name
names(alldata) <- c('deviceid','tid','vin','Start','End','Dura','Period','Lat.St','Lon.St','Lat.En','Lon.En','m','Lat.St','Lon.St','Lat.En','Lon.En','m','Speed.Mean','GPS.Speed.Sd','Speed.Sd','Freq','ID2','Dura2','Dura_flag')



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


pb <- tkProgressBar("进度","已完成 %",  0, 100)
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
    info <- sprintf("进度已完成 %d%%", round(k*100/max(alldata$ID2)))
    setTkProgressBar(pb, k*100/max(alldata$ID2), sprintf("进度 (%s)", info), info)
  }
}
close(pb)
alldata <- journey_combine
save(alldata,file = "alldata_combine.rda")
# is_ture & is_district ---------------------------------------------------
alldata <- subset(alldata,subset=(alldata$m/alldata$Dura/alldata$Speed.Mean<=1.5))
alldata <- subset(alldata,subset = (alldata$Lat.St>=28.513548 && alldata$Lat.St<=28.802762
                              && alldata$Lat.En>=28.513548 && alldata$Lat.En<=28.802762
                              && alldata$Lon.St>=115.636345 && alldata$Lon.St<=116.078888
                              && alldata$Lon.En>=115.636345 && alldata$Lon.En<=116.078888))
# save(alldata,file = "alldata_nanchang.rda")
# rating_scale ------------------------------------------------------------
alldata$Speed.Mean<-floor(alldata$Speed.Mean)
alldata$Dura<-floor(alldata$Dura)
alldata$Speed.Sd<-floor(alldata$Speed.Sd*1000)
########################
### speed scale      ###
########################
journey.Grading.Speed.Mean<-alldata.frame(matrix(0,ncol=2))
names(journey.Grading.Speed.Mean)<-c('Speed.Mean.num','Speed.Mean.point')
#hist(alldata$Speed.Mean,breaks=max(alldata$Speed.Mean),xlim=c(0,200),col="red",xlab="Speed.Mean")
for(i in 1:max(alldata$Speed.Mean))
{
  journey.Grading.Speed.Mean[i,'Speed.Mean.num']<-length(which(alldata$Speed.Mean==i))
}


for(i in 1:max(alldata$Speed.Mean))
{
  journey.Grading.Speed.Mean$Speed.Mean.point<-journey.Grading.Speed.Mean$Speed.Mean.num/max(journey.Grading.Speed.Mean$Speed.Mean.num)*100
}
#Speed.Mean.point.max<-which(journey.Grading.Speed.Mean$Speed.Mean.num==max(journey.Grading.Speed.Mean$Speed.Mean.num),arr.ind=TRUE)
journey.Grading.Speed.Mean<-subset(journey.Grading.Speed.Mean[1:80,c('Speed.Mean.num','Speed.Mean.point')])
########################
### period   scale   ###
########################
# journey.Grading.Period<-read.table("period.csv",header=TRUE,sep=",")
#journey.Grading.Period<-alldata.frame(matrix(0,ncol=2))
#names(journey.Grading.Period)<-c('Period.num','Period.point')
########################
### Dura scale       ###
########################
journey.Grading.Dura<-alldata.frame(matrix(0,ncol=2))
names(journey.Grading.Dura)<-c('Dura.num','Dura.point')
#hist(alldata$Dura,breaks=max(alldata$Dura),col="red",xlab="Dura")
for(i in 1:max(alldata$Dura))
{
  journey.Grading.Dura[i,'Dura.num']<-length(which(alldata$Dura==i))
}

for(i in 1:max(alldata$Dura))
{
  journey.Grading.Dura$Dura.point<-journey.Grading.Dura$Dura.num/max(journey.Grading.Dura$Dura.num)*100
}

journey.Grading.Dura<-subset(journey.Grading.Dura[1:120,c('Dura.num','Dura.point')])
########################
### ACC Scale        ###
########################
journey.Grading.Speed.Sd<-alldata.frame(matrix(0,ncol=3))
names(journey.Grading.Speed.Sd)<-c('Speed.Sd.num','Speed.Sd.point','Speed.Sd.endpoint')
#hist(alldata$Speed.Sd,breaks=max(alldata$Speed.Sd),col="red",xlab="Speed.Sd")
for(i in 1:max(alldata$Speed.Sd))
{
  journey.Grading.Speed.Sd[i,'Speed.Sd.num']<-length(which(alldata$Speed.Sd==i))
}
for(i in 1:max(alldata$Speed.Sd))
{
  journey.Grading.Speed.Sd$Speed.Sd.point<-journey.Grading.Speed.Sd$Speed.Sd.num/sum(journey.Grading.Speed.Sd$Speed.Sd.num)
  journey.Grading.Speed.Sd$Speed.Sd.point<-max(journey.Grading.Speed.Sd$Speed.Sd.point)-journey.Grading.Speed.Sd$Speed.Sd.point
  journey.Grading.Speed.Sd$Speed.Sd.point<-journey.Grading.Speed.Sd$Speed.Sd.num/sum(journey.Grading.Speed.Sd$Speed.Sd.num)*100
}
journey.Grading.Speed.Sd[1,'Speed.Sd.endpoint']<-100
for(i in 2:max(alldata$Speed.Sd))
{
  journey.Grading.Speed.Sd[i,'Speed.Sd.endpoint']<-journey.Grading.Speed.Sd[i-1,'Speed.Sd.endpoint']-journey.Grading.Speed.Sd[i,'Speed.Sd.point']
}
journey.Grading.Speed.Sd<-journey.Grading.Speed.Sd[c(-2)]
# show_scale --------------------------------------------------------------
write.csv(journey.Grading.Speed.Sd,file = "C:\\Users\\HOHO\\Desktop\\journey.Grading.Speed.Sd.csv")
write.csv(journey.Grading.Dura,file = "C:\\Users\\HOHO\\Desktop\\journey.Grading.Dura.csv")
# write.csv(journey.Grading.Period,file = "C:\\Users\\HOHO\\Desktop\\journey.Grading.Period.csv")
write.csv(journey.Grading.Speed.Mean,file = "C:\\Users\\HOHO\\Desktop\\journey.Grading.Speed.Mean.csv")

cbind(year=as.numeric(rownames(a)),a)
merge(alldata,journey.Grading.Speed.Sd,by.x = "",by.y = "",all.x = T)
merge(alldata,journey.Grading.Dura,by.x = "",by.y = "",all.x = T)
merge(alldata,journey.Grading.Period,by.x = "",by.y = "",all.x = T)
merge(alldata,journey.Grading.Speed.Mean,by.x = "",by.y = "",all.x = T)







