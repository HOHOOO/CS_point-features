rm(list=ls())
# path.root<-"E:\\"
path<-"E:\\Data/1BEIJING"
setwd(path)

if (!require("SoDA"))install.packages('SoDA')
library(SoDA)
if (!require("gdata"))install.packages('gdata')
library(gdata)
if (!require("dplyr"))install.packages('dplyr')
library(dplyr)


# 读入统计表 -------------------------------------------------------------------
alldata<-read.table("journey_statistics.csv",header=TRUE,sep=",")

#add feature
#alldata$Weekday<-
alldata$Sort.En<-0
alldata$Avg.Lat.En<-0
alldata$Avg.Lon.En<-0
alldata$Num.En<-0
alldata$Sort.St<-0
alldata$Avg.Lat.St<-0
alldata$Avg.Lon.St<-0
alldata$Num.St<-0

#test switch
testdata<-alldata

#journey end/start point
dlenhome<-data.frame(matrix(0,1000,ncol=1000))
dlenstart<-data.frame(matrix(0,1000,ncol=1000))
testdata$set<-0
testdata$ID2 <- 1
for(k in 1:max(testdata$ID2))
{
  test1<-subset(testdata,subset=(ID2==k))
  d1<-subset(test1[1:length(test1$ID2),c('Lat.En','Lon.En','Lat.St','Lon.St','set')])
  
  d2<-subset(test1[1:length(test1$ID2),c('Lat.St','Lon.St','Lat.En','Lon.En','set')])
  names(d2)<-c('Lat.En','Lon.En','Lat.St','Lon.St','set')
  d<-rbind(d1,d2)
  
  d$set<-(1:length(d$set))
  for(i in 1:(length(d$set)-1))
  {
    for(j in (i+1):length(d$set))
    {
      if(geoDist(d[i,'Lat.En'],d[i,'Lon.En'],d[j,'Lat.En'],d[j,'Lon.En'])<200)
      {
        d$set[j]<-d$set[i]
      }
      else
      {
      }
    }
  }
  
  d1<-subset(d[1:length(test1$ID2),c('Lat.En','Lon.En','Lat.St','Lon.St','set')])
  d2<-subset(d[(length(test1$ID2)+1):(2*length(test1$ID2)),c('Lat.En','Lon.En','Lat.St','Lon.St','set')])
  for(i in 1:length(test1$ID2))
  {
    d1$set1[i]<-d2$set[i]
  }
  
  d<-subset(test1[1:length(test1$ID2),c('Lat.En','Lon.En')])
  ddist<-dist(d)
  dlenhome[k,5]<-dim(d)[1]
  ddist<-data.frame(matrix(0,dim(d)[1],ncol=dim(d)[1]))
  for(i in 1:dim(d)[1])
  {
    for(j in 1:dim(d)[1])
    {	
      ddist[i,j]<-geoDist(d[i,1],d[i,2],d[j,1],d[j,2])
    }
  } 
  for(i in 1:dim(d)[1])
  {
    
    dlenhome[k,i+5]<-dim((subset(ddist[i],ddist[i]<200)))[1]-1
  }
  for(i in 1:dim(d)[1])
  {
    test1$flag.En[i]<-dlenhome[k,i+5]
  }
  d1<-subset(test1[1:length(test1$ID2),c('Lat.St','Lon.St')])
  ddist1<-dist(d1)
  dlenstart[k,5]<-dim(d1)[1]
  ddist1<-data.frame(matrix(0,dim(d1)[1],ncol=dim(d1)[1]))
  for(i in 1:dim(d1)[1])
  {
    for(j in 1:dim(d1)[1])
    {	
      ddist1[i,j]<-geoDist(d1[i,1],d1[i,2],d1[j,1],d1[j,2])
    }
  } 
  for(i in 1:dim(d1)[1])
  {
    
    dlenstart[k,i+5]<-dim((subset(ddist1[i],ddist1[i]<200)))[1]-1
  }
  for(i in 1:dim(d1)[1])
  {
    test1$flag.St[i]<-dlenstart[k,i+5]
  }
  
  #聚点数量排序并去重
  #arrangepoint<-arrange(as.data.frame(test1)$Dura_flag)
  arr.En<-unique(test1$flag.En)
  arr.En<-arrange(as.data.frame(arr.En),-arr.En)
  arr.St<-unique(test1$flag.St)
  arr.St<-arrange(as.data.frame(arr.St),-arr.St)
  #   dlen<-0
  #   dlen<-as.daata.frame(dlen)
  
  for(m in 1:dim(arr.En)[1])
  {
    #for(x in 1:dim(test1)[1])
    # maxpoint<-subset(test1,subset=(test1$Dura_flag[x]==arr[m,1]))
    temp<-arr.En[m,1]
    assign(paste("maxpoint.En", m, sep = ""),subset(test1,subset=(test1$flag.En==temp)))
  }
  #get(paste("maxpoint", m, sep = ""))<-subset(test1,subset=(test1$Dura_flag==temp))
  for(m in 1:dim(arr.St)[1])
  {
    temp<-arr.St[m,1]
    assign(paste("maxpoint.St", m, sep = ""),subset(test1,subset=(test1$flag.St==temp)))
  }
  #sift point
  
  maxpoint.En<-data.frame(matrix(0,dim(arr.En)[1],ncol=5))
  maxpoint.St<-data.frame(matrix(0,dim(arr.St)[1],ncol=5))
  names(maxpoint.En)<-c('Lat','Lon','flag.En','Num','class')
  names(maxpoint.St)<-c('Lat','Lon','flag.St','Num','class')
  for(m in 1:dim(arr.En)[1])
  {
    maxpoint.En$Lat[m]<-mean(get(paste("maxpoint.En", m, sep = ""))$Lat.En)
    maxpoint.En$Lon[m]<-mean(get(paste("maxpoint.En", m, sep = ""))$Lon.En)
    maxpoint.En$flag.En[m]<-get(paste("maxpoint.En", m, sep = ""))$flag.En[1]
    maxpoint.En$Num[m]<-length(get(paste("maxpoint.En", m, sep = ""))$flag.En)
    maxpoint.En$class[m]<-m
  }
  
  for(m in 1:dim(arr.St)[1])
  {
    maxpoint.St$Lat[m]<-mean(get(paste("maxpoint.St", m, sep = ""))$Lat.St)
    maxpoint.St$Lon[m]<-mean(get(paste("maxpoint.St", m, sep = ""))$Lon.St)
    maxpoint.St$flag.St[m]<-get(paste("maxpoint.St", m, sep = ""))$flag.St[1]
    maxpoint.St$Num[m]<-length(get(paste("maxpoint.St", m, sep = ""))$flag.St)
    maxpoint.St$class[m]<-(-m)
  }
  
  
  for(m in 1:dim(arr.En)[1])
  {
    for(n in m:dim(arr.En)[1])
    {
      if(geoDist(maxpoint.En$Lat[m],maxpoint.En$Lon[m],maxpoint.En$Lat[n],maxpoint.En$Lon[n])<1000)
      {
        maxpoint.En$class[n]<-maxpoint.En$class[m]
      }
      else
      {
      }
    }
  }
  
  for(m in 1:dim(arr.St)[1])
  {
    for(n in m:dim(arr.St)[1])
    {
      if(geoDist(maxpoint.St$Lat[m],maxpoint.St$Lon[m],maxpoint.St$Lat[n],maxpoint.St$Lon[n])<1000)
      {
        maxpoint.St$class[n]<-maxpoint.St$class[m]
      }
      else
      {
      }
    }
  }
  
  for(m in 1:dim(arr.En)[1])
  {
    for(n in 1:dim(arr.St)[1])
    {
      if(geoDist(maxpoint.En$Lat[m],maxpoint.En$Lon[m],maxpoint.St$Lat[n],maxpoint.St$Lon[n])<2000)
      {
        maxpoint.St$class[n]<-maxpoint.En$class[m]
      }
      else
      {}
    }
  }
  
  
  for(l in 1:dim(test1)[1])
  {
    for(m in 1:dim(arr.En)[1])
    {
      if(test1$flag.En[l]==get(paste("maxpoint.En", m, sep = ""))$flag.En[1])
      {
        test1$Sort.En[l]<-maxpoint.En$class[m]
        test1$Avg.Lat.En[l]<-maxpoint.En$Lat[m]
        test1$Avg.Lon.En[l]<-maxpoint.En$Lon[m]
        test1$Num.En[l]<- maxpoint.En$Num[m]
      }
      else
      {
      }
    }
  }
  write.csv(test1,file=paste(path,paste(k,'user.csv'),sep='\\'))
}


# home&company ------------------------------------------------------------
maxID2 <- 2163L
Users<-data.frame(matrix(0,ncol=3,nrow=maxID2))

names(Users)<-c('ID','home','company')
for(k in 789:maxID2)
{
  if (length(which(dir()==paste(k,'user.csv',sep=' ')))==0)
  {
    
  }else{
    
 
  testdata<-read.table(paste(k,"user.csv",sep = " "),header=TRUE,sep=",")
  
  if(length(testdata$ID2)[1]<=100)
  {
    
  }else
  {
    # testdata$Dura2[length(testdata$ID2)[1]]<-0
    # for(i in 1:(length(testdata$ID2)[1]-1))
    # {
    #   testdata$Dura2[i]<-testdata$Dura2[i+1]
    # }
    testdata$Start_adj<-testdata$Start-4/24
    testdata$End_adj<-testdata$End-4/24
    
    testdata$Start_Floor<-floor((testdata$Start-floor(testdata$Start))*24)
    testdata$End_Floor<-floor((testdata$End-floor(testdata$End))*24)
    
    testdata$Is_First_St <- 0
    testdata$Is_First_St[1] <- 1
    testdata$Is_Last_St <- 0
    testdata$Is_Last_St[length(testdata$ID2)]<- 1
    testdata$Is_First_long_trip <- 0
    for(l in 2:(length(testdata$ID2)[1]))
    {
      if(floor(testdata$Start_adj[l])-floor(testdata$Start_adj[l-1])==1)
      {
        testdata$Is_First_St[l] <- 1
        testdata$Is_Last_En[l-1] <- 1
      }else
      {
        testdata$Is_First_St[l] <- 0
        testdata$Is_Last_En[l-1] <- 0
      }
      
      if(testdata$Dura2[l]>2/24)
      {
        
        if(testdata$Is_First_St[l]==1)
        {
          testdata$Is_First_long_trip[l] <- 1
        }else{
          for(p in 1:10000)
          {
            if(testdata$Is_Last_En[l-p]!=1)
            {
              if(testdata$Is_First_long_trip[l-p]!=1)
              {
                if(testdata$Is_First_St[l-p]!=1)
                {
                  
                }else{
                  testdata$Is_First_long_trip[l] <- 1
                  break
                }
              }else
              {
                testdata$Is_First_long_trip[l] <- 0
                break
              }
              
            }else
            {
              testdata$Is_First_long_trip[l] <- 0
              break
            }
          }
        }
      }else
      {
      }}    
    
    ## Sub_Sta.
    Point_List <- as.data.frame(table(testdata$Sort.En))
    Point_List_adj <- subset(Point_List,subset = (Point_List$Freq!=1 &Point_List$Freq!=2 ))
    Point_List_adj$Aarive_time <- 0
    ##sort
    First_St <- subset(testdata,subset = (testdata$Is_First_St==1 ))
    First_St_sort <- as.data.frame(sort(table(First_St$Sort.St), decreasing = T))
    First_St_sort <- First_St_sort$Var1[1:3]
    
    Last_En <- subset(testdata,subset = (testdata$Is_Last_En==1 ))
    Last_En_sort <- as.data.frame(sort(table(Last_En$Sort.En), decreasing = T))
    Last_En_sort <- Last_En_sort$Var1[1:3]
    
    first_longtrip_En <- subset(testdata,subset = (testdata$Is_First_long_trip==1 ))
    first_longtrip_sort <- as.data.frame(sort(table(first_longtrip_En$Sort.En), decreasing = T))
    first_longtrip_sort <- first_longtrip_sort$Var1[1:3]
    
    
    
    for(m in 1:length(Point_List_adj$Var1))
    {
      a <- as.numeric(Point_List_adj$Var1[m])
      Pointdata_En <- subset(testdata,subset = (testdata$Sort.En == a))
      Pointdata_St <- subset(testdata,subset = (testdata$Sort.St == a))
      # Point_List_adj$total_time[m] <-sum(Pointdata_En$Dura2)
      # Point_List_adj$Active_Day[m] <-length(table(floor(Pointdata_En$End_adj)))
      if(length(table(floor(Pointdata_En$End_adj)))==0)
      {
        Point_List_adj$time_active_perday[m] <- 0
      }else
      {
        Point_List_adj$time_active_perday[m] <- sum(Pointdata_En$Dura2)/length(table(floor(Pointdata_En$End_adj)))
      }

      b <- which.max(table(Pointdata_En$End_Floor))
      g <- as.data.frame(table(Pointdata_En$End_Floor))
      e <- g$Freq[b]
      d <- subset(g,subset=(g$Freq==e))

      if(length(as.numeric(as.vector(unlist(g[which.max(table(last(d)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Aarive_time[m] <- 0
      }else
      {
        h <- last(d$Var1)
        v <- as.vector(unlist(h))
        mode(v) <- "numeric"
        Point_List_adj$Aarive_time[m] <- v
      }

      bb <- which.max(table(Pointdata_St$Start_Floor))
      cc <- as.data.frame(table(Pointdata_St$Start_Floor))
      ee <- cc$Freq[bb]
      
      dd <- subset(cc,subset=(cc$Freq==ee))


      if(length(as.numeric(as.vector(unlist(cc[which.max(table(last(dd)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Leave_time[m] <- 0
      }else
      {
        hh <- last(dd$Var1)
        vv <- as.vector(unlist(hh))
        mode(vv) <- "numeric"
        Point_List_adj$Leave_time[m] <-  vv
      }
      
      Pointdata_En_Workday <- subset(Pointdata_En,subset = (Pointdata_En$WEEKDAY != 6 &Pointdata_En$WEEKDAY != 7 ))
      
      bbb <- which.max(table(Pointdata_En_Workday$End_Floor))
      ccc <- as.data.frame(table(Pointdata_En_Workday$End_Floor))
      eee <- ccc$Freq[bbb]
      ddd <- subset(ccc,subset=(ccc$Freq==eee))
   

      if(length(as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Aarive_time_workday[m] <- 0
      }else
      {
        hhh <- last(ddd$Var1)
        vvv <- as.vector(unlist(hhh))
        mode(vvv) <- "numeric"
        Point_List_adj$Aarive_time_workday[m] <- vvv
      }
      
      
      
      if(sum(Pointdata_En_Workday$ID2)==0)
      {
        Point_List_adj$times_perWorkday[m]<- 0
      }else
      {
        Point_List_adj$times_perWorkday[m] <- length(Pointdata_En_Workday$ID2)/(last(floor(Pointdata_En_Workday$End_adj))-first(floor(Pointdata_En_Workday$Start_adj))+1)
      }
      
      if(sum(Pointdata_En_Workday$Dura2)==0)
      {
        Point_List_adj$time_perWorkday[m] <- 0
      }else
      {
        Point_List_adj$time_perWorkday[m] <- sum(Pointdata_En_Workday$Dura2)/length(table(floor(Pointdata_En_Workday$End_adj)))
      }
      
      Point_List_adj$check1[m] <- if(Point_List_adj$Aarive_time[m] %in% c(c(0:2),c(17:24))){1}else{0}
      Point_List_adj$check2[m] <- if(Point_List_adj$Leave_time[m] %in% c(4:22)){1}else{0}
      Point_List_adj$check3[m] <- if(Point_List_adj$time_active_perday[m]>=6/24){1}else{0}
      Point_List_adj$check4[m] <- if(Point_List_adj$Var1[m] %in% First_St_sort & Point_List_adj$Var1[m] %in% Last_En_sort ){1}else{0}
      Point_List_adj$check5[m] <- if(Point_List_adj$Aarive_time_workday[m] %in% c(7:15)){1}else{0}
      Point_List_adj$check6[m] <- if(Point_List_adj$time_perWorkday[m] >2/24 & Point_List_adj$time_perWorkday[m]<18/24){1}else{0}
      Point_List_adj$check7[m] <- if(Point_List_adj$times_perWorkday[m] >4/22.75){1}else{0}
      Point_List_adj$check8[m] <- if(Point_List_adj$Var1[m] %in% first_longtrip_sort){1}else{0}
      # 这里为判断家和公司的条件
      if(Point_List_adj$check1[m] +Point_List_adj$check2[m] +Point_List_adj$check3[m] +Point_List_adj$check4[m] == 4)
      {
        Point_List_adj$check9[m] <- 1
      }else
      {
        Point_List_adj$check9[m] <- 0
      }
      
      if(Point_List_adj$check5[m] +Point_List_adj$check6[m] +Point_List_adj$check7[m] +Point_List_adj$check8[m] == 4)
      {
        Point_List_adj$check10[m] <- 1
      }else
      {
        Point_List_adj$check10[m] <- 0
      }}
    
    Point_List_adj_home <- subset(Point_List_adj,subset = (Point_List_adj$check9==1))
    if(length(Point_List_adj_home$Var1)==0)
    {
      Users$home[k]<- 0
    }else
    {
      if(length(Point_List_adj_home$Var1)==1)
      {
        Users$home[k] <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
      }else
      {
        str_home <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
        for(mm in 2:length(Point_List_adj_home$check9))
        {
          Users$home[k] <- paste(str_home,Point_List_adj_home$Var1[mm],Point_List_adj_home$Freq[mm],Point_List_adj_home$Aarive_time[mm],Point_List_adj_home$Leave_time[mm],Point_List_adj_home$time_active_perday[mm],sep="_")
        }
      }
    }
    
    Point_List_adj_company <- subset(Point_List_adj,subset = (Point_List_adj$check10==1))
    if(length(Point_List_adj_company$Var1)==0)
    {
      Users$company[k]<- 0
    }else
    {
      if(length(Point_List_adj_company$Var1)==1)
      {
        Users$company[k] <- paste(Point_List_adj_company$Var1[1],Point_List_adj_company$Freq[1],Point_List_adj_company$Aarive_time_workday[1],Point_List_adj_company$time_perWorkday[1],Point_List_adj_company$times_perWorkday[1],sep="_")
      }else
      {
        str_company <- paste(Point_List_adj_company$Var1[1],Point_List_adj_company$Freq[1],Point_List_adj_company$Aarive_time_workday[1],Point_List_adj_company$time_perWorkday[1],Point_List_adj_company$times_perWorkday[1],sep="_")
        for(mm in 2:length(Point_List_adj_company$check9))
        {
          Users$company[k] <- paste(str_company,Point_List_adj_company$Var1[mm],Point_List_adj_company$Freq[mm],Point_List_adj_company$Aarive_time_workday[mm],Point_List_adj_company$time_perWorkday[mm],Point_List_adj_company$times_perWorkday[mm],sep="_")
        }
      }
    }
    
    Users$ID[k] <- k}}}  
write.csv(Users,"Users.CSV")
