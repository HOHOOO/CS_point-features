d <- subset(c,subset=(c$Freq==e))
Point_List_adj$Aarive_time[m] <- as.numeric(as.vector(c[which.max(table(last(d)[1])),"Var1"]))
bb <- which.max(table(Pointdata_St$Start_Floor))
cc <- as.data.frame(table(Pointdata_St$Start_Floor))
ee <- cc$Freq[bb]
dd <- subset(cc,subset=(cc$Freq==ee))
if(length(as.numeric(as.vector(unlist(cc[which.max(table(last(dd)[1])),"Var1"]))))==0)
{
  Point_List_adj$Leave_time[m] <- 0
}
else{
  Point_List_adj$Leave_time[m] <-  as.numeric(as.vector(unlist(cc[which.max(table(first(dd)[1])),"Var1"])))
}
Pointdata_En_Workday <- subset(Pointdata_En,subset = (Pointdata_En$WEEKDAY != 6 &Pointdata_En$WEEKDAY != 7 ))
bbb <- which.max(table(Pointdata_En_Workday$End_Floor))
ccc <- as.data.frame(table(Pointdata_En_Workday$End_Floor))
eee <- c$Freq[bbb]
ddd <- subset(ccc,subset=(ccc$Freq==eee))
if(length(as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"]))))==0)
{
  Point_List_adj$Aarive_time_workday[m] <- 0
}
else
{
  Point_List_adj$Aarive_time_workday[m] <- as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"])))
}
if(sum(Pointdata_En_Workday$ID2)==0)
{
  Point_List_adj$times_perWorkday[m]<- 0
}
else{
  Point_List_adj$times_perWorkday[m] <- length(Pointdata_En_Workday$ID2)/(last(floor(Pointdata_En_Workday$End_adj))-first(floor(Pointdata_En_Workday$Start_adj))+1)
}
if(sum(Pointdata_En_Workday$Dura2)==0)
{
  Point_List_adj$time_perWorkday[m] <- 0
}
else
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
# 杩欓噷涓哄垽鏂鍜屽叕鍙哥殑鏉′欢
if(Point_List_adj$check1[m] +Point_List_adj$check2[m] +Point_List_adj$check3[m] +Point_List_adj$check4[m] == 4)
{
  Point_List_adj$check9[m] <- 1
}
else
{
  Point_List_adj$check9[m] <- 0
}
if(Point_List_adj$check5[m] +Point_List_adj$check6[m] +Point_List_adj$check7[m] +Point_List_adj$check8[m] == 4)
{
  Point_List_adj$check10[m] <- 1
}
else
{
  Point_List_adj$check10[m] <- 0
}
}
Point_List_adj_home <- subset(Point_List_adj,subset = (Point_List_adj$check9[m]==1))
if(length(Point_List_adj_home$Var1)==0)
{
  Users$home[k]<- 0
}
else{
  if(length(Point_List_adj_home$Var1)==1)
  {
    Users$home[k] <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
  }
  else
  {
    str_home <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
    for(mm in 2:length(Point_List_adj_home$check9))
    {
      Users$home[k] <- paste(str_home,Point_List_adj_home$Var1[mm],Point_List_adj_home$Freq[mm],Point_List_adj_home$Aarive_time[mm],Point_List_adj_home$Leave_time[mm],Point_List_adj_home$time_active_perday[mm],sep="_")
    }
  }
}
}
}
View(Users)
Users$ID[k] <- k
k
maxID2 <- 200L
Users<-data.frame(matrix(0,ncol=3,nrow=maxID2))
names(Users)<-c('ID','home','company')
for(k in 1:10)
{
  testdata<-read.table(paste(k,"user.csv",sep = " "),header=TRUE,sep=",")
  if(length(testdata$ID2)[1]<=100)
  {
  }
  else
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
      }
      else
      {
        testdata$Is_First_St[l] <- 0
        testdata$Is_Last_En[l-1] <- 0
      }
      if(testdata$Dura2[l]>2/24)
      {
        if(testdata$Is_First_St[l]==1)
        {
          testdata$Is_First_long_trip[l] <- 1
        }
        else{
          for(p in 1:10000)
          {
            if(testdata$Is_Last_En[l-p]!=1)
            {
              if(testdata$Is_First_long_trip[l-p]!=1)
              {
                if(testdata$Is_First_St[l-p]!=1)
                {
                }
                else{
                  testdata$Is_First_long_trip[l] <- 1
                  break
                }
              }
              else
              {
                testdata$Is_First_long_trip[l] <- 0
                break
              }
            }
            else
            {
              testdata$Is_First_long_trip[l] <- 0
              break
            }
          }
        }
      }
      else
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
      # for(m in 11:11)
    {
      a <- as.numeric(Point_List_adj$Var1[m])
      Pointdata_En <- subset(testdata,subset = (testdata$Sort.En == a))
      Pointdata_St <- subset(testdata,subset = (testdata$Sort.St == a))
      # Point_List_adj$total_time[m] <-sum(Pointdata_En$Dura2)
      # Point_List_adj$Active_Day[m] <-length(table(floor(Pointdata_En$End_adj)))
      Point_List_adj$time_active_perday[m] <- sum(Pointdata_En$Dura2)/length(table(floor(Pointdata_En$End_adj)))
      b <- which.max(table(Pointdata_En$End_Floor))
      c <- as.data.frame(table(Pointdata_En$End_Floor))
      e <- c$Freq[b]
      d <- subset(c,subset=(c$Freq==e))
      Point_List_adj$Aarive_time[m] <- as.numeric(as.vector(c[which.max(table(last(d)[1])),"Var1"]))
      bb <- which.max(table(Pointdata_St$Start_Floor))
      cc <- as.data.frame(table(Pointdata_St$Start_Floor))
      ee <- cc$Freq[bb]
      dd <- subset(cc,subset=(cc$Freq==ee))
      if(length(as.numeric(as.vector(unlist(cc[which.max(table(last(dd)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Leave_time[m] <- 0
      }
      else{
        Point_List_adj$Leave_time[m] <-  as.numeric(as.vector(unlist(cc[which.max(table(first(dd)[1])),"Var1"])))
      }
      Pointdata_En_Workday <- subset(Pointdata_En,subset = (Pointdata_En$WEEKDAY != 6 &Pointdata_En$WEEKDAY != 7 ))
      bbb <- which.max(table(Pointdata_En_Workday$End_Floor))
      ccc <- as.data.frame(table(Pointdata_En_Workday$End_Floor))
      eee <- c$Freq[bbb]
      ddd <- subset(ccc,subset=(ccc$Freq==eee))
      if(length(as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Aarive_time_workday[m] <- 0
      }
      else
      {
        Point_List_adj$Aarive_time_workday[m] <- as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"])))
      }
      if(sum(Pointdata_En_Workday$ID2)==0)
      {
        Point_List_adj$times_perWorkday[m]<- 0
      }
      else{
        Point_List_adj$times_perWorkday[m] <- length(Pointdata_En_Workday$ID2)/(last(floor(Pointdata_En_Workday$End_adj))-first(floor(Pointdata_En_Workday$Start_adj))+1)
      }
      if(sum(Pointdata_En_Workday$Dura2)==0)
      {
        Point_List_adj$time_perWorkday[m] <- 0
      }
      else
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
      # 杩欓噷涓哄垽鏂鍜屽叕鍙哥殑鏉′欢
      if(Point_List_adj$check1[m] +Point_List_adj$check2[m] +Point_List_adj$check3[m] +Point_List_adj$check4[m] == 4)
      {
        Point_List_adj$check9[m] <- 1
      }
      else
      {
        Point_List_adj$check9[m] <- 0
      }
      if(Point_List_adj$check5[m] +Point_List_adj$check6[m] +Point_List_adj$check7[m] +Point_List_adj$check8[m] == 4)
      {
        Point_List_adj$check10[m] <- 1
      }
      else
      {
        Point_List_adj$check10[m] <- 0
      }
    }
    Point_List_adj_home <- subset(Point_List_adj,subset = (Point_List_adj$check9[m]==1))
    if(length(Point_List_adj_home$Var1)==0)
    {
      Users$home[k]<- 0
    }
    else{
      if(length(Point_List_adj_home$Var1)==1)
      {
        Users$home[k] <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
      }
      else
      {
        str_home <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
        for(mm in 2:length(Point_List_adj_home$check9))
        {
          Users$home[k] <- paste(str_home,Point_List_adj_home$Var1[mm],Point_List_adj_home$Freq[mm],Point_List_adj_home$Aarive_time[mm],Point_List_adj_home$Leave_time[mm],Point_List_adj_home$time_active_perday[mm],sep="_")
        }
      }
    }
    Point_List_adj_company <- subset(Point_List_adj,subset = (Point_List_adj$check9[m]==1))
    if(length(Point_List_adj_company$Var1)==0)
    {
      Users$company[k]<- 0
    }
    else{
      if(length(Point_List_adj_company$Var1)==1)
      {
        Users$company[k] <- paste(Point_List_adj_company$Var1[1],Point_List_adj_company$Freq[1],Point_List_adj_company$Aarive_time[1],Point_List_adj_company$Leave_time[1],Point_List_adj_company$time_active_perday[1],sep="_")
      }
      else
      {
        str_company <- paste(Point_List_adj_company$Var1[1],Point_List_adj_company$Freq[1],Point_List_adj_company$Aarive_time[1],Point_List_adj_company$Leave_time[1],Point_List_adj_company$time_active_perday[1],sep="_")
        for(mm in 2:length(Point_List_adj_company$check9))
        {
          Users$company[k] <- paste(str_company,Point_List_adj_company$Var1[mm],Point_List_adj_company$Freq[mm],Point_List_adj_company$Aarive_time[mm],Point_List_adj_company$Leave_time[mm],Point_List_adj_company$time_active_perday[mm],sep="_")
        }
      }
    }
    Users$ID[k] <- k
  }
}
View(Users)
maxID2 <- 200L
Users<-data.frame(matrix(0,ncol=3,nrow=maxID2))
names(Users)<-c('ID','home','company')
for(k in 1:200)
{
  testdata<-read.table(paste(k,"user.csv",sep = " "),header=TRUE,sep=",")
  if(length(testdata$ID2)[1]<=100)
  {
  }
  else
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
      }
      else
      {
        testdata$Is_First_St[l] <- 0
        testdata$Is_Last_En[l-1] <- 0
      }
      if(testdata$Dura2[l]>2/24)
      {
        if(testdata$Is_First_St[l]==1)
        {
          testdata$Is_First_long_trip[l] <- 1
        }
        else{
          for(p in 1:10000)
          {
            if(testdata$Is_Last_En[l-p]!=1)
            {
              if(testdata$Is_First_long_trip[l-p]!=1)
              {
                if(testdata$Is_First_St[l-p]!=1)
                {
                }
                else{
                  testdata$Is_First_long_trip[l] <- 1
                  break
                }
              }
              else
              {
                testdata$Is_First_long_trip[l] <- 0
                break
              }
            }
            else
            {
              testdata$Is_First_long_trip[l] <- 0
              break
            }
          }
        }
      }
      else
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
      # for(m in 11:11)
    {
      a <- as.numeric(Point_List_adj$Var1[m])
      Pointdata_En <- subset(testdata,subset = (testdata$Sort.En == a))
      Pointdata_St <- subset(testdata,subset = (testdata$Sort.St == a))
      # Point_List_adj$total_time[m] <-sum(Pointdata_En$Dura2)
      # Point_List_adj$Active_Day[m] <-length(table(floor(Pointdata_En$End_adj)))
      Point_List_adj$time_active_perday[m] <- sum(Pointdata_En$Dura2)/length(table(floor(Pointdata_En$End_adj)))
      b <- which.max(table(Pointdata_En$End_Floor))
      c <- as.data.frame(table(Pointdata_En$End_Floor))
      e <- c$Freq[b]
      d <- subset(c,subset=(c$Freq==e))
      Point_List_adj$Aarive_time[m] <- as.numeric(as.vector(c[which.max(table(last(d)[1])),"Var1"]))
      bb <- which.max(table(Pointdata_St$Start_Floor))
      cc <- as.data.frame(table(Pointdata_St$Start_Floor))
      ee <- cc$Freq[bb]
      dd <- subset(cc,subset=(cc$Freq==ee))
      if(length(as.numeric(as.vector(unlist(cc[which.max(table(last(dd)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Leave_time[m] <- 0
      }
      else{
        Point_List_adj$Leave_time[m] <-  as.numeric(as.vector(unlist(cc[which.max(table(first(dd)[1])),"Var1"])))
      }
      Pointdata_En_Workday <- subset(Pointdata_En,subset = (Pointdata_En$WEEKDAY != 6 &Pointdata_En$WEEKDAY != 7 ))
      bbb <- which.max(table(Pointdata_En_Workday$End_Floor))
      ccc <- as.data.frame(table(Pointdata_En_Workday$End_Floor))
      eee <- c$Freq[bbb]
      ddd <- subset(ccc,subset=(ccc$Freq==eee))
      if(length(as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"]))))==0)
      {
        Point_List_adj$Aarive_time_workday[m] <- 0
      }
      else
      {
        Point_List_adj$Aarive_time_workday[m] <- as.numeric(as.vector(unlist(ccc[which.max(table(last(ddd)[1])),"Var1"])))
      }
      if(sum(Pointdata_En_Workday$ID2)==0)
      {
        Point_List_adj$times_perWorkday[m]<- 0
      }
      else{
        Point_List_adj$times_perWorkday[m] <- length(Pointdata_En_Workday$ID2)/(last(floor(Pointdata_En_Workday$End_adj))-first(floor(Pointdata_En_Workday$Start_adj))+1)
      }
      if(sum(Pointdata_En_Workday$Dura2)==0)
      {
        Point_List_adj$time_perWorkday[m] <- 0
      }
      else
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
      # 杩欓噷涓哄垽鏂鍜屽叕鍙哥殑鏉′欢
      if(Point_List_adj$check1[m] +Point_List_adj$check2[m] +Point_List_adj$check3[m] +Point_List_adj$check4[m] == 4)
      {
        Point_List_adj$check9[m] <- 1
      }
      else
      {
        Point_List_adj$check9[m] <- 0
      }
      if(Point_List_adj$check5[m] +Point_List_adj$check6[m] +Point_List_adj$check7[m] +Point_List_adj$check8[m] == 4)
      {
        Point_List_adj$check10[m] <- 1
      }
      else
      {
        Point_List_adj$check10[m] <- 0
      }
    }
    Point_List_adj_home <- subset(Point_List_adj,subset = (Point_List_adj$check9[m]==1))
    if(length(Point_List_adj_home$Var1)==0)
    {
      Users$home[k]<- 0
    }
    else{
      if(length(Point_List_adj_home$Var1)==1)
      {
        Users$home[k] <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
      }
      else
      {
        str_home <- paste(Point_List_adj_home$Var1[1],Point_List_adj_home$Freq[1],Point_List_adj_home$Aarive_time[1],Point_List_adj_home$Leave_time[1],Point_List_adj_home$time_active_perday[1],sep="_")
        for(mm in 2:length(Point_List_adj_home$check9))
        {
          Users$home[k] <- paste(str_home,Point_List_adj_home$Var1[mm],Point_List_adj_home$Freq[mm],Point_List_adj_home$Aarive_time[mm],Point_List_adj_home$Leave_time[mm],Point_List_adj_home$time_active_perday[mm],sep="_")
        }
      }
    }
    Point_List_adj_company <- subset(Point_List_adj,subset = (Point_List_adj$check9[m]==1))
    if(length(Point_List_adj_company$Var1)==0)
    {
      Users$company[k]<- 0
    }
    else{
      if(length(Point_List_adj_company$Var1)==1)
      {
        Users$company[k] <- paste(Point_List_adj_company$Var1[1],Point_List_adj_company$Freq[1],Point_List_adj_company$Aarive_time[1],Point_List_adj_company$Leave_time[1],Point_List_adj_company$time_active_perday[1],sep="_")
      }
      else
      {
        str_company <- paste(Point_List_adj_company$Var1[1],Point_List_adj_company$Freq[1],Point_List_adj_company$Aarive_time[1],Point_List_adj_company$Leave_time[1],Point_List_adj_company$time_active_perday[1],sep="_")
        for(mm in 2:length(Point_List_adj_company$check9))
        {
          Users$company[k] <- paste(str_company,Point_List_adj_company$Var1[mm],Point_List_adj_company$Freq[mm],Point_List_adj_company$Aarive_time[mm],Point_List_adj_company$Leave_time[mm],Point_List_adj_company$time_active_perday[mm],sep="_")
        }
      }
    }
    Users$ID[k] <- k
  }
}
Point_List_adj$Aarive_time[m]
Point_List_adj$Aarive_time
