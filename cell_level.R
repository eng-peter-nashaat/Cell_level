



from <- '2017-10-23'
to <- '2017-10-24'
hourly <- "FALSE"
cluster <- "NEW CAIRO"
path <- "C:/Users/Red Telecom/Desktop/power BI/power BI reports/Cell_level/"

extractData <- function(from,to,cluster,hourly,path){



from.date <- from
to.date <- to

DateTime_series <- seq(as.Date(from.date),as.Date(to.date), by=1)

#from <- paste("(","'",from,"')")
#to <- paste("(","'",to,"')")


getData<- function(from,to,cluster,hourly,path){

  grep_sites_from_cluster <- function(Cluster_name){
    Cluster_name <- as.character(Cluster_name)
    path_full <- paste(path,"Sites_Clusters_mapping.csv", sep = "")
    clusters <- read.csv(path_full,strip.white=TRUE)
    path_full <- paste(path,"Sites_cells_mapping.csv", sep = "")
    
    Sites_Cells_Mapping <- read.csv(path_full)
    
    colnames(Sites_Cells_Mapping) <- c("ERBS", "EutrancellFDD")
    
    clusters$AREA <- as.character(clusters$AREA)
    
    t <- as.vector(grep(Cluster_name,clusters$AREA,ignore.case=TRUE))
    
    cluster.sites <- clusters[t,]
    
    
    return(as.character(unique(cluster.sites$SITE.ID)))
  }
  
  if(cluster =="ALL"){
    
    l <- ""
    erbs <- paste("")
    
  }else if (length(cluster) ==1){
    sites <- grep_sites_from_cluster(cluster)
    l <- paste(sites, collapse = "','")
    erbs <- paste("ERBS IN ('",l,"') and",sep = "")
    browser()
    
    
    
  }else{
    l <- paste(as.character(cluster), collapse = "','")
    erbs <- paste("ERBS IN ('",l,"') and",sep = "")
    browser()
    
    }
  
  if (hourly == "TRUE"){
    
    hourly <- "DateTime_ID,"
    
  }else{
    hourly <- ""
  }
  
    

query2 <- paste("select
                Date_ID,", hourly,"
                ERBS,
                
                
                
                pmMacHarqDlNack16qam=sum(pmMacHarqDlNack16qam),
                pmMacHarqDlNack64qam=sum(pmMacHarqDlNack64qam),
                pmMacHarqDlNackQpsk=sum(pmMacHarqDlNackQpsk),
                pmRadioTbsPwrRestricted=sum(pmRadioTbsPwrRestricted),
                pmRadioTbsPwrUNRestricted=sum(pmRadioTbsPwrUNRestricted),
                pmRlcArqDlAck=sum(pmRlcArqDlAck),
                pmRlcArqDlNack=sum(pmRlcArqDlNack),
                pmMacHarqUlSucc16qam=sum(pmMacHarqUlSucc16qam),
                pmMacHarqUlSuccQpsk=sum(pmMacHarqUlSuccQpsk),
                pmMacHarqUlFailQpsk=sum(pmMacHarqUlFailQpsk),
                pmRadioThpVolDl=sum(pmRadioThpVolDl),
                pmRadioThpResDl=sum(pmRadioThpResDl),
                pmRadioThpVolUl=sum(pmRadioThpVolUl),
                pmRadioThpResUl=sum(pmRadioThpResUl),
                pmRlcDelayTimeDl=sum(pmRlcDelayTimeDl),
                pmRlcDelayPktTransDl=sum(pmRlcDelayPktTransDl),
                pmMacDelayTimeDl=sum(pmMacDelayTimeDl),
                pmMacHarqDlAckQpsk=sum(pmMacHarqDlAckQpsk),
                pmMacHarqDlAck16qam=sum(pmMacHarqDlAck16qam),
                pmMacHarqDlAck64qam=sum(pmMacHarqDlAck64qam),
                pmHoPrepRejInLicConnUsers=sum(pmHoPrepRejInLicConnUsers),
                pmMacHarqUlFail16qam=sum(pmMacHarqUlFail16qam),
                pmRlcArqUlAck=sum(pmRlcArqUlAck),
                pmRlcArqUlNack=sum(pmRlcArqUlNack),
                pmRrcConnMax=sum(pmRrcConnMax),
                pmRrcConnEstabSucc=sum(pmRrcConnEstabSucc),
                pmRrcConnEstabAtt=sum(pmRrcConnEstabAtt),
                pmRrcConnEstabAttReatt=sum(pmRrcConnEstabAttReatt),
                pmS1SigConnEstabSucc=sum(pmS1SigConnEstabSucc),
                pmS1SigConnEstabAtt=sum(pmS1SigConnEstabAtt),
                pmErabEstabSuccInit=sum(pmErabEstabSuccInit),
                pmErabEstabAttInit=sum(pmErabEstabAttInit),
                pmErabEstabSuccAdded=sum(pmErabEstabSuccAdded),
                pmErabEstabAttAdded=sum(pmErabEstabAttAdded),
                pmErabEstabFailAddedLic=sum(pmErabEstabFailAddedLic),
                pmRrcConnEstabFailLic=sum(pmRrcConnEstabFailLic),
                pmErabEstabFailInitLic=sum(pmErabEstabFailInitLic),
                pmUeCtxtEstabSucc=sum( pmUeCtxtEstabSucc),
                pmSchedActivityUeDl=sum(pmSchedActivityUeDl),
                
                pmUeCtxtEstabAtt=sum(pmUeCtxtEstabAtt),
                pmPagDiscarded=sum(pmPagDiscarded),
                pmPagReceived=sum(pmPagReceived),
                pmRaSuccCbra=sum(pmRaSuccCbra),
                pmRaAttCbra=sum(pmRaAttCbra),
                pmRaFailCbraMsg2Disc=sum(pmRaFailCbraMsg2Disc),
                pmErabRelAbnormalEnbAct=sum(pmErabRelAbnormalEnbAct),
                pmErabRelAbnormalMmeAct=sum(pmErabRelAbnormalMmeAct),
                pmErabRelMme=sum(pmErabRelMme),
                pmErabRelNormalEnb=sum(pmErabRelNormalEnb),
                pmErabRelAbnormalEnb=sum(pmErabRelAbnormalEnb),
                pmUeThpTimeDl=sum(pmUeThpTimeDl),
                pmUeThpVolUl=sum(pmUeThpVolUl),
                pmUeThpTimeUl=sum(pmUeThpTimeUl),
                pmActiveUeDlSum=sum(pmActiveUeDlSum),
                pmActiveDrbDlSum=sum(pmActiveDrbDlSum),
                pmSchedActivityCellDl=sum(pmSchedActivityCellDl),
                pmActiveUeUlSum=sum(pmActiveUeUlSum),
                pmActiveDrbUlSum=sum(pmActiveDrbUlSum),
                pmSchedActivityCellUl=sum(pmSchedActivityCellUl),
                pmSchedActivityUeUl=sum(pmSchedActivityUeUl),
                pmRrcConnLevSum=sum(pmRrcConnLevSum),
                pmRrcConnLevSamp=sum(pmRrcConnLevSamp),
                pmPdcpVolDlDrb=sum(pmPdcpVolDlDrb),
                pmPdcpVolDlDrbLastTti=sum(pmPdcpVolDlDrbLastTti),
                pmPdcpLatPktTransDl=sum(pmPdcpLatPktTransDl),
                pmPdcpLatTimeDl=sum(pmPdcpLatTimeDl),
                pmAnrNeighbrelAdd=sum(pmAnrNeighbrelAdd),
                pmAnrNeighbrelRem=sum(pmAnrNeighbrelRem),
                pmUeCtxtRelSCCdma=sum(pmUeCtxtRelSCCdma),
                pmUeCtxtRelSCEUtra=sum(pmUeCtxtRelSCEUtra),
                pmUeCtxtRelSCWcdma=sum(pmUeCtxtRelSCWcdma),
                pmBestCellEvalReport=sum(pmBestCellEvalReport),
                pmErabLevSamp=sum(pmErabLevSamp),
                pmErabLevSum=sum(pmErabLevSum),
                pmSessionTimeUe=sum(pmSessionTimeUe),
                pmSessionTimeDrb=sum(pmSessionTimeDrb),
                pmHoPrepRejInLicMob=sum(pmHoPrepRejInLicMob),
                pmHoPrepRejInLicMultiErab=sum(pmHoPrepRejInLicMultiErab),
                pmBadCovEvalReport=sum(pmBadCovEvalReport),
                pmPdcpVolDlSrb=sum(pmPdcpVolDlSrb),
                pmPdcpVolUlDrb=sum(pmPdcpVolUlDrb),
                pmPdcpVolUlSrb=sum(pmPdcpVolUlSrb),
                pmCellDowntimeAuto=sum(pmCellDowntimeAuto),
                pmCellDowntimeMan=sum(pmCellDowntimeMan),
                pmAnrHoSuccLevelLow=sum(pmAnrHoSuccLevelLow),
                pmAnrHoSuccLevelMedium=sum(pmAnrHoSuccLevelMedium),
                pmHoPreventedProblematicCell=sum(pmHoPreventedProblematicCell),
                pmMacHarqDlDtxQpsk=sum(pmMacHarqDlDtxQpsk),
                pmMacHarqDlDtx16qam=sum(pmMacHarqDlDtx16qam),
                pmMacHarqDlDtx64qam=sum(pmMacHarqDlDtx64qam),
                pmMacHarqUlDtxQpsk=sum(pmMacHarqUlDtxQpsk),
                pmMacHarqUlDtx16qam=sum(pmMacHarqUlDtx16qam),
                pmB2BestCellEvalReport=sum(pmB2BestCellEvalReport),
                pmBadCovSearchEvalReport=sum(pmBadCovSearchEvalReport),
                pmGoodCovSearchEvalReport=sum(pmGoodCovSearchEvalReport),
                pmCriticalBorderEvalReport=sum(pmCriticalBorderEvalReport),
                pmA5BestCellEvalReport=sum(pmA5BestCellEvalReport),
                pmPrbUsedDlBcch=sum(pmPrbUsedDlBcch),
                pmPrbUsedDlDtch=sum(pmPrbUsedDlDtch),
                pmPrbUsedDlPcch=sum(pmPrbUsedDlPcch),
                pmPrbUsedDlSrbFirstTrans=sum(pmPrbUsedDlSrbFirstTrans),
                pmPrbAvailDl=sum(pmPrbAvailDl),
                pmPrbUsedUlDtch=sum(pmPrbUsedUlDtch),
                pmPrbUsedUlSrb=sum(pmPrbUsedUlSrb),
                pmRaSuccCfra=sum(pmRaSuccCfra),
                pmRaAttCfra=sum(pmRaAttCfra),
                pmRaFailCfraMsg2Disc=sum(pmRaFailCfraMsg2Disc),
                pmRaUnassignedCfraFalse=sum(pmRaUnassignedCfraFalse),
                pmActiveUeUlMax=sum(pmActiveUeUlMax),
                pmActiveUeDlMax=sum(pmActiveUeDlMax)
                
                
                
                from dc.DC_E_ERBSG2_EUTRANCELLFDD_RAW where ", erbs , "                
                Date_ID between ",  from , " and " , to ," Group By ERBS, ", hourly, "Date_ID")


query3 <- paste("Select
                Date_ID,", hourly,"
                ERBS,
                
                pmHoPrepSuccLteIntraF=sum(pmHoPrepSuccLteIntraF),
                pmHoPrepSuccLteInterF=sum(pmHoPrepSuccLteInterF),
                pmHoPrepAttLteIntraF=sum(pmHoPrepAttLteIntraF),
                pmHoPrepAttLteInterF=sum(pmHoPrepAttLteInterF),
                pmHoExeAttLteInterF=sum(pmHoExeAttLteInterF),
                pmHoExeAttLteIntraF=sum(pmHoExeAttLteIntraF),
                pmHoExeSuccLteInterF=sum(pmHoExeSuccLteInterF),
                pmHoExeSuccLteIntraF=sum(pmHoExeSuccLteIntraF)
                
                
                
                
                from dc.DC_E_ERBSG2_EUTRANCELLRELATION_RAW where ", erbs , "
                
                
                Date_ID between ",  from , " and " , to ," Group By ERBS, ", hourly, "Date_ID")



query4 <- paste("Select
                Date_ID,", hourly,"
                ERBS,
                
                
                
                pmHoPrepSucc=sum(pmHoPrepSucc),
                pmHoPrepAtt=sum(pmHoPrepAtt),
                pmHoExeSucc=sum(pmHoExeSucc),
                pmHoExeAtt=sum(pmHoExeAtt)
                
                
                
                
                
                from dc.DC_E_ERBSG2_UTRANCELLRELATION_RAW where ", erbs , "
                
                Date_ID between ",  from , " and " , to ," Group By ERBS, ", hourly, "Date_ID")

query1 <- paste("Select
                Date_ID,", hourly,"
                ERBS,
                
                
                pmPdcpPktLostUl=sum(pmPdcpPktLostUl),
                pmPdcpPktReceivedUl=sum(pmPdcpPktReceivedUl) ,
                pmPdcpPktDiscDlPelr=sum(pmPdcpPktDiscDlPelr),
                pmPdcpPktDiscDlHo=sum(pmPdcpPktDiscDlHo),
                pmPdcpPktDiscDlPelrUu=sum(pmPdcpPktDiscDlPelrUu),
                pmPdcpPktTransDl=sum(pmPdcpPktTransDl)
                
                
                
                
                
                from dc.DC_E_ERBS_EUTRANCELLFDD_RAW where ", erbs , "
                
                Date_ID between ",  from , " and " , to," Group By ERBS, ", hourly, "Date_ID")


query <- c(query1,query2,query3,query4)

library(parallel)

# Calculate the number of cores
no_cores <- detectCores() - 2

# Initiate cluster
cl <- makeCluster(no_cores)

clusterEvalQ(cl, library("RJDBC"))


clusterExport(cl,"path")


rs <- parLapply(cl,query,function(query){
path_full <- paste(path,"jconn4.jar",sep = "")
drv <-JDBC("com.sybase.jdbc4.jdbc.SybDriver",
           path_full)

conn <- dbConnect(drv, "jdbc:sybase:Tds:10.74.231.82:2640",
                  "Reporter",
                  "EtiENIQ")  
dataset <- dbGetQuery(conn,query)
dbDisconnect(conn)
rm(conn)
return(dataset)
} )


stopCluster(cl)


gc()
h <- NULL

if (hourly == ""){
  h <- c("Date_ID")
  
}else{
  
  h <- c("DateTime_ID")
}

dataset<- merge(rs[[1]], rs[[2]], all.x = TRUE, all.y = TRUE, by.x = c(h,"ERBS"),by.y = c(h,"ERBS"))
dataset <- merge(dataset, rs[[3]], all.x = TRUE, all.y = TRUE, by.x = c(h,"ERBS"),by.y = c(h,"ERBS"))
dataset <- merge(dataset, rs[[4]], all.x = TRUE, all.y = TRUE, by.x = c(h,"ERBS"),by.y = c(h,"ERBS"))

browser()
if (hourly == ""){
  h <- c("Date_ID")
  dataset$DateTime_ID <- dataset$Date_ID
  
}else{
  
  h <- c("DateTime_ID")
  
  t <- dataset$DateTime_ID
  
  l <- t[!is.na(strptime(t, format = c("%Y-%m-%d %H:0-%M:%S")))] 
  t[!is.na(strptime(t, format = c("%Y-%m-%d %H:0-%M:%S")))] <- as.character(as.POSIXlt(l,format = c("%Y-%m-%d %H:0-%M:%S")))
  
  
  s <- t[!is.na(strptime(t, format = c("%Y-%m-%d %H:%M:%S")))] 
  t[!is.na(strptime(t, format = c("%Y-%m-%d %H:%M:%S")))] <- as.character(as.POSIXlt(s,format = c("%Y-%m-%d %H:%M:%S")))
  
  dataset$DateTime_ID <- as.POSIXct(t)
}





KPIs_networkLevel<- dataset[,c("DateTime_ID","ERBS","pmRrcConnMax")]

KPIs_networkLevel$Average.DL.PDCP.Cell.Throughput =dataset$pmPdcpVolDlDrb / dataset$pmSchedActivityCellDl 
KPIs_networkLevel$Average.DL.PDCP.UE.Throughput = (dataset$pmPdcpVolDlDrb - dataset$pmPdcpVolDlDrbLastTti) / dataset$pmUeThpTimeDl 

DL_used_Prb=(dataset$pmPrbUsedDlBcch+dataset$pmPrbUsedDlDtch+dataset$pmPrbUsedDlPcch+dataset$pmPrbUsedDlSrbFirstTrans)/(900000)
DL_Avail_PRB=dataset$pmPrbAvailDl/(900000)
KPIs_networkLevel$DL_PRB_Utilization=100*(DL_used_Prb/DL_Avail_PRB)

KPIs_networkLevel$UL_used_Prb =100*(dataset$pmPrbUsedUlDtch+dataset$pmPrbUsedUlSrb)/(900000)
KPIs_networkLevel$Avg.UL_throughput_per_RB= dataset$pmPdcpVolUlDrb/dataset$pmPrbUsedUlDtch



KPIs_networkLevel$Int_ActiveUEsDl=dataset$pmActiveUeDlSum / dataset$pmSchedActivityCellDl

KPIs_networkLevel$Average.UL.PDCP.Cell.Throughput <- dataset$pmPdcpVolUlDrb / dataset$pmSchedActivityCellUl 

KPIs_networkLevel$DL.Volume =(dataset$pmPdcpVolDlDrb+dataset$pmPdcpVolDlSrb)/(8.192*1024*1024)
KPIs_networkLevel$UL.Volume =(dataset$pmPdcpVolUlDrb+dataset$pmPdcpVolUlSrb)/(8.192*1024*1024) 

KPIs_networkLevel$Average.UL.PDCP.UE.Throughput <- (dataset$pmUeThpVolUl ) / dataset$pmUeThpTimeUl 
KPIs_networkLevel$Int_ActiveUEsUl=dataset$pmActiveUeUlSum / dataset$pmSchedActivityCellUl
KPIs_networkLevel$Res_AvgNrOfRrcConnectedUsers = dataset$pmRrcConnLevSum / dataset$pmRrcConnLevSamp

#KPIs_networkLevel$pmRrcConnMax=dataset$pmRrcConnMax

KPIs_networkLevel$pmCellDowntimeMan <- dataset$pmCellDowntimeMan/(60*60)
KPIs_networkLevel$pmCellDowntimeAuto <- dataset$pmCellDowntimeAuto/(60*60)


KPIs_networkLevel$pmErabRelAbnormalEnbAct <- dataset$pmErabRelAbnormalEnbAct
KPIs_networkLevel$pmErabRelAbnormalMmeAct <- dataset$pmErabRelAbnormalMmeAct
KPIs_networkLevel$pmErabRelAbnormalEnb <- dataset$pmErabRelAbnormalEnb


KPIs_networkLevel$pmHoPrepSuccLteInterF <- dataset$pmHoPrepSuccLteInterF 
KPIs_networkLevel$pmHoPrepAttLteInterF <- dataset$pmHoPrepAttLteInterF
KPIs_networkLevel$pmHoExeSuccLteInterF <- dataset$pmHoExeSuccLteInterF 
KPIs_networkLevel$pmHoExeAttLteInterF <- dataset$pmHoExeAttLteInterF

KPIs_networkLevel$pmHoExeSuccLteIntraF <- dataset$pmHoExeSuccLteIntraF
KPIs_networkLevel$pmHoExeAttLteIntraF <- dataset$pmHoExeAttLteIntraF 


KPIs_networkLevel$pmRrcConnEstabAtt <- dataset$pmRrcConnEstabAtt
KPIs_networkLevel$pmS1SigConnEstabAtt <- dataset$pmS1SigConnEstabAtt
KPIs_networkLevel$pmErabEstabAttInit <- dataset$pmErabEstabAttInit
KPIs_networkLevel$Acc_InitialErabSetupSuccRate <- 100 * dataset$pmErabEstabSuccInit/ dataset$pmErabEstabAttInit
KPIs_networkLevel$Acc_S1SigEstabSuccRate <- 100 * dataset$pmS1SigConnEstabSucc/ dataset$pmS1SigConnEstabAtt
KPIs_networkLevel$Acc_RrcConnSetupSuccRate <- 100 * dataset$pmRrcConnEstabSucc / (dataset$pmRrcConnEstabAtt - dataset$pmRrcConnEstabAttReatt)

KPIs_networkLevel$pmRaAttCbra=dataset$pmRaAttCbra #line 
KPIs_networkLevel$Acc_RandomAccessDecodingRate=100 * dataset$pmRaSuccCbra / dataset$pmRaAttCbra #bar

KPIs_networkLevel$pmPagReceived=dataset$pmPagReceived # line

KPIs_networkLevel$Acc_pagingDiscardRate = 100 * dataset$pmPagDiscarded / dataset$pmPagReceived
KPIs_networkLevel$Ret_ERabRetainabilityRate =100 * (dataset$pmErabRelAbnormalEnbAct + dataset$pmErabRelAbnormalMmeAct) / (dataset$pmErabRelMme + dataset$pmErabRelNormalEnb + dataset$pmErabRelAbnormalEnb)
KPIs_networkLevel$Int_DlLatency=dataset$pmPdcpLatTimeDl / dataset$pmPdcpLatPktTransDl 

KPIs_networkLevel$Mob_Intra_HoExecSuccRate = 100 * dataset$pmHoExeSuccLteIntraF / dataset$pmHoExeAttLteIntraF
KPIs_networkLevel$Mob_Intra_HoPrepSuccRate = 100 * dataset$pmHoPrepSuccLteIntraF / dataset$pmHoPrepAttLteIntraF
KPIs_networkLevel$Mob_Intra_SR = KPIs_networkLevel$Mob_Intra_HoExecSuccRate*KPIs_networkLevel$Mob_Intra_HoPrepSuccRate/100


KPIs_networkLevel$pmHoPrepAttLteIntraF <- dataset$pmHoPrepAttLteIntraF

KPIs_networkLevel$Mob_Inter_HoPrepSuccRate =  100 * dataset$pmHoPrepSuccLteInterF / dataset$pmHoPrepAttLteInterF
KPIs_networkLevel$Mob_Inter_HoExecSuccRate  = 100 * dataset$pmHoExeSuccLteInterF / dataset$pmHoExeAttLteInterF
KPIs_networkLevel$Mob_Inter_SR= KPIs_networkLevel$Mob_Inter_HoExecSuccRate*KPIs_networkLevel$Mob_Inter_HoPrepSuccRate/100

KPIs_networkLevel$DL_16QAM_Utilization = 100*dataset$pmMacHarqDlAck16qam/(dataset$pmMacHarqDlAck16qam+dataset$pmMacHarqDlAck64qam+dataset$pmMacHarqDlAckQpsk)
KPIs_networkLevel$DL_64QAM_Utilization_percent = 100*dataset$pmMacHarqDlAck64qam/(dataset$pmMacHarqDlAck64qam+dataset$pmMacHarqDlAck16qam+dataset$pmMacHarqDlAckQpsk)
KPIs_networkLevel$DL_QPSK_Distribution_percent = 100*dataset$pmMacHarqDlAckQpsk/(dataset$pmMacHarqDlAckQpsk+dataset$pmMacHarqDlAck64qam+dataset$pmMacHarqDlAck16qam)


KPIs_networkLevel$UL_16QAM_Utilization_percent = 100*dataset$pmMacHarqUlSucc16qam/(dataset$pmMacHarqUlSuccQpsk+dataset$pmMacHarqUlSucc16qam)
KPIs_networkLevel$UL_QPSK_Distribution_percent =100*dataset$pmMacHarqUlSuccQpsk/(dataset$pmMacHarqUlSuccQpsk+dataset$pmMacHarqUlSucc16qam)

KPIs_networkLevel$DL_packet_loss=100*((dataset$pmPdcpPktDiscDlPelr+dataset$pmPdcpPktDiscDlPelrUu+dataset$pmPdcpPktDiscDlHo)/(dataset$pmPdcpPktDiscDlHo+dataset$pmPdcpPktDiscDlPelr+dataset$pmPdcpPktDiscDlPelrUu+dataset$pmPdcpPktTransDl))
KPIs_networkLevel$UL_packet_loss=100*(dataset$pmPdcpPktLostUl/(dataset$pmPdcpPktLostUl+dataset$pmPdcpPktReceivedUl))

KPIs_networkLevel$Int_pmRadioTbsPwrUNRestricted =100*dataset$pmRadioTbsPwrUNRestricted/(dataset$pmRadioTbsPwrRestricted+dataset$pmRadioTbsPwrUNRestricted)
KPIs_networkLevel$Int_pmRadioTbsPwrRestricted=100*dataset$pmRadioTbsPwrRestricted/(dataset$pmRadioTbsPwrRestricted+dataset$pmRadioTbsPwrUNRestricted)


KPIs_networkLevel$DL_SE_per_TTI <- dataset$pmSchedActivityUeDl / dataset$pmSchedActivityCellDl
KPIs_networkLevel$ActiveUeUlMax <- dataset$pmActiveUeUlMax
KPIs_networkLevel$ActiveUeDlMax <- dataset$pmActiveUeDlMax

KPIs_networkLevel$Int_AverageDlMacDelay=dataset$pmMacDelayTimeDl / (dataset$pmMacHarqDlAckQpsk + dataset$pmMacHarqDlAck16qam + dataset$pmMacHarqDlAck64qam)
KPIs_networkLevel$Int_AverageDlRlcDelay=dataset$pmRlcDelayTimeDl / dataset$pmRlcDelayPktTransDl 

KPIs_networkLevel$Contention_Free_RA_Success_Rate=100*dataset$pmRaSuccCfra/dataset$pmRaAttCfra
KPIs_networkLevel$Contention_Free_RA_Failure_Rate_Timer=100*dataset$pmRaFailCfraMsg2Disc/dataset$pmRaAttCfra
KPIs_networkLevel$Contention_Free_RA_False_Preambles_Rate=100*dataset$pmRaUnassignedCfraFalse/(dataset$pmRaUnassignedCfraFalse+dataset$pmRaAttCfra)


KPIs_networkLevel$UL_SE_per_TTI  =dataset$pmSchedActivityUeUl / dataset$pmSchedActivityCellUl

rm(dataset)
KPIs_networkLevel [is.na(KPIs_networkLevel)] <- 0
return(KPIs_networkLevel)
}

KPIs_cell_level <- data.frame()

#KPIs_cell_level<- getData(from,to,cluster,hourly,path)


#cl<-makeCluster(2)
#registerDoParallel(cl)

#foreach(e = 1:length(DateTime_series), 
#        .combine = rbind)  %dopar% { 
#from <- as.character(DateTime_series[e])
#to <- as.character(DateTime_series[e])
#from <- paste("(","'",from,"')")
#to <- paste("(","'",to,"')")
#dat <- getData(from,to,cluster,hourly,path,conn)
#KPIs_cell_level <- rbind(KPIs_cell_level,dat)
#gc()
#}

#stopCluster(cl)

#hourly <- "FALSE"

if (hourly == "TRU"){
  
  for (e in 1:length(DateTime_series)){
    from <- as.character(DateTime_series[e])
    to <- as.character(DateTime_series[e])
    from <- paste("(","'",from,"')")
    to <- paste("(","'",to,"')")
    dat <- getData(from,to,cluster,hourly,path)
    KPIs_cell_level <- rbind(KPIs_cell_level,dat)
    gc()
  
    }
}else{
  from <- paste("(","'",from,"')")
  to <- paste("(","'",to,"')")
  KPIs_cell_level <- getData(from,to,cluster,hourly,path)
  
  }

write.csv(KPIs_cell_level,paste(path,"Cell_level.csv"), row.names = F)

return(KPIs_cell_level)  

}

#KPIs <- extractData(from,to,cluster,hourly,path)

