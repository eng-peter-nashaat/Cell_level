extractData <- function(from,to,cluster,hourly,path){
  
  
  
  #install.packages("RPostgreSQL")
  
  from.date <- from
  to.date <- to
  
  
  
  #DateTime_series <- seq(as.Date(from.date),as.Date(to.date), by=1)
  getdata <- function(from,to,cluster,hourly,path){
    require("RPostgreSQL")
    
    
    
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
      erbs <- paste("ERBS IN ('", l  ,"') and",sep = "")
    }else{
      l <- paste(erbs, collapse = "','")
      erbs <- as.character(erbs)
      erbs <- paste("ERBS IN ('", l  ,"') and",sep = "")
    }
    
    if (hourly == "TRUE"){
      
      hourly <- "hour_ID,"
      
    }else{
      hourly <- ""
    }
    
    
    
    
    pg = dbDriver("PostgreSQL")
    
    con = dbConnect(pg, user="ideftar", password="ideftar",
                    host="10.74.226.165", port=5432, dbname="rf")
    
    
    from <- paste("(","'",from,"')")
    to <- paste("(","'",to,"')")
    
    query1 <- paste("Select
                    Date_ID,
                    DCVECTOR_INDEX,
                    ", hourly,"
                    ERBS,
                    
                    
                    sum(pmRadioTxRankDistr) as pmRadioTxRankDistr,
                    sum(pmRadioUeRepRankDistr) as pmRadioUeRepRankDistr,
                    sum(pmRadioUeRepCqiDistr) as pmRadioUeRepCqiDistr,
                    sum(pmRadioUeRepCqiDistr2) as pmRadioUeRepCqiDistr2,
                    sum(pmPrbUtilUl) as pmPrbUtilUl,
                    sum(pmPrbUtilDl) as pmPrbUtilDl,
                    sum(pmSinrPucchDistr) as pmSinrPucchDistr,
                    sum(pmSinrPuschDistr) as pmSinrPuschDistr,
                    sum(pmPdcchCceUtil) as pmPdcchCceUtil,
                    sum(pmRadioRecInterferencePwrPUCCH) as pmRadioRecInterferencePwrPUCCH,
                    sum(pmRadioRecInterferencePwr) as pmRadioRecInterferencePwr,
                    sum(pmUeThpDlDistr) as pmUeThpDlDistr,
                    sum(pmUeThpUlDistr) as pmUeThpUlDistr,
                    sum(pmUlPathlossDistr) as pmUlPathlossDistr
                    
                    
                    
                    from eniq.dc_dc_e_erbsg2_eu_internal
                    
                    where ", erbs ,  "DCVECTOR_INDEX<=20 and 
                    
                    Date_ID between ",  from , " and " , to ," Group By ERBS, ", hourly,
                    " Date_ID,DCVECTOR_INDEX")
    
    dataset = dbGetQuery(con, query1)
    dbDisconnect(con)
    #browser()
    dataset[is.na(dataset)] <- 0
    
    
    if (hourly == ""){
      dataset$DateTime_ID <- as.Date(dataset$date_id)
      
    }else{
      dataset$DateTime_ID <- as.POSIXct(paste(dataset$date_id,dataset$hour_id), format="%Y-%m-%d %H:%M:%S")
    }
    
    #write.csv(dataset,paste(path,"DC_row_data.csv"), row.names = F)
    
    library(plyr)
    
    library(dplyr)
    
    
    #this script read data from DCvector_index_networklvl file and start calculating the CQI on network level 
    rs <- split(dataset, with(dataset, interaction(dataset$DateTime_ID,dataset$erbs)), drop = TRUE)
    
    KPIs_vectorized <- function(dat){
      
      
      CQI_weights <- 0:15
      CQI_weights <- append(CQI_weights,rep(0, 5))
      
      
      Interference_weights <- c(0.794328235,1, 1.258925412,1.584893192,1.995262315, 2.511886432,  3.16227766,  3.981071706, 5.011872336, 6.309573445, 10, 25.11886432,  63.09573445, 158.4893192,  398.1071706, 630.9573445)
      Interference_weights <- append(Interference_weights,rep(0, 5))
      
      get_Interference_PUCCH_weights <- c(0.794328235, 1, 1.258925412,1.584893192,1.995262315, 2.511886432,  3.16227766,  3.981071706, 5.011872336, 6.309573445, 10, 25.11886432,  63.09573445, 158.4893192,  398.1071706, 630.9573445)
      get_Interference_PUCCH_weights <- append(get_Interference_PUCCH_weights,rep(0, 5))
      
      Path_loss_weights <- c(25,52.5,57.5,62.5,67.5,72.5,77.5, 82.5, 87.5, 92.5, 97.5, 102.5, 107.5, 112.5, 117.5, 122.5, 127.5, 132.5, 137.5, 142.5, 147.5)
      MIMO_weights <- append(c(0,1,1),rep(0, 18))
      MIMO_Rank2_weights <- append(c(0,0,1),rep(0, 18))
      PDCCH_weights <- c(0.025,0.075,0.125,0.175,0.225,0.275,0.325,0.375,0.425,0.475,0.525,0.575,0.625,0.675,0.725,0.775,0.825,0.875,0.925,0.975,0)
      
      
      DC_weights <- rbind(CQI_weights,Interference_weights,get_Interference_PUCCH_weights,Path_loss_weights,MIMO_weights,MIMO_Rank2_weights,PDCCH_weights)
      
      
      dataset <- dat[1,c("DateTime_ID","erbs")]
      dat <- dat[order(dat$dcvector_index),]
      dat <- dat[,c("pmradiouerepcqidistr","pmradiorecinterferencepwr","pmradiorecinterferencepwrpucch","pmulpathlossdistr","pmradiotxrankdistr", "pmradiotxrankdistr","pmpdcchcceutil")]
      
      dat[16:21,1] <- 0
      
      dat <- as.matrix(dat)
      
      KPI <- DC_weights%*%dat
      
      a <- diag(KPI)
      
      
      b <-colSums(dat)
      
      
      c <- a/b
      
      if( b[2] > 0) {
        c[2] <- (10*log10(c[2]/1000000000000))
        
      }else {
        c[2] <- (-1)
      }
      
      if( b[3] > 0) {
        c[3] <- (10*log10(c[3]/1000000000000))
        
      }else {
        c[3] <- (-1)
      }
      
      c[5] <- 100*c[6]/c[5]
      c <- c[1:6] 
      #colnames(c) <- c("Average.CQI", "Average_Interference_PUSCH", "Average_Interference_PUCCH", "Avg_path_loss","Int_MIMO_Rank2")
      c <- cbind(dataset,t(c))
      return(c)
    }
    
    #library(parallel)
    # Calculate the number of cores
    #no_cores <- detectCores() - 2
    
    # Initiate cluster
    #cl <- makeCluster(no_cores)
    
    #clusterEvalQ(cl, library(dplyr))
    
    #clusterEvalQ(cl,varlist=c("get_CQI","get_Interference","get_Interference_PUCCH",
    #                          "get_Interference_PUCCH","get_path_loss","get_MIMO"))
    
    KPIs <- lapply(rs,function(dataset){
      dataset[,1] <- as.Date(dataset[,1])
      
      
      if(length(dataset[,1])>=21){
        KPIs_DCINDEX_VECTOR<- KPIs_vectorized(dataset)
        
      }
      else{
        KPIs_DCINDEX_VECTOR <- cbind(dataset[1,c("DateTime_id","erbs")],rep(0,6))
        
        
      }
      
      
      return(KPIs_DCINDEX_VECTOR)
    })
    
    
    
    df <- ldply(KPIs, data.frame)
    rm(KPIs)
    rm(dataset)

    df <- df[,2:9]
    df[is.na(df)] <- 0
    colnames(df) <- c("DateTime_ID","erbs","Average.CQI", "Average_Interference_PUSCH", "Average_Interference_PUCCH", "Avg_path_loss","Int_MIMO_Rank2","PDCCH_CCE_Util")
    
    return(df)
  }
  
  KPIs_cell_level_DCvector <- getdata(from,to,cluster,hourly,path)
  
  
  
  
  
  #for (e in length(DateTime_series)){
  #  from <- as.character(DateTime_series[e])
  #  to <- as.character(DateTime_series[e])
  #  from <- paste("(","'",from,"')")
  #  to <- paste("(","'",to,"')")
  #  dat <- getdata(from,to,cluster,hourly,path)
  #  KPIs_cell_level_DCvector <- rbind(KPIs_cell_level_DCvector,dat)
  #}
  
  
  write.csv(KPIs_cell_level_DCvector,paste(path,"Cell_level_vector.csv"), row.names = F)
  
  return(KPIs_cell_level_DCvector)}


#KPIs_DC <- extractData(from,to,cluster,hourly,path)

