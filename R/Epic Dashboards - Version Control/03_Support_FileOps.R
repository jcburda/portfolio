library(dplyr)
library(odbc)
library(DBI)
library(lubridate)
library(config)
library(glue)
library(rlang)

automationFolder <- REDACTED

source("01_Support.R")

#________________________________________________________

s3_GetMasterFileVariables <- function(p_mf, p_mf_desc){
  
  mf_id_column = paste0(p_mf, "_ID")
  mf_id_padded_column = paste0(p_mf, "_ID_PADDED")
  mf_name_column = paste0(p_mf, "_NAME")
  targetOutputDirectory <- paste0(localRepo,"/",p_mf,"_",p_mf_desc)
  createDirectoryIfDoesNotExist(targetOutputDirectory)
  targetLookupFile <- paste0(targetOutputDirectory,"/zz",p_mf,"_lookup.txt")
  deleteFileIfExists(targetLookupFile)
  
  returnList <- list(
    "mf_id_column" = mf_id_column
    ,"mf_id_padded_column" = mf_id_padded_column
    ,"mf_name_column" = mf_name_column
    ,"targetOutputDirectory" = targetOutputDirectory
    ,"targetLookupFile" = targetLookupFile
    )
  return(returnList)
}

s3_GetRowVariables <- function(p_df_current, p_i, p_mfvar, p_mf){
  
  rw <- p_df_current[p_i, ]
  id <- trimws(as.character(rw[ ,p_mfvar$mf_id_column]))
  id_padded <- trimws(as.character(rw[ ,p_mfvar$mf_id_padded_column]))
  name <- trimws(rw[ ,p_mfvar$mf_name_column])
  file <- paste0(p_mfvar$targetOutputDirectory,"/",p_mf,"_",id_padded,".txt")
  deleteFileIfExists(file)
  
  returnList <- list(
    "rw" = rw
    ,"id" = id
    ,"id_padded" = id_padded
    ,"name" = name
    ,"file" = file
  )
  return(returnList)
}

s3_writeBasicDetails <- function(p_mfvar, p_rvar){
  
  writeToFile(p_mfvar$targetLookupFile, paste0(p_rvar$id_padded, " - ", p_rvar$name))
  
  lines_to_write <- character(0)
  for (col_name in names(p_rvar$rw)) {
    column_line <- paste0(padright(paste0(col_name, ":"), padBasic), coalesce(as.character(p_rvar$rw[, col_name]), ""))
    lines_to_write <- c(lines_to_write, column_line)
  }
  writeToFile(p_rvar$file, lines_to_write)
}

s3_writeRelatedObjects <- function(p_sourceMF, p_targetMF, p_df_base, p_rvar){
  
  # get IDs and Names columns
  sourceMFId = paste0(p_sourceMF,"_ID")
  targetMFId = paste0(p_targetMF,"_ID")
  targetMFName = paste0(p_targetMF,"_NAME")
  
  # use this syntax to turn string "IDM_ID" into column name
  df_specific <- p_df_base %>% 
    filter(!!as.symbol(sourceMFId) == p_rvar$id) %>% 
    filter(!!as.symbol(targetMFId) != "NA") %>% 
    distinct(!!as.symbol(targetMFId), !!as.symbol(targetMFName))
  
  # translate MF to more useful name for section header
  if(p_targetMF == "IDM"){
    p_targetMF = paste0(p_targetMF, "_DASHBOARDS")
  }
  else if(p_targetMF == "IDB"){
    p_targetMF = paste0(p_targetMF, "_COMPONENTS")
  }
  else if(p_targetMF == "IDK"){
    p_targetMF = paste0(p_targetMF, "_RESOURCES")
  }
  else if(p_targetMF == "IDN"){
    p_targetMF = paste0(p_targetMF, "_METRICS")
  }
  else if(p_targetMF == "IDJ"){
    p_targetMF = paste0(p_targetMF, "_METRICQUERIES")
  }
  text = paste0("RELATED_",p_targetMF,":")
  writeToFile(p_rvar$file, paste0(ret, text))
  
  # actually write lines
  lines_to_write <- character(0)
  if (nrow(df_specific) != 0) {
    for (i in 1:nrow(df_specific)) {
      p_rw2 <- df_specific[i, ]
      column_line <- paste0(
        as.character(p_rw2[ ,targetMFName])
        , " ["
        , as.character(p_rw2[ ,targetMFId])
        , "]"
      )
      lines_to_write <- c(lines_to_write, column_line)
    }
  }
  else {
    lines_to_write <- "NA"
  }
  writeToFile(p_rvar$file, lines_to_write) 
}