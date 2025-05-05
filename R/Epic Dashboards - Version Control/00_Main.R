library(jsonlite)
library(httr)
library(stringr)
library(dplyr)
library(purrr)
library(rlang)

#____________________________________

automationFolder <- REDACTED
setWorkingDirectory <- function(filePath) {
  setwd(automationFolder)
}

setWorkingDirectory()
source("01_Support.R")
source("02_Support_SQL.R")
source("03_Support_FileOps.R")

result <- tryCatch(
{
  #____________________________________
  # intro ====
  #____________________________________
  clearConsole()
  getToTheFileChoppa(log, maxLogLines, logLinesToChop)
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  writeToFileAndConsole(log, content <- paste0(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", ret, "RunDate: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), ret))
  
  #____________________________________
  # git clean repo
  #____________________________________
  writeToFileAndConsole(log, paste0(ret, linebreak, ret,"GitOps - clean repo", ret, linebreak, ret))
  
  if (!dir.exists(localRepo)) {
    stop(paste0("repo: ", localRepo, " does not exist."))
  }
  
  gitOps_CleanRepo(localRepo, remoteRepo)
  
  #____________________________________
  # start processing
  #____________________________________
  writeToFileAndConsole(log, paste0(ret, linebreak, ret,"Starting processing", ret, linebreak, ret))
  deleteFileIfExists(zzlog)
  
  # get all data
  df_base <- s2_GetSQLData(s2_query_base) %>% arrange(IDM_ID, IDB_ID, IDK_ID, IDN_ID, IDJ_ID, HCM_ID)
  df_dashboards <- s2_GetSQLData(s2_query_dashboards) %>% arrange(IDM_ID)
  df_dashboards_componentList <- s2_GetSQLData(s2_query_dashboards_componentList) %>% arrange(IDM_ID, LINE)
  df_components <- s2_GetSQLData(s2_query_components) %>% arrange(IDB_ID)
  df_components_links <- s2_GetSQLData(s2_query_components_links) %>% arrange(IDB_ID, LINE)
  df_resources <- s2_GetSQLData(s2_query_resources) %>% arrange(IDK_ID)
  df_metrics <- s2_GetSQLData(s2_query_metrics) %>% arrange(IDN_ID)
  df_metricQueries <- s2_GetSQLData(s2_query_metricQueries) %>% arrange(IDJ_ID)
  df_metricQueries_text <- s2_GetSQLData(s2_query_metricQueries_text) %>% arrange(IDJ_ID, LINE)
  
  # create summary; don't use Excel since the binary seems to be added to the commits each time, even though no changes...
  # you can just open with Excel to view nicely
  setwd(localRepo)
  write.table(df_base,"dashboard_overview.txt",sep="\t",row.names=FALSE)
  setWorkingDirectory()
  
  #____________________________________
  # dashboards ====
  #____________________________________
  mf = "IDM"
  mf_desc = "DASHBOARD"
  dashboardCounter <- 0
  df_current <- df_dashboards

  mfvar <- s3_GetMasterFileVariables(mf, mf_desc)

  for (i in 1:nrow(df_current)) {

    rvar <- s3_GetRowVariables(df_current, i, mfvar, mf)
    if (rvar$id != "-666") {
    # if (rvar$id == "82000") {

      # write standard details
      dashboardCounter <- dashboardCounter + 1
      s3_writeBasicDetails(mfvar, rvar)

      # write related objects
      s3_writeRelatedObjects(mf, "IDB", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDK", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDN", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDJ", df_base, rvar)

    }
  }

  writeToFileAndConsole(log, paste0("Dashboards completed!"))

  #____________________________________
  # components ====
  #____________________________________
  mf = "IDB"
  mf_desc = "COMPONENT"
  componentCounter <- 0
  df_current <- df_components

  mfvar <- s3_GetMasterFileVariables(mf, mf_desc)

  for (i in 1:nrow(df_current)) {

    rvar <- s3_GetRowVariables(df_current, i, mfvar, mf)
    if (rvar$id != "-666") {
    # if (rvar$id == "82000") {

      # write standard details
      componentCounter <- componentCounter + 1
      s3_writeBasicDetails(mfvar, rvar)

      #__________________________________________________________________________________________
      # write links list (only for components with DISPLAY FORMAT = LINK)
      writeToFile(rvar$file, paste0(ret, "LINKS: (NAME; TYPE; URL; OPEN_NEW_BROWSER)"))
      df_specific <- df_components_links %>%
        filter(IDB_ID == rvar$id) %>%
        distinct()

      # most components are not links, so don't write anything if they're not...
      lines_to_write <- character(0)
      if (nrow(df_specific) != 0) {
        for (i in 1:nrow(df_specific)) {
          p_rw2 <- df_specific[i, ]
          column_line <- paste0(
            as.character(p_rw2$LINK_LABEL)
            , "; "
            , as.character(p_rw2$LINK_TYPE_NAME)
            , "; "
            , as.character(p_rw2$LINK_URL_ACT)
            , "; "
            , as.character(p_rw2$LINK_NEW_BROWSER_YN)
          )
          lines_to_write <- c(lines_to_write, column_line)
        }
      }
      else {
        lines_to_write <- "NA"
      }
      writeToFile(rvar$file, lines_to_write)
      #__________________________________________________________________________________________

      # write related objects
      s3_writeRelatedObjects(mf, "IDM", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDK", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDN", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDJ", df_base, rvar)

    }
  }

  writeToFileAndConsole(log, paste0("Components completed!"))

  #____________________________________
  # resources ====
  #____________________________________
  mf = "IDK"
  mf_desc = "RESOURCE"
  resourceCounter <- 0
  df_current <- df_resources

  mfvar <- s3_GetMasterFileVariables(mf, mf_desc)

  for (i in 1:nrow(df_current)) {

    rvar <- s3_GetRowVariables(df_current, i, mfvar, mf)
    if (rvar$id != "-666") {
    # if (rvar$id == "82000") {

      # write standard details
      resourceCounter <- resourceCounter + 1
      s3_writeBasicDetails(mfvar, rvar)

      # write related objects
      s3_writeRelatedObjects(mf, "IDM", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDB", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDN", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDJ", df_base, rvar)

    }
  }

  writeToFileAndConsole(log, paste0("Resources completed!"))

  #____________________________________
  # metrics ====
  #____________________________________
  mf = "IDN"
  mf_desc = "METRIC"
  metricCounter <- 0
  df_current <- df_metrics

  mfvar <- s3_GetMasterFileVariables(mf, mf_desc)

  for (i in 1:nrow(df_current)) {

    rvar <- s3_GetRowVariables(df_current, i, mfvar, mf)
    if (rvar$id != "-666") {
    # if (rvar$id == "82000") {

      # write standard details
      metricCounter <- metricCounter + 1
      s3_writeBasicDetails(mfvar, rvar)

      # write related objects
      s3_writeRelatedObjects(mf, "IDM", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDB", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDK", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDJ", df_base, rvar)

    }
  }

  writeToFileAndConsole(log, paste0("Metrics completed!"))

  #____________________________________
  # metric queries ====
  #____________________________________
  mf = "IDJ"
  mf_desc = "METRICQUERY"
  metricQueryCounter <- 0
  df_current <- df_metricQueries

  mfvar <- s3_GetMasterFileVariables(mf, mf_desc)

  for (i in 1:nrow(df_current)) {

    rvar <- s3_GetRowVariables(df_current, i, mfvar, mf)
    if (rvar$id != "-666") {
    # if (rvar$id == "82000") {

      # write standard details
      metricQueryCounter <- metricQueryCounter + 1
      s3_writeBasicDetails(mfvar, rvar)

      #_____________________________________________________________
      #write query text
      writeToFile(rvar$file, paste0(ret, "QUERY TEXT:"))
      df_specific <- df_metricQueries_text %>%
        filter(IDJ_ID == rvar$id) %>%
        distinct()

      lines_to_write <- character(0)
      if (nrow(df_specific) != 0) {
        for (i in 1:nrow(df_specific)) {
          p_rw2 <- df_specific[i, ]
          column_line <- paste0(
            as.character(p_rw2$Text)
          )
          lines_to_write <- c(lines_to_write, column_line)
        }
      }
      else {
        lines_to_write <- "NA"
      }
      writeToFile(rvar$file, lines_to_write)
      #_____________________________________________________________

      # write related objects
      s3_writeRelatedObjects(mf, "IDM", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDB", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDK", df_base, rvar)
      s3_writeRelatedObjects(mf, "IDN", df_base, rvar)

    }
  }

  writeToFileAndConsole(log, paste0("MetricQueries completed!"))
  
  #____________________________________
  # final ====
  #____________________________________

  filesProcessedMessage <- character(0)

  filesProcessedMessage <- paste0(filesProcessedMessage, ret, "Dashboards processed: ", dashboardCounter)
  filesProcessedMessage <- paste0(filesProcessedMessage, ret, "Components processed: ", componentCounter)
  filesProcessedMessage <- paste0(filesProcessedMessage, ret, "Resources processed: ", resourceCounter)
  filesProcessedMessage <- paste0(filesProcessedMessage, ret, "Metrics processed: ", metricCounter)
  filesProcessedMessage <- paste0(filesProcessedMessage, ret, "MetricQueries processed: ", metricQueryCounter)
  writeToFileAndConsole(log, filesProcessedMessage)
  
  logMessage <- paste0("update ", now)
  writeToFile(zzlog, logMessage)

  #_________________________
  # git add + commit + push + log
  #_________________________
  writeToFileAndConsole(log, paste0(ret, linebreak, ret,"GitOps - push changes", ret, linebreak, ret))

  emailOutputSuccess <- c(paste0("Repo: ", repoName), "")
  emailOutputSuccess <- c(emailOutputSuccess, filesProcessedMessage, ret)
  logResult <- gitOps_PushChanges(localRepo, remoteRepo, logMessage)
  emailOutputSuccess <- c(emailOutputSuccess, logResult)

  #_________________________
  # Final
  #_________________________
  resultCodeMessage <- paste0(ret, "Return code: ", resultCode)
  resultTimeMessage <- paste0("Run time: ", getElapsedTime(startTime, Sys.time()))

  writeToFileAndConsole(log, resultCodeMessage)
  writeToFileAndConsole(log, c(resultTimeMessage, ret, ret))

  emailOutputSuccess <- c(emailOutputSuccess, ret, resultCodeMessage, resultTimeMessage)
  sendEmail(emailOutputSuccess, resultCode)
},
error = function(e)
{
  resultCode <- 1

  errorMessage <- paste0(ret, "***ERROR***", ret, e, ret, traceback(), ret)
  resultCodeMessage <- paste0(ret, "Return code: ", resultCode)
  resultTimeMessage <- paste0("Run time: ", getElapsedTime(startTime, Sys.time()))

  writeToFileAndConsole(log, errorMessage)
  writeToFileAndConsole(log, resultCodeMessage)
  writeToFileAndConsole(log, c(resultTimeMessage, ret, ret))

  emailOutputFailure <- c(c(paste0("Repo: ", repoName), ""), ret, "Error during processing: ", errorMessage, ret, resultCodeMessage, resultTimeMessage)
  sendEmail(emailOutputFailure, resultCode)
})
