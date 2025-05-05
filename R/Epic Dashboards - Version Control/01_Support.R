library(dplyr)
library(odbc)
library(DBI)
library(lubridate)
library(config)
library(emayili)
library(glue)

automationFolder <- REDACTED

#_______________________

configFile <- paste0(automationFolder, "config.yml")
config_github <- config::get("github")

repoName <- REDACTED
localRepo <- REDACTED
remoteRepo <- paste0("https://",config_github$user,":",config_github$pat,"@github.com/",repoName,".git")

zzlog <-paste0(localRepo,"/zzlog.txt")
log <- paste0(automationFolder, "logHistory.txt")

errorFull <- ""
ret <- "\n"
linebreak <- "--------------------------"
maxLogLines <- 5000
logLinesToChop <- 1000
padBasic <- 35
output <- ""
cmdResult <- list()

#github api
auth <- base64_enc(paste0(config_github$user, ":", config_github$pat))
headers <- add_headers(
  Authorization = paste("Basic", auth)
  ,`User-Agent` = "R-GitHub-API-Request"
)

resultCode <- 0
startTime <- Sys.time()

#_____________________________________________
# general operations
#_____________________________________________
clearConsole <- function() {
  shell("cls")
}

writeToFile <- function(filePath, content) {
  if (!file.exists(filePath)) {
    file.create(filePath)
  }
  
  file_conn <- file(filePath, open = "a", encoding = "UTF-8")
  cat(content, sep = ret, file = file_conn)
  close(file_conn)
}

writeToFileAndConsole <- function(filePath, content) {
  writeToFile(filePath, content)
  cat(content, sep = ret)
}

writeToConsole <- function(content) {
  cat(content, sep = ret)
}

writeToStringList <- function(list, content) {
  return(c(list, content))
}

deleteFileIfExists <- function(filePath) {
  if (file.exists(filePath)) {
    file.remove(filePath)
  } 
}

createDirectoryIfDoesNotExist <- function(dir) {
  if (!file.exists(dir)) {
    dir.create(dir)
  } 
}

getToTheFileChoppa <- function(file, maxLines, lineToChop) {
  if (file.exists(file)) {
    lines <- readLines(file, warn = FALSE, encoding = "UTF-8")
    
    if (length(lines) > maxLines) {
      # Remove the first logLineSkip lines
      lines <- lines[-seq_len(lineToChop)]
      
      # Overwrite the log file with the modified content
      writeLines(lines, file, useBytes=T)
      
    # writeToFileAndConsole(file, paste0("Deleted the first ", lineToChop, " lines from logHistory.\n"))
    # } else {
    # writeToFileAndConsole(file, paste0("logHistory file has ", maxLines, " or fewer lines; none deleted.\n"))
    }
  }
}

getElapsedTime <- function(start, end) {
  time_diff <- difftime(end, start, units = "secs")
  
  # Extract hours, minutes, and seconds components
  hours <- as.integer(time_diff) %/% 3600
  minutes <- (as.integer(time_diff) %% 3600) %/% 60
  seconds <- as.integer(time_diff) %% 60
  
  # Format as HH:MM:SS
  formatted_time <- sprintf("%02d:%02d:%02d", hours, minutes, seconds)
  
  # Print the formatted time
  return(formatted_time)
  
}

padright <- function(input_string, width, padding_char = " ") {
  if (length(input_string) >= width) {
    return(input_string)
  }
  
  padding_length <- width - nchar(input_string)
  padding <- strrep(padding_char, padding_length)
  
  return(paste0(input_string, padding))
}

transposeAndWriteToFile <- function(filePath, df) {
  lines_to_write <- character(0)

  for (i in 1:nrow(df)) {
    row <- df[i, ]
    
    column_lines <- sapply(names(row), function(col_name) {
      paste0(padright(paste0(col_name, ":"), padBasic), coalesce(as.character(row[col_name]), ""))
    })
    
    lines_to_write <- c(lines_to_write, paste(column_lines, collapse = "\n"), "")
  }
  
  writeToFile(filePath, lines_to_write)
}

#_____________________________________________
# git operations
#_____________________________________________
gitOps_RunCmd <- function(cmd) {
  cmdResult <- list()
  
  # no extra logging for final log + diff
  if (grepl("git diff", cmd) || grepl("git log", cmd)) {
    argsText <- substr(cmd, 5, nchar(cmd))
    cmdResult <- system2("git", args = argsText, stdout = TRUE, stderr = TRUE)
  }
  else
  {
    #don't write token
    if (grepl("git push", cmd)) {
      writeToFileAndConsole(log, "git push to remote repo")
    }
    else {
      writeToFileAndConsole(log, cmd)
    } 
    
    argsText <- substr(cmd, 5, nchar(cmd))
    cmdResult <- system2("git", args = argsText, stdout = TRUE, stderr = TRUE)
    
    for (str in cmdResult) {
      writeToFileAndConsole(log, paste0("  ",str))
    } 
    
  }
  
  return(cmdResult) 
}

gitOps_CleanRepo <- function(localRepo, remoteRepo) {
  
  setwd(localRepo)
  
  gitOps_RunCmd("git status --short")
  gitOps_RunCmd("git clean -f -d -n")
  gitOps_RunCmd("git clean -f -d")
  gitOps_RunCmd("git status --short")
  gitOps_RunCmd("git reset --hard origin/main")
  gitOps_RunCmd("git pull")
  
  #verify no pending changes at this point
  counter <- 0
  cmdOutput <- gitOps_RunCmd("git status")
  for (str in cmdOutput) {
    if(grepl("nothing to commit", str)){
      counter <- counter + 1
    }
  }
  if (counter == 0)
  {
    stop("Changes are present after cleaning repo")
  }
  
  #no changes now; so we can verify our remote access
  counter <- 0
  cmdOutput <- gitOps_RunCmd(paste0("git push ", remoteRepo))
  for (str in cmdOutput) {
    if(grepl("fatal: Authentication failed", str)){
      counter <- counter + 1
    }
  }
  if (counter != 0)
  {
    stop("Authentication failed")
  }
}

gitOps_PushChanges <- function(localRepo, remoteRepo, commitMessage) {
  
  setwd(localRepo)
  
  gitOps_RunCmd("git status --short")
  gitOps_RunCmd("git add .")
  gitOps_RunCmd("git status --short")
  
  #verify at least one pending change at this point
  counter <- 0
  cmdOutput <- gitOps_RunCmd("git status")
  for (str in cmdOutput) {
    if(grepl("no changes added to commit", str)){
      counter <- counter + 1
    }
    if(grepl("nothing to commit", str)){
      counter <- counter + 1
    }
  }
  if (counter != 0)
  {
    stop(paste0("No files added to commit. Expecting at least 1 file (the log file)"))
  }
  
  gitOps_RunCmd(paste0("git commit -m \"", commitMessage, "\""))
  gitOps_RunCmd("git status --short")
  gitOps_RunCmd(paste0("git push ", remoteRepo))
  gitOps_RunCmd("git status --short")
  
  #verify no pending changes at this point
  counter <- 0
  cmdOutput <- gitOps_RunCmd("git status")
  for (str in cmdOutput) {
    if(grepl("nothing to commit", str)){
      counter <- counter + 1
    }
  }
  if (counter == 0)
  {
    stop("Changes are present after pushing repo")
  }
  
  cmdOutputLog <- gitOps_RunCmd("git log --oneline --numstat -1")
  cmdOutput <- c(cmdOutputLog)
  return(cmdOutput)
}

# _____________________________________________
# email operations
# _____________________________________________
sendEmail <- function(listOfStringMessage, isFailure = 0) {
  emailBody <- paste(listOfStringMessage, collapse = "\n")
  emailSubject <- ""
  if (isFailure == 0) {
    emailSubject <- "CW Job Success - Dashboards"
  }
  else {
    emailSubject <- "CW Job Failure - Dashboards"
  }

  email <- envelope() %>%
    from(REDACTED) %>%
    to(REDACTED) %>%
    subject(emailSubject) %>%
    text(content = emailBody)
  
  smtp <- server(
    host = REDACTED,
    port = REDACTED
  )

  smtp(email, verbose = FALSE)
}















