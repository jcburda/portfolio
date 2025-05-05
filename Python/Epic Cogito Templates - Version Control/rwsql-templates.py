import os
import traceback
import time
from supportLibrary import *
import warnings

# ---------------------------------------------------
warnings.filterwarnings("ignore", category=UserWarning)
os.system("cls")
startTime = time.time()
errorMessage = ""

try:
    getToTheFileChoppa(log)
    writeToFileAndConsole(log, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + ret + "RunDate: " + getTimeAsFormattedString(startTime) + ret)
    
    #-------------------------
    # git clean repo
    #-------------------------
    writeToFileAndConsole(log, ret + linebreak + ret + "Git Ops - clean repo" + ret + linebreak + ret)  
    gitCleanRepo(outputRepo, remoteRepo)

    #-------------------------
    # get template data
    #-------------------------
    writeToFileAndConsole(log, ret + linebreak + ret + "Get template data" + ret + linebreak + ret) 
    deleteFileIfExists(lookup)
    deleteFileIfExists(zzlog)

    writeToFileAndConsole(log, "Getting data...")
    basicData = getResults("basic")
    paramData = getResults("param")
    displayData = getResults("display")
    queryData = getResults("query")

    writeToFileAndConsole(log, "Starting template processing...")
    templateCounter = 0
    os.makedirs(outputRepoSubfolder, exist_ok=True)

    for index, row in basicData.iterrows():
        hgrPadded = str(row.HgrRecordIdPadded).strip()
        hgrNumber = int(row.HgrRecordId)
        hgr = str(hgrNumber)
        templateName = row.TemplateName.strip()
        templateFile = outputRepoSubfolder + "\\" + hgrPadded + ".txt"

        # if hgrNumber == 100508 or hgrNumber == 100510: ------------------------------------------------------------------------------------------- testing
        if hgrNumber != -666:
            templateCounter = templateCounter + 1
            writeToFile(lookup, hgrPadded + " - " + templateName)
            
            deleteFileIfExists(templateFile)
            writeToFile(templateFile, "/**********************" + ret + ret + "***METADATA START***")

            #----------------------------------------------------
            # basic template info (HGR, HCM, RPT)
            #----------------------------------------------------
            writeToFile(templateFile, ret + ret + "BASIC " + linebreak + ret)
            transposeSingleRowAndWriteToFile(templateFile, basicData, row)

            #----------------------------------------------------
            # parameter info (HGP and HGT)
            #----------------------------------------------------
            writeToFile(templateFile, ret + ret + "PARAMETERS " + linebreak + ret)

            filteredData = paramData[tryConvertToInt(paramData.HgrRecordId) == hgrNumber]
            columns_to_exclude = ['HgrRecordId', 'ParamLine']
            filteredData = filteredData.drop(columns=columns_to_exclude, axis=1)
            transposeMultiRowAndWriteToFile(templateFile, filteredData)

            #--------------------------------------------
            # display columns
            #--------------------------------------------
            writeToFile(templateFile, ret + ret + "DISPLAY " + linebreak + ret)

            filteredData = displayData[tryConvertToInt(displayData.HgrRecordId) == hgrNumber]
            columns_to_exclude = ['HgrRecordId', 'Line']
            filteredData = filteredData.drop(columns=columns_to_exclude, axis=1)
            transposeMultiRowAndWriteToFile(templateFile, filteredData)

            writeToFile(templateFile, ret + ret + "***METADATA END***" + ret + ret + "**********************/" + ret)

            #--------------------------------------------
            # query
            #--------------------------------------------
            filteredData = queryData[tryConvertToInt(queryData.HgrRecordId) == hgrNumber]["Text"]
            lines_to_write = []
            for line in filteredData:
                if line is not None:
                    lines_to_write.append(line)
                else:
                    lines_to_write.append("")
            writeToFile(templateFile, '\n'.join(lines_to_write))

            #--------------------------------------------------

            writeToFileAndConsole(log, "Finished: " + hgrPadded + " - " + templateName)

    #--------------------------------------------------
    filesProcessedMessage = "Files processed: " + str(templateCounter)
    writeToFileAndConsole(log, filesProcessedMessage)
    logMessage = "update " + getTimeAsFormattedString(startTime)
    writeToFile(zzlog, logMessage)

    #-------------------------
    # git add + commit + push + log
    #-------------------------
    writeToFileAndConsole(log, ret + linebreak + ret + "GitOps - push changes" + ret + linebreak + ret)
    gitPushChanges(outputRepo, remoteRepo, logMessage)
    logAndDiffOutput = gitLogAndDiff(outputRepo)

    #-------------------------
    # get final email details
    #-------------------------
    emailOutput = []
    emailOutput.append("Repo: " + repo + ret)
    emailOutput.append(filesProcessedMessage + ret)
    emailOutput.append(logAndDiffOutput)

except Exception as e:
    returnCode = 1
    errorMessage = ret + "*** ERROR ***" + ret + str(e) + ret + ret + traceback.format_exc()
    writeToFileAndConsole(log, errorMessage)

finally:
    writeToFileAndConsole(log, ret)

    returnCodeMessage = "ReturnCode: " + str(returnCode)
    executionTimeMessage = "ExecutionTime: " + getElapsedTime(startTime)
    writeToFileAndConsole(log, returnCodeMessage)
    writeToFileAndConsole(log, executionTimeMessage + ret*3)

    if returnCode == 1:
        emailSubject = "CW Job Failure - RW SQL Templates"
        if errorMessage is None:
            errorMessage = ""
        sendEmail(emailSubject, errorMessage + ret + returnCodeMessage + ret + executionTimeMessage)
    else:
        emailSubject = "CW Job Success - RW SQL Templates"
        sendEmail(emailSubject, '\n'.join(emailOutput) + ret + returnCodeMessage + ret + executionTimeMessage)