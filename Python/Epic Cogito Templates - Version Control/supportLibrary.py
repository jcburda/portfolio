import subprocess
import time
from datetime import datetime
import os
import fileinput
import json
import pyodbc
import pandas
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# --------------------------------------

config = os.environ["GitHubConfig"]
with open(config, "r", encoding="utf-8") as file:
    data = json.load(file)
configAdminUser = data["admin"]["user"]
configAdminPw = data["admin"]["pw"]
configGitHubUser = data["github"]["user"]
configGitHubPat = data["github"]["pat"]
configEmailHost = data["email"]["smtpHost"]
configEmailPort = data["email"]["port"]
configSqlDatabasesClarityPrd = data["sql"]["databases"]["REDACTED"]

returnCode = 0
repo = REDACTED
remoteRepo = (
    "https://"
    + configGitHubUser
    + ":"
    + configGitHubPat
    + "@github.com/" + repo + ".git"
)
outputRepo = REDACTED
outputRepoSubfolder = outputRepo + r"\templates"
automationFolder = (
    REDACTED
)

zzlog = outputRepoSubfolder + r"\zzlog.txt"
lookup = outputRepoSubfolder + r"\zz1lookup.txt"
log = automationFolder + r"\logHistory.txt"

fileCounter = 0
templateFile = ""
ret = "\n"
tab = "\t"
linebreak = "--------------------------"
maxLogLines = 5000
logLinesToChop = 1000
padBasic = 25
output = ""
cmdResult = list()

# --------------------------------------


class CustomException(Exception):
    def __init__(self, message):
        super().__init__(message)


def writeToFileAndConsole(filePath, message):
    with open(filePath, "a", encoding="utf-8") as file:
        if message is not None:
            file.write(message + ret)
        else:
            file.write("" + ret)
    print(message)


def writeToFile(filePath, message):
    with open(filePath, "a", encoding="utf-8") as file:
        if message is not None:
            file.write(message + ret)
        else:
            file.write("" + ret)


def deleteFileIfExists(filePath):
    if os.path.exists(filePath):
        os.remove(filePath)


def getToTheFileChoppa(filePath):
    if os.path.exists(filePath):
        with open(filePath, "r", encoding="utf-8") as file:
            lines = file.readlines()

    if len(lines) > maxLogLines:
        lines = lines[logLinesToChop:]

    with open(filePath, "w", encoding="utf-8") as file:
        file.writelines(lines)


def getTimeAsFormattedString(startTime):
    datetime_obj = datetime.fromtimestamp(startTime)
    formatted_time = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time


def getElapsedTime(startTime):
    elapsed_time = time.time() - startTime
    elapsed_hours, remainder = divmod(int(elapsed_time), 3600)
    elapsed_minutes, elapsed_seconds = divmod(remainder, 60)
    return f"{elapsed_hours:02d}:{elapsed_minutes:02d}:{elapsed_seconds:02d}"


def padRight(s, width, fillchar=" "):
    return s + (fillchar * (width - len(s)))


def tryConvertToInt(value):
    try:
        result = int(value)
    except:
        result = value
    return result


def transposeSingleRowAndWriteToFile(filePath, df, row):
    lines_to_write = []
    for colName in df.columns:
        colLine = padRight(colName, padBasic) + str(tryConvertToInt(row[colName]))
        lines_to_write.append(colLine)

    writeToFile(filePath, "\n".join(lines_to_write))


def transposeMultiRowAndWriteToFile(filePath, df):
    lines_to_write = []
    for i in range(len(df)):
        row = df.iloc[i]
        for colName in df.columns:
            colLine = padRight(colName, padBasic) + str(tryConvertToInt(row[colName]))
            lines_to_write.append(colLine)
        lines_to_write.append("")

    writeToFile(filePath, "\n".join(lines_to_write))


def coalesce(value, replacement=""):
    return value if value is not None else replacement


# ---------------------------------------
# sql operations
# ---------------------------------------


def getResults(option):
    connection = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=" + REDACTED + ";"
        "DATABASE=" + "REDACTED" + ";"
        "UID=" + configAdminUser + ";"
        "PWD=" + configAdminPw + ";"
        "Trusted_Connection=yes"
    )
    df = pandas.read_sql_query(
        "set nocount on; EXEC REDACTED @option='"
        + option
        + "'",
        connection,
    )
    return df


# --------------------------------------
# git operations
# --------------------------------------


def gitRunCmd(command):
    # write command to log (except for these)
    searchTerms = ("git log", "git diff", "git push")
    if not any(term in command for term in searchTerms):
        writeToFileAndConsole(log, command)

    # specifically for push, just write a generic message (otherwise you will write token)
    searchTerms = ("git push",)
    if any(term in command for term in searchTerms):
        writeToFileAndConsole(log, "git push to remote repo")

    result = subprocess.run(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode == 0:
        # log and diff specifically should only be written to the final success email
        # sometimes stderr is a continuation of a valid git operation message (e.g. git push w/ no changes has success message in stderr)
        searchTerms = ("git log", "git diff")
        if not any(term in command for term in searchTerms):
            stdoutOutput = (result.stdout).split("\n")
            for message in stdoutOutput:
                writeToFileAndConsole(log, tab + message)

            stderrOutput = (result.stderr).split("\n")
            for message in stderrOutput:
                writeToFileAndConsole(log, tab + message)

        return coalesce(result.stdout) + ret + coalesce(result.stderr)

    else:
        raise CustomException(
            "Git command failed with an error: " + coalesce(result.stdout) + ret + coalesce(result.stderr)
        )


def gitCleanRepo(localRepoPath, remoteRepoPath):
    os.chdir(localRepoPath)
    gitRunCmd("git status --short")
    gitRunCmd("git clean -f -d -n")
    gitRunCmd("git clean -f -d")
    gitRunCmd("git status --short")
    gitRunCmd("git reset --hard origin/main")
    gitRunCmd("git pull")

    # verify no pending changes to the repo at this point
    counter = 0
    output = gitRunCmd("git status")
    outputSplit = output.split("\n")
    for message in outputSplit:
        if "nothing to commit" in message:
            counter = counter + 1
    if counter == 0:
        raise CustomException("Changes are present after cleaning repo: " + output)

    # if you've made it this far...there should be no pending changes
    counter = 0
    output = gitRunCmd("git push " + remoteRepoPath)
    outputSplit = output.split("\n")
    for message in outputSplit:
        if "fatal: Authentication failed" in message:
            counter = counter + 1
    if counter != 0:
        raise CustomException("Authentication failed: " + output)


def gitPushChanges(localRepoPath, remoteRepoPath, logMessageText):
    os.chdir(localRepoPath)
    gitRunCmd("git status --short")
    gitRunCmd("git add .")
    gitRunCmd("git status --short")

    #verify at least one pending change at this point
    counter = 0
    output = gitRunCmd("git status")
    outputSplit = output.split("\n")
    for message in outputSplit:
        if "nothing to commit" in message or "no changes added to commit" in message:
            counter = counter + 1
    if counter != 0:
        raise CustomException("No files added to commit. Expecting at least 1 file (the log file): " + output)
    
    gitRunCmd("git commit -m \"" + logMessageText + "\"")
    gitRunCmd("git status --short")
    gitRunCmd("git push " + remoteRepoPath)
    gitRunCmd("git status --short")

    #verify no pending changes at this point
    counter = 0
    output = gitRunCmd("git status")
    outputSplit = output.split("\n")
    for message in outputSplit:
        if "nothing to commit" in message:
            counter = counter + 1
    if counter == 0:
        raise CustomException("Changes are present after pushing to remote repo: " + output)
    
    
def gitLogAndDiff(localRepoPath):
    logOutput = gitRunCmd("git log --oneline --numstat -1")
    diffOutput = gitRunCmd("git diff head~1..head -U3")
    return (logOutput + ret + diffOutput)


# ---------------------------------------
# email operations
# ---------------------------------------
def sendEmail(emailSubject, emailMessage):
    sender_email = REDACTED
    receiver_email = sender_email
    subject = emailSubject
    body = emailMessage

    smtp_server = configEmailHost
    smtp_port = configEmailPort

    # Create a MIMEText object to represent the email content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()  # Use TLS for secure connection
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()