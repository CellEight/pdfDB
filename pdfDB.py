import shutil
import os
import re
import sqlite3
import time

def listFiles(targetDir): #Works
    """Returns list of all the files in the target directory"""
    return os.listdir(targetDir)

def moveFiles(fileList, location, targetDir): #Works
    """Moves all the files in location with filename on fileList to target directory"""
    for file in fileList:
        shutil.move(location+"/"+file, targetDir)

def pdfGen(fileList): #Works
    """Generator takes list of file names and only yeilds those that end in .pdf"""
    for file in fileList:
        if re.match(r'.+\.pdf', file):
            yield file

def checkForPdfs(targetDir): #Works
    """Returns list of all the .pdf files in targetDir"""
    fileList = listFiles(targetDir)
    pdfFiles = [pdf for pdf in pdfGen(fileList)]
    return pdfFiles

def initDb(): #Works
    global conn
    c = conn.cursor()
    #Check if table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pdfDB';")
    conn.commit()
    if not c.fetchall():
        c.execute("CREATE TABLE pdfDB (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL, date DATETIME, location TEXT NOT NULL)")
        conn.commit()
    return

def dupGen(pdfFiles, date, dupOrNew): #Works
    global conn
    c = conn.cursor()
    for file in pdfFiles:
        location = os.path.expanduser(f'~/pdfDB/{date}/{file}')
        c.execute(r'SELECT * FROM pdfDB WHERE location = ?', (location, ))
        duplicates = c.fetchall()
        if dupOrNew:
            if duplicates:
                yield file, len(duplicates)
        elif not dupOrNew:
            if not duplicates:
                yield file

def moveDup(fileList, location, targetDir): #Works
    dupFileList = []
    for file in fileList:
        shutil.move(location+"/"+file[0], targetDir+"/"+file[0][:-4]+"-"+str(file[1])+".pdf")
        dupFileList.append(file[0][:-4]+"-"+str(file[1])+".pdf")
    return dupFileList

def addDbEntires(pdfFiles, date): #Works
    global conn
    c = conn.cursor()
    if pdfFiles:
        for file in pdfFiles:
            c.execute("INSERT INTO pdfDB (filename, date, location) VALUES (?,?,?)", (file,date,os.path.expanduser(f"~/pdfDB/{date}/{file}")))
    return

def main():
    while True:
        c = conn.cursor()
        pdfFiles = checkForPdfs(os.path.expanduser("~/Downloads"))
        if pdfFiles != []:
            date = time.strftime("%x").replace("/", "-")
            if not os.path.isdir(os.path.expanduser("~/pdfDB/"+date)):
                os.mkdir(os.path.expanduser("~/pdfDB/"+date))
            #Dived list into duplicates and non-duplicates
            newFiles = [file for file in dupGen(pdfFiles, date, False)]
            dupFiles = [(file, num) for file, num in dupGen(pdfFiles, date, True)]
            moveFiles(newFiles, os.path.expanduser("~/Downloads"), os.path.expanduser("~/pdfDB/"+date))
            addDbEntires(newFiles, date)
            print(dupFiles)
            dupFiles = moveDup(dupFiles, os.path.expanduser("~/Downloads"), os.path.expanduser("~/pdfDB/"+date))
            addDbEntires(dupFiles, date)
            #Check Updates Worked
            c.execute("SELECT * FROM pdfDB")
            conn.commit()
            print(c.fetchall())

#Check if the directory Structure for the program and archive are in place
#And if not create them
#NOTE Should probably add some try/except statments to deal with privalage issues
if not os.path.isdir(os.path.expanduser("~/pdfDB")):
    os.system("mkdir ~/pdfDB")
if not os.path.isdir(os.path.expanduser("~/pdfDB/config")):
    os.system("mkdir ~/pdfDB/config")
#Conect to the DB/file or create if not initialized
conn = sqlite3.connect(os.path.expanduser("~/pdfDB/config/pdfDB.db"))
#Add code to check if table exists and if not to initialize it
initDb()
main()
