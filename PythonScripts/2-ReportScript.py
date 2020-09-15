# *************************************************************************
# Desc: Functions for Report generaton
# Change Log: When,Who,What
# 2020-06-17, Emilija Dimikj, Created file and functions
# *************************************************************************
import pymongo
import pyodbc
from datetime import datetime as dt


def getDWConnectionString():
    server = 'is-root01.ischool.uw.edu\\BI'
    database = 'DWClinicReportDataEDimikj'
    username = 'UN**'
    password = 'PASS**'
    driver = "{ODBC Driver 17 for SQL Server}"
    strConn = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';' \
                                                                                   'UID=' + username + ';PWD=' + password + ";"
    try:
        cnxn = pyodbc.connect(strConn, timeout=1)
        return strConn
    except Exception as ex:
        print(ex)
        return str(ex)


def getMongoConnectionStr():
    strConn = "mongodb+srv://admin:***@cluster0-yogcb.azure.mongodb.net/test?retryWrites=true&w=majority"
    try:
        objCon = pymongo.MongoClient(strConn)
        return strConn
    except Exception as ex:
        print(ex)
        return str(ex)


def getMongoDBVisits():
    strConn = getMongoConnectionStr()
    try:
        objConn = pymongo.MongoClient(strConn)
        dbName = 'ClinicReports'
        collName = 'Visits'

        db = objConn[dbName]
        objColl = db[collName]
        VisitsColl = objColl.find({}).sort('VisitClinicName')
        print(VisitsColl.collection.count_documents({}))

        pipeline = [{"$group": {
            "_id": "$VisitClinicName",
            "ClinicName": {"$first": "$VisitClinicName"},
            "ClinicVisitCharge": {"$sum": "$VisitCharge"},
            "Count": {"$sum": 1}}}, {"$sort": {
            "ClinicName": 1}}]
        resultTable = []
        for d in VisitsColl.collection.aggregate(pipeline):
            resultTable.append({'Clinic Name': d['ClinicName'],
                                'Total Visits Charge': d['ClinicVisitCharge'],
                                'Number of Visits': d['Count']})
        pipeline = [{"$group": {
            "_id": None,
            "TotalCharge": {"$sum": "$VisitCharge"},
            "TotalRows": {"$sum": 1}}}]
        for d in VisitsColl.collection.aggregate(pipeline):
            resultTable.append({'Clinic Name': 'GRAND TOTAL',
                                'Total Visits Charge': d["TotalCharge"],
                                'Number of Visits': d["TotalRows"]})
        return resultTable

    except Exception as ex:
        print(ex)


def getDWVisits():
    strConn = getDWConnectionString()
    try:
        cnxn = pyodbc.connect(strConn)
        cursor = cnxn.cursor()
        sqlStr = "select VisitClinicName,sum(VisitCharge),count(*)" \
                 "from [dbo].[vETLVisitsCollection] " \
                 "group by VisitClinicName order by VisitClinicName"
        cursor.execute(sqlStr)
        rows = cursor.fetchall()

        FactVisitsTable = []
        for row in rows:
            dicData = {'Clinic Name': row[0],
                       'Total Visits Charge': row[1],
                       'Number of Visits': row[2]}
            FactVisitsTable.append(dicData)

        sqlStr = "select 'GRAND TOTAL', sum(VisitCharge),count(*) from [dbo].[vETLVisitsCollection] "
        cursor.execute(sqlStr)
        rows = cursor.fetchall()
        for row in rows:
            dicData = {'Clinic Name': row[0],
                       'Total Visits Charge': row[1],
                       'Number of Visits': row[2]}
            FactVisitsTable.append(dicData)

        return FactVisitsTable
    except Exception as ex:
        print(ex)


def getMongoDBDrShifts():
    strConn = getMongoConnectionStr()
    try:
        objConn = pymongo.MongoClient(strConn)
        dbName = 'ClinicReports'
        collName = 'DrShifts'

        db = objConn[dbName]
        objColl = db[collName]
        DrShiftsColl = objColl.find({})
        print(DrShiftsColl.collection.count_documents({}))

        pipeline = [{"$group": {
            "_id": "$DoctorName",
            "DoctorName": {"$first": "$DoctorName"},
            "TotalHoursWorked": {"$sum": "$HoursWorked"},
            "Count": {"$sum": 1}}}, {"$sort": {
            "DoctorName": 1}}]
        resultTable = []
        TotalHours = 0
        TotalRows = 0
        for d in DrShiftsColl.collection.aggregate(pipeline):
            resultTable.append({'Doctor Name': d['DoctorName'],
                                'Total Hours Worked': d['TotalHoursWorked'],
                                'Number of Shifts': d['Count']})
            TotalHours += d['TotalHoursWorked']
            TotalRows += d['Count']

        resultTable.append({'Doctor Name': 'GRAND TOTAL',
                            'Total Hours Worked': TotalHours,
                            'Number of Shifts': TotalRows})

        return resultTable

    except Exception as ex:
        print(ex)


def getDWDrShifts():
    strConn = getDWConnectionString()
    try:
        cnxn = pyodbc.connect(strConn)
        cursor = cnxn.cursor()
        sqlStr = "select DoctorName,sum(HoursWorked),count(*)" \
                 "from [dbo].[vETLDrShiftsCollecton] " \
                 "group by DoctorName order by DoctorName"
        cursor.execute(sqlStr)
        rows = cursor.fetchall()

        FactVisitsTable = []
        for row in rows:
            dicData = {'Doctor Name': row[0],
                       'Total Hours Worked': row[1],
                       'Number of Shifts': row[2]}
            FactVisitsTable.append(dicData)

        sqlStr = "select 'GRAND TOTAL', sum(HoursWorked),count(*) from [dbo].[vETLDrShiftsCollecton] "
        cursor.execute(sqlStr)
        rows = cursor.fetchall()
        for row in rows:
            dicData = {'Doctor Name': row[0],
                       'Total Hours Worked': row[1],
                       'Number of Shifts': row[2]}
            FactVisitsTable.append(dicData)

        return FactVisitsTable
    except Exception as ex:
        print(ex)


def createHTMLtable(tableList, caption, color):
    table: str = "<table style='border:1px solid black; background-color:" + color + "'>\n"
    table += "<caption style='font-weight: bold; font-size: 20px;' >" + caption + "</caption>\n"
    table += '<tr>\n'
    for k in tableList[0].keys():
        table += '<th>' + k + '</th>'
    table += '</tr>\n'

    table += "  <tr>\n"
    for row in tableList:
        for k in row.keys():
            table += '<td>' + str(row[k]) + '</td>\n'
        table += '</tr>\n'

    table += '\t</table>\n'
    return table


def CreateReport():
    strTitle = "ETL Process Page"
    # Start the page
    strContent = '''
          <html>
            <head>
              <title>''' + strTitle + '''</title> 
            </head>  
            <body>\n'''

    # Add content to the body
    strContent += "<p><div>"
    strContent += "<div style='float:left; width:50%;'>"
    strContent += createHTMLtable(getMongoDBVisits(), 'MongoDB - Visits Data', '#FADBD8')
    strContent += '</div>'
    strContent += "<div style='float:left; width:50%;'>"
    strContent += createHTMLtable(getDWVisits(), 'DataWarehouse - Visits Data', '#FADBD8')
    strContent += '</div>'
    strContent += '</div>'
    strContent += '</p>'

    strContent += '<p><div>'
    strContent += "<div style='float:left; width:50%;'>"
    strContent += createHTMLtable(getMongoDBDrShifts(), 'MongoDB - Doctor Shifts Data', '#D4E6F1')
    strContent += '</div>'
    strContent += "<div style='float:left; width:50%;'>"
    strContent += createHTMLtable(getDWDrShifts(), 'DataWarehouse - Doctor Shifts Data', '#D4E6F1')
    strContent += '</div>'
    strContent += '</div>'
    strContent += '</p>'
    strContent += "<p>\t<table style='border:1px solid black; background-color:#E5E7E9;'>\n"
    strContent += "\t\t<tr><th>ETL Process</th><th>DateTime</th><th>Status</th></tr>\n"
    strContent += '\t\t<tr><td>Convert To HTML</td><td>' + str(dt.now()) + '</td><td>Success</td></tr>\n'
    strContent += '\t</table></p>\n'

    # Close the body and end the file
    strContent += '''    </body>
      </html>
      '''

    # Save the HTML code
    objFile = open('../Reports/ETLReport.html', 'w')
    objFile.write(strContent)
    objFile.close()


CreateReport()
