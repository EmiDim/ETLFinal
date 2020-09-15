import pyodbc
import pymongo
from bson.decimal128 import Decimal128
from datetime import datetime, date, time


def getDWConnectionString():
    server = 'is-root01.ischool.uw.edu\\BI'
    database = 'DWClinicReportDataEDimikj'
    username = 'UN**'
    password = 'PASS**'
    driver = "{ODBC Driver 17 for SQL Server}"
    strConn = 'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';' \
              'UID=' + username + ';PWD=' + password + ";"
    # print(strConn)
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


def VisitsColl():
    ConnStr = getDWConnectionString()
    cnxn = pyodbc.connect(ConnStr)
    cursor = cnxn.cursor()
    sqlStr = "select VisitKey,VisitDate,VisitClinicName,VisitPatientFullName,VisitDoctorFullName,VisitProcedureName," \
             "VisitProcedureDesc,VisitCharge from [dbo].[vETLVisitsCollection] "
    cursor.execute(sqlStr)
    rows = cursor.fetchall()

    t = time(0, 0)
    FactVisitsTable = []
    for row in rows:
        dicData = {'VisitKey': row[0],
                   'VisitDate': str(row[1]),
                   'VisitClinicName': row[2],
                   'VisitPatientFullName': row[3],
                   'VisitDoctorFullName': row[4],
                   'VisitProcedureName': row[5],
                   'VisitProcedureDesc': row[6],
                   'VisitCharge': Decimal128(row[7])}
        FactVisitsTable.append(dicData)
    print('FactVisits returned ' + str(len(FactVisitsTable)) + ' rows')

    return FactVisitsTable


def DrShiftsColl():
    ConnStr = getDWConnectionString()
    cnxn = pyodbc.connect(ConnStr)
    cursor = cnxn.cursor()
    sqlStr = "SELECT DoctorsShiftID,DoctorShiftDate,DoctorShiftClinic,ShiftStart,ShiftEnd,DoctorName,HoursWorked FROM " \
             "[dbo].[vETLDrShiftsCollecton]"
    cursor.execute(sqlStr)
    rows = cursor.fetchall()

    FactDrShiftsTable = []
    for row in rows:
        dicData = {'DoctorsShiftID': row[0],
                   'DoctorShiftDate': str(row[1]),
                   'DoctorShiftClinic': row[2],
                   'ShiftStart': str(row[3]),
                   'ShiftEnd': str(row[4]),
                   'DoctorName': row[5],
                   'HoursWorked': row[6]}
        FactDrShiftsTable.append(dicData)
    print('FactDrShifts returned ' + str(len(FactDrShiftsTable)) + ' rows')

    return FactDrShiftsTable


def Load2Mongo(dbName, collName, inTable):
    strConn = getMongoConnectionStr()
    try:
        objConn = pymongo.MongoClient(strConn)

        db = objConn[dbName]
        if db[collName].drop():
            print('Collection ' + collName + ' dropped')
        objColl = db[collName]
        objColl.insert_many(inTable)
        print('In ' + dbName + '.' + collName + ' inserted ' + str(len(inTable)) + ' docs.')
    except Exception as ex:
        print(ex)
        return str(ex)


def fillClinicReportsDatabase():
    try:
        dbName = 'ClinicReports'

        collName = 'Visits'
        VisitsTable = VisitsColl()
        Load2Mongo(dbName, collName, VisitsTable)

        collName = 'DrShifts'
        DrShiftsTable = DrShiftsColl()
        Load2Mongo(dbName, collName, DrShiftsTable)

    except Exception as ex:
        print(ex)


fillClinicReportsDatabase()
