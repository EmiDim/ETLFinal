# -*- coding: utf-8 -*-
# *************************************************************************
# Desc: Script for export report data to CSV file
# Change Log: When,Who,What
# 2020-06-18, Emilija Dimikj, Created file
# *************************************************************************
import pymongo
import csv

strCon = "mongodb+srv://admin:Bothell2018@cluster0-yogcb.azure.mongodb.net/test?retryWrites=true&w=majority"

objCon = pymongo.MongoClient(strCon)
db = objCon['ClinicReports']
objCol = db['Visits']
# Verify Data Was Uploaded
curData = objCol.find()

with open('..\DataFiles\MongoDBVisitsReportData.csv','w', newline='') as csvFile:
    fieldnames = ["VisitDate", "VisitClinicName", "VisitDoctorFullName", "VisitProcedureName", "VisitCharge"]
    writer = csv.DictWriter(csvFile,fieldnames=fieldnames)
    writer.writeheader()
    for val in curData:
        writer.writerow({
            "VisitDate": val["VisitDate"],
            "VisitClinicName": val["VisitClinicName"],
            "VisitDoctorFullName": val["VisitDoctorFullName"],
            "VisitProcedureName": val["VisitProcedureName"],
            "VisitCharge": val["VisitCharge"]
        })
