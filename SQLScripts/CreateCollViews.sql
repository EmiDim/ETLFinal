/***************************************************************************
ETL Final Project: DWClinicReportDataEDimikj
Dev: Emilija Dimikj
Date:6/12/2020
Desc: Scripts for Creating ETL Views for Extract to DocumentDB.
ChangeLog: (Who, When, What) 
	
*****************************************************************************************/

Use DWClinicReportDataEDimikj;
go

--/****** [dbo].[FactVisits] ******/
DROP VIEW IF EXISTS [dbo].[vETLVisitsCollection];
GO
CREATE VIEW vETLVisitsCollection
AS
	SELECT FV.VisitKey
	  ,[VisitDate]=Cast(DD.FullDate as date)
      ,[VisitClinicName]=DC.ClinicName
      ,[VisitPatientFullName]=DP.PatientFullName
      ,[VisitDoctorFullName]=DDr.DoctorFullName
      ,[VisitProcedureName]=DPr.ProcedureName
	  ,[VisitProcedureDesc]=DPr.ProcedureDesc
      ,[VisitCharge]=FV.ProcedureVistCharge
  FROM [dbo].[FactVisits] as FV
  INNER JOIN [dbo].[DimDates] AS DD ON FV.DateKey=DD.DateKey
  INNER JOIN [dbo].[DimClinics] AS DC ON FV.ClinicKey=DC.ClinicKey
  INNER JOIN [dbo].[DimPatients] AS DP ON FV.PatientKey=DP.PatientKey
	AND ((DP.IsCurrent=1 AND DD.FullDate>=DP.StartDate) OR (DP.IsCurrent=0 AND DD.FullDate between DP.StartDate and DP.EndDate))
  INNER JOIN [dbo].[DimDoctors] AS DDr ON FV.DoctorKey=DDr.DoctorKey
  INNER JOIN [dbo].[DimProcedures] AS DPr ON FV.ProcedureKey=DPr.ProcedureKey
GO

--/****** [dbo].[FactDoctorShifts] ******/
DROP VIEW IF EXISTS [dbo].[vETLDrShiftsCollecton];
GO
CREATE VIEW [dbo].[vETLDrShiftsCollecton]
AS
    SELECT FDrs.DoctorsShiftID
          ,[DoctorShiftDate]=Cast(DD.FullDate as date)
          ,[DoctorShiftClinic]=DC.ClinicName
          ,DS.[ShiftStart], DS.[ShiftEnd]
          ,[DoctorName]=DDr.DoctorFullName
          ,FDrs.HoursWorked
      FROM [dbo].[FactDoctorShifts] AS FDrS
	    INNER JOIN [dbo].[DimDates] AS DD ON FDrS.ShiftDateKey=DD.DateKey 
	    INNER JOIN [dbo].[DimClinics] as DC ON FDrS.ClinicKey=DC.ClinicKey
	    INNER JOIN [dbo].[DimShifts] as DS ON FDrS.ShiftKey=DS.ShiftKey
	    INNER JOIN [dbo].DimDoctors as DDr ON FDrS.DoctorKey=DDr.DoctorKey
GO