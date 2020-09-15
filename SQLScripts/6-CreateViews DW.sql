/***************************************************************************
ETL Final Project: DWClinicReportDataEDimikj
Dev: Emilija Dimikj
Date:6/7/2020
Desc: Script for Creating ETL Views.
ChangeLog: (Who, When, What) 
	EDimikj, 6/12/20, removed dummy row from dimenson ETL views, changed null handling in fact views
	EDimikj, 6/12/20, duplicates removed in vETLFactDoctorShifts, bug fixed in vETLFactVisits
*****************************************************************************************/

Use DWClinicReportDataEDimikj;
go

/****** [dbo].[DimPatients] ******/
DROP VIEW IF EXISTS [dbo].[vETLDimPatients];
GO
CREATE VIEW vETLDimPatients
AS
	select [PatientID]=[ID] 
		,[PatientFullName]=Cast(CONCAT(isnull(FName,'NA'),' ',isnull(LName,'NA')) as varchar(100))
		,[PatientCity]=Cast(isnull([City],'NA') as varchar(100))
		,[PatientState]=Cast(isnull([State],'NA') as varchar(100))
		,[PatientZipCode]=isnull([ZipCode],0)
	from [Patients].[dbo].[Patients]
GO	

/****** [dbo].[DimProcedures] ******/
DROP VIEW IF EXISTS [dbo].[vETLDimProcedures];
GO
CREATE VIEW vETLDimProcedures
AS
	select [ProcedureID]=ID
		,[ProcedureName]=Cast(isnull([Name],'NA') as varchar(100))
		,[ProcedureDesc]=Cast(REPLACE(isnull([Desc],'NA'),'"','') as varchar(1000))
		,[ProcedureCharge]=Cast(isnull([Charge],0.00) as money)
	from [Patients].[dbo].[Procedures]
GO

/****** [dbo].[DimShifts] ******/
DROP VIEW IF EXISTS [dbo].[vETLDimShifts];
GO
CREATE VIEW vETLDimShifts
AS
	select [ShiftID]
		, [ShiftStart]=iif(DATEDIFF(hh,isnull([ShiftStart],'00:00:00'),isnull([ShiftEnd],'00:00:00'))>0 and DATEPART(hh,isnull([ShiftStart],'00:00:00')) in (1,2,3,4,5,6,7),
				DATEADD(hh,12,isnull([ShiftStart],'00:00:00')),isnull([ShiftStart],'00:00:00'))
		, [ShiftEnd]=iif(DATEDIFF(hh,isnull([ShiftStart],'00:00:00'),isnull([ShiftEnd],'00:00:00'))<0 and DATEPART(hh,isnull([ShiftEnd],'00:00:00')) in (1,2,3,4,5,6,7),
				DATEADD(hh,12,isnull([ShiftEnd],'00:00:00')),isnull([ShiftEnd],'00:00:00'))
	from [DimShiftsStaging]
go

--/****** [dbo].[DimClinics] ******/
DROP VIEW IF EXISTS [dbo].[vETLDimClinics];
GO
CREATE VIEW vETLDimClinics
AS
	select [ClinicID]=ClinicID
		,[ClinicName]=cast(isnull([ClinicName],'NA') as nvarchar(100))
		,[ClinicCity]=cast(isnull([City],'NA') as nvarchar(100))
		,[ClinicState]=cast(isnull([State],'NA') as nvarchar(100))
		,[ClinicZip]=cast(isnull([Zip],'NA') as nvarchar(5))
	from [dbo].[DimClinicsStaging]
GO

/****** [dbo].[DimDoctors] ******/
DROP VIEW IF EXISTS [dbo].[vETLDimDoctors];
GO
CREATE VIEW vETLDimDoctors
AS
	select [DoctorID]=DoctorID
		, [DoctorFullName]=Cast(CONCAT(isnull([FirstName],'NA'),' ',isnull([LastName],'NA')) as nvarchar(200))
		, [DoctorEmailAddress]=cast(isnull([EmailAddress],'NA') as nvarchar(100))
		, [DoctorCity]=cast(iif(len(Ltrim(RTrim(REPLACE([State],char(9),' '))))>2, LEFT(Ltrim(RTrim(REPLACE([State],char(9),' '))),len(Ltrim(RTrim(REPLACE([State],char(9),' '))))-2),ltrim(rtrim(replace([City],char(9),' ')))) as nvarchar(100))
		, [DoctorState]=cast(iif(len(Ltrim(RTrim(REPLACE([State],char(9),' '))))>2,Right(Ltrim(RTrim(REPLACE([State],char(9),' '))),2),Ltrim(RTrim(REPLACE([State],char(9),' ')))) as nvarchar(100))
		, [DoctorZip]=cast(rtrim(ltrim(isnull([Zip],'NA'))) as nvarchar(5))
	from [dbo].[DimDoctorsStaging]
GO

/****** [dbo].[FactVisits] ******/
DROP VIEW IF EXISTS [dbo].[vETLFactVisits];
GO
CREATE VIEW vETLFactVisits
AS
	WITH VisitsCl AS (
		select [VisitKey]=V.ID
			, [FullDateTime]=isnull(V.Date,CAST(0 AS datetime))
			, [ClinicKey]=DC.ClinicKey
			, [PatientKey]=DP.PatientKey
			, [DoctorKey]=D.DoctorKey
			, [ProcedureKey]=P.ProcedureKey
			, [ProcedureVisitCharge]=isnull(V.Charge,0.00)
			, [DuplicateCount]=ROW_NUMBER() OVER (PARTITION BY V.Date,DC.ClinicKey,DP.PatientKey,D.DoctorKey,P.ProcedureKey,V.Charge ORDER BY DateKey)
		from [Patients].[dbo].[Visits] as V
			inner join [dbo].DimDates as DD on Cast(isnull(V.Date,CAST(0 AS datetime)) as date)=DD.FullDate
			inner join [dbo].DimClinics as DC on iif(isnull(V.Clinic,0)>=100,(V.Clinic/100),-1)=DC.ClinicID
			inner join [dbo].DimPatients as DP on isnull(V.Patient,0)=DP.PatientID
				AND ((DP.IsCurrent=1 AND DD.FullDate>=DP.StartDate) OR (DP.IsCurrent=0 AND DD.FullDate between DP.StartDate and DP.EndDate))
			inner join [dbo].DimDoctors as D on isnull(V.Doctor,0)=D.DoctorID
			inner join DimProcedures as P on isnull(V.[Procedure],0)=P.ProcedureID
	)
	select V.VisitKey, DD.DateKey, V.ClinicKey, V.PatientKey, V.DoctorKey, V.ProcedureKey, v.ProcedureVisitCharge
	from VisitsCl as V
		inner join [dbo].DimDates as DD on Cast(V.FullDateTime as date)=DD.FullDate
	where V.DuplicateCount=1
go

--/****** [dbo].[FactDoctorShifts] ******/
DROP VIEW IF EXISTS [dbo].[vETLFactDoctorShifts];
GO
CREATE VIEW vETLFactDoctorShifts
AS
	WITH DoctorsShift AS (
		select [DoctorsShiftID]=DrSh.DoctorsShiftID
			, [ShiftDateKey]=DD.DateKey
			, [ClinicKey]=C.ClinicKey
			, [ShiftKey]=DS.ShiftKey
			, [DoctorKey]=D.DoctorKey
			, [HoursWorked]= iif(DATEDIFF(hh,[ShiftStart],[ShiftEnd])<0, 
								24-abs(DATEDIFF(hh,[ShiftStart],[ShiftEnd])),
								DATEDIFF(hh,[ShiftStart],[ShiftEnd]))
			, [DuplicateCount]=ROW_NUMBER() OVER (PARTITION BY DD.DateKey,C.ClinicKey,DS.ShiftKey,D.DoctorKey ORDER BY DateKey) 
		from [dbo].[FactDoctorShiftsStaging] as DrSh
			inner join [dbo].DimDates as DD on Cast(isnull(DrSh.ShiftDate,CAST(0 AS datetime)) as date)=DD.FullDate
			inner join [dbo].DimClinics as C on isnull(DrSh.ClinicID,0)=C.ClinicID
			inner join [dbo].DimDoctors as D on isnull(DrSh.DoctorID,0)=D.DoctorID
			inner join [dbo].DimShifts as DS on isnull(DrSh.ShiftID,0)=DS.ShiftID
		)
	SELECT [DoctorsShiftID],[ShiftDateKey],[ClinicKey],[ShiftKey],[DoctorKey],[HoursWorked]
	FROM DoctorsShift
	WHERE DuplicateCount=1
go
