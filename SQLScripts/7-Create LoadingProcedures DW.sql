/***************************************************************************
ETL Final Project: DWClinicReportDataEDimikj
Dev: Emilija Dimikj
Date:6/7/2020
Desc: Script for Creating ETL Stored Procedures.
ChangeLog: (Who, When, What) 
	EDimikj, 6/12/20, excluded matching of dummy row from dimenson ETL procedures 
*****************************************************************************************/

Use DWClinicReportDataEDimikj;
go

DROP PROCEDURE IF EXISTS [dbo].[pETLFillDimPatients];
GO
CREATE PROCEDURE [dbo].[pETLFillDimPatients]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO DimPatients WITH (SERIALIZABLE) AS TargetTable
			USING vETLDimPatients AS SourceTable
				ON SourceTable.PatientID=TargetTable.PatientID
					AND TargetTable.IsCurrent=1 AND TargetTable.PatientID<>0
			WHEN NOT MATCHED THEN
				INSERT (PatientID, PatientFullName, PatientCity, PatientState, PatientZipCode,
						StartDate, IsCurrent)
				VALUES(SourceTable.PatientID, SourceTable.PatientFullName, SourceTable.PatientCity, SourceTable.PatientState, SourceTable.PatientZipCode,
						CAST(0 AS datetime),1)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.PatientID, SourceTable.PatientFullName,
						SourceTable.PatientCity, SourceTable.PatientState,
						SourceTable.PatientZipCode
				EXCEPT
				SELECT TargetTable.PatientID, TargetTable.PatientFullName,
						TargetTable.PatientCity, TargetTable.PatientState,
						TargetTable.PatientZipCode
					WHERE IsCurrent=1 AND TargetTable.PatientID<>0) THEN
				update set TargetTable.EndDate=GetDate(), TargetTable.IsCurrent = 0
			WHEN NOT MATCHED BY SOURCE AND TargetTable.IsCurrent=1 AND TargetTable.PatientID<>0 THEN
				UPDATE SET TargetTable.EndDate=GetDate(), TargetTable.IsCurrent = 0;

			INSERT INTO DimPatients(PatientID, PatientFullName, PatientCity, PatientState, PatientZipCode, StartDate, IsCurrent)
			SELECT PatientID, PatientFullName, PatientCity, PatientState, PatientZipCode, GETDATE(),1
				FROM vETLDimPatients
			EXCEPT
			SELECT PatientID, PatientFullName, PatientCity, PatientState, PatientZipCode, GETDATE(),1
				FROM DimPatients 
			WHERE IsCurrent=1;

			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillDimProcedures];
GO
CREATE PROCEDURE [dbo].[pETLFillDimProcedures]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO DimProcedures AS TargetTable
			USING vETLDimProcedures AS SourceTable
				ON SourceTable.ProcedureID=TargetTable.ProcedureID
			WHEN NOT MATCHED THEN
				INSERT (ProcedureID, ProcedureName, ProcedureDesc, ProcedureCharge)
				VALUES(SourceTable.ProcedureID, SourceTable.ProcedureName, SourceTable.ProcedureDesc, SourceTable.ProcedureCharge)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.ProcedureID, SourceTable.ProcedureName, SourceTable.ProcedureDesc, SourceTable.ProcedureCharge
				EXCEPT 
				SELECT TargetTable.ProcedureID, TargetTable.ProcedureName, TargetTable.ProcedureDesc, TargetTable.ProcedureCharge
				WHERE TargetTable.ProcedureID<>0
					) THEN
				UPDATE SET TargetTable.ProcedureID=SourceTable.ProcedureID, 
							TargetTable.ProcedureName=SourceTable.ProcedureName, 
							TargetTable.ProcedureDesc=SourceTable.ProcedureDesc, 
							TargetTable.ProcedureCharge=SourceTable.ProcedureCharge
			WHEN NOT MATCHED BY SOURCE AND TargetTable.ProcedureID<>0 THEN
				DELETE;
			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillDimShifts];
GO
CREATE PROCEDURE [dbo].[pETLFillDimShifts]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO DimShifts AS TargetTable
			USING vETLDimShifts AS SourceTable
				ON SourceTable.ShiftID=TargetTable.ShiftID
			WHEN NOT MATCHED THEN
				INSERT (ShiftID, ShiftStart, ShiftEnd)
				VALUES(SourceTable.ShiftID, SourceTable.ShiftStart, SourceTable.ShiftEnd)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.ShiftID, SourceTable.ShiftStart, SourceTable.ShiftEnd
				EXCEPT 
				SELECT TargetTable.ShiftID, TargetTable.ShiftStart, TargetTable.ShiftEnd
				WHERE TargetTable.ShiftID<>0
					) THEN
				UPDATE SET TargetTable.ShiftID=SourceTable.ShiftID, 
							TargetTable.ShiftStart=SourceTable.ShiftStart, 
							TargetTable.ShiftEnd=SourceTable.ShiftEnd
			WHEN NOT MATCHED BY SOURCE AND TargetTable.ShiftID<>0 THEN
				DELETE;
			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillDimClinics];
GO
CREATE PROCEDURE [dbo].[pETLFillDimClinics]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO DimClinics AS TargetTable
			USING vETLDimClinics AS SourceTable
				ON SourceTable.ClinicID=TargetTable.ClinicID 
			WHEN NOT MATCHED THEN
				INSERT (ClinicID, ClinicName, ClinicCity, ClinicState, ClinicZip)
				VALUES(SourceTable.ClinicID, SourceTable.ClinicName, SourceTable.ClinicCity, SourceTable.ClinicState, SourceTable.ClinicZip)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.ClinicID, SourceTable.ClinicName, SourceTable.ClinicCity, SourceTable.ClinicState, SourceTable.ClinicZip
					EXCEPT 
				SELECT TargetTable.ClinicID, TargetTable.ClinicName, TargetTable.ClinicCity, TargetTable.ClinicState, TargetTable.ClinicZip
				WHERE TargetTable.ClinicID<>0
					) THEN
				UPDATE SET TargetTable.ClinicID=SourceTable.ClinicID, 
							TargetTable.ClinicName=SourceTable.ClinicName, 
							TargetTable.ClinicCity=SourceTable.ClinicCity, 
							TargetTable.ClinicState=SourceTable.ClinicState,
							TargetTable.ClinicZip=SourceTable.ClinicZip
			WHEN NOT MATCHED BY SOURCE AND TargetTable.ClinicID<>0 THEN
				DELETE;
			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillDimDoctors];
GO
CREATE PROCEDURE [dbo].[pETLFillDimDoctors]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO DimDoctors AS TargetTable
			USING vETLDimDoctors AS SourceTable
				ON SourceTable.DoctorID=TargetTable.DoctorID 
			WHEN NOT MATCHED THEN
				INSERT (DoctorID, DoctorFullName, DoctorEmailAddress, 
					DoctorCity, DoctorState, DoctorZip)
				VALUES(SourceTable.DoctorID, SourceTable.DoctorFullName, SourceTable.DoctorEmailAddress,
					SourceTable.DoctorCity, SourceTable.DoctorState, SourceTable.DoctorZip)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.DoctorID, SourceTable.DoctorFullName, SourceTable.DoctorEmailAddress,
					SourceTable.DoctorCity, SourceTable.DoctorState, SourceTable.DoctorZip
					EXCEPT 
				SELECT TargetTable.DoctorID, TargetTable.DoctorFullName, TargetTable.DoctorEmailAddress,
					TargetTable.DoctorCity, TargetTable.DoctorState, TargetTable.DoctorZip
				WHERE TargetTable.DoctorID<>0
					) THEN
				UPDATE SET TargetTable.DoctorID=SourceTable.DoctorID, 
							TargetTable.DoctorFullName=SourceTable.DoctorFullName, 
							TargetTable.DoctorEmailAddress=SourceTable.DoctorEmailAddress,
							TargetTable.DoctorCity=SourceTable.DoctorCity, 
							TargetTable.DoctorState=SourceTable.DoctorState,
							TargetTable.DoctorZip=SourceTable.DoctorZip
			WHEN NOT MATCHED BY SOURCE AND TargetTable.DoctorID<>0 THEN
				DELETE;
			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillDimDates];
GO
CREATE PROCEDURE [dbo].[pETLFillDimDates]
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	declare @dummyDate datetime=CAST(0 AS datetime)
	if ((select count(*) from DimDates where FullDate=@dummyDate) = 0)
	begin
		insert into DimDates([FullDate], [FullDateName]
			, [MonthID], [MonthName]
			, [YearID], [YearName])
		   Values (
			 @dummyDate, DateName(weekday, @dummyDate) + ', ' + Convert(nVarchar(50), @dummyDate, 110) 
			, Cast(Left(Convert(nVarchar(50), @dummyDate, 112), 6) as int), DateName(month, @dummyDate) + ' - ' + DateName(YYYY,@dummyDate)
			, Year(@dummyDate), Cast(Year(@dummyDate) as nVarchar(50)))  
	 end

	 -- Process Dates
	 Declare @StartDate datetime;
     Declare @EndDate datetime;
	 Declare @LastDimDate datetime;

	 select @StartDate=min(Cast([Date] as date)), @EndDate=max(Cast([Date] as date)) from [Patients].dbo.[Visits] 
     select @LastDimDate=isnull(max([FullDate]),@StartDate) from [dbo].[DimDates] where [FullDate]<>@dummyDate
	
	 set @StartDate=iif(@LastDimDate<@EndDate and @LastDimDate<>@StartDate,dateadd(d,1,@LastDimDate),@StartDate);
	 Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
     While @DateInProcess <= @EndDate and @LastDimDate<@EndDate
		Begin
			Insert Into DimDates(
			  [FullDate]
			, [FullDateName]
			, [MonthID]
			, [MonthName]
			, [YearID]
			, [YearName])
		   Values (
			  @DateInProcess
			, DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) 
			, Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  
			, DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess)
			, Year(@DateInProcess)
			, Cast(Year(@DateInProcess ) as nVarchar(50)))  
		   Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
		End 
	Set @RC = +1
  End Try
  Begin Catch
	Set @RC = -1
  End Catch
  Return @RC;
 End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillFactVisits];
GO
CREATE PROCEDURE [dbo].[pETLFillFactVisits]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO [dbo].FactVisits AS TargetTable
			USING [dbo].vETLFactVisits AS SourceTable
				ON SourceTable.DateKey=TargetTable.DateKey
				AND SourceTable.ClinicKey=TargetTable.ClinicKey
				AND SourceTable.PatientKey=TargetTable.PatientKey
				AND SourceTable.DoctorKey=TargetTable.DoctorKey
				AND SourceTable.ProcedureKey=TargetTable.ProcedureKey
			WHEN NOT MATCHED THEN
				INSERT (VisitKey, DateKey, ClinicKey,
						PatientKey, DoctorKey, ProcedureKey, ProcedureVistCharge)
				VALUES(SourceTable.VisitKey, SourceTable.DateKey, SourceTable.ClinicKey,
						SourceTable.PatientKey, SourceTable.DoctorKey, SourceTable.ProcedureKey, SourceTable.ProcedureVisitCharge)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.VisitKey, SourceTable.DateKey, SourceTable.ClinicKey,
						SourceTable.PatientKey, SourceTable.DoctorKey, SourceTable.ProcedureKey, SourceTable.ProcedureVisitCharge
				EXCEPT 
				SELECT TargetTable.VisitKey, TargetTable.DateKey, TargetTable.ClinicKey,
						TargetTable.PatientKey, TargetTable.DoctorKey, TargetTable.ProcedureKey, TargetTable.ProcedureVistCharge
					) THEN
				UPDATE SET TargetTable.VisitKey=SourceTable.VisitKey, 
							TargetTable.DateKey=SourceTable.DateKey, 
							TargetTable.ClinicKey=SourceTable.ClinicKey,
							TargetTable.PatientKey=SourceTable.PatientKey,
							TargetTable.DoctorKey=SourceTable.DoctorKey,
							TargetTable.ProcedureKey=SourceTable.ProcedureKey,
							TargetTable.ProcedureVistCharge=SourceTable.ProcedureVisitCharge
			WHEN NOT MATCHED BY SOURCE THEN
				DELETE;
			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO

DROP PROCEDURE IF EXISTS [dbo].[pETLFillFactDoctorShifts];
GO
CREATE PROCEDURE [dbo].[pETLFillFactDoctorShifts]
AS
	Begin
		Declare @RC int = 0;
		Begin Try
			MERGE INTO [dbo].FactDoctorShifts AS TargetTable
			USING [dbo].vETLFactDoctorShifts AS SourceTable
				ON SourceTable.ShiftDateKey=TargetTable.ShiftDateKey
					AND SourceTable.ClinicKey=TargetTable.ClinicKey
					AND SourceTable.ShiftKey=TargetTable.ShiftKey
					AND SourceTable.DoctorKey=TargetTable.DoctorKey
			WHEN NOT MATCHED THEN
				INSERT (DoctorsShiftID, ShiftDateKey, ClinicKey,
						ShiftKey, DoctorKey, HoursWorked)
				VALUES(SourceTable.DoctorsShiftID, SourceTable.ShiftDateKey, SourceTable.ClinicKey,
						SourceTable.ShiftKey, SourceTable.DoctorKey, SourceTable.HoursWorked)
			WHEN MATCHED AND EXISTS (
				SELECT SourceTable.DoctorsShiftID, SourceTable.ShiftDateKey, SourceTable.ClinicKey,
						SourceTable.ShiftKey, SourceTable.DoctorKey, SourceTable.HoursWorked
				EXCEPT 
				SELECT TargetTable.DoctorsShiftID, TargetTable.ShiftDateKey, TargetTable.ClinicKey,
						TargetTable.ShiftKey, TargetTable.DoctorKey, TargetTable.HoursWorked
					) THEN
				UPDATE SET TargetTable.DoctorsShiftID=SourceTable.DoctorsShiftID, 
							TargetTable.ShiftDateKey=SourceTable.ShiftDateKey, 
							TargetTable.ClinicKey=SourceTable.ClinicKey,
							TargetTable.ShiftKey=SourceTable.ShiftKey,
							TargetTable.DoctorKey=SourceTable.DoctorKey,
							TargetTable.HoursWorked=SourceTable.HoursWorked
			WHEN NOT MATCHED BY SOURCE THEN
				DELETE;
			Set @RC = +1
		End Try
		Begin Catch
			Set @RC = -1
		End Catch
		Return @RC;
	End
GO