/***************************************************************************
ETL Final Project: DWClinicReportDataEDimikj
Dev: Emilija Dimikj
Date:6/12/2020
Desc: Script for inserting a dummy row with ID=0 in each Dimenson Table for handling NULLs.
ChangeLog: (Who, When, What) 
*****************************************************************************************/

Use DWClinicReportDataEDimikj;
go

Declare @hasDummyRow int;
select @hasDummyRow=count(*) from [dbo].[DimDoctors] where [DoctorID]=0;
if @hasDummyRow=0
begin
	--insert Dummy Row for NULL handling
	Set Identity_Insert [dbo].[DimDoctors] On;
	Insert Into [dbo].[DimDoctors](
			[DoctorKey], [DoctorID], [DoctorFullName], 
			[DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
	Values(
			0, 0, 'Unknown Doctor', 
			'Unknown', 'Unknown', 'Unknown', '00000')
	Set Identity_Insert [dbo].[DimDoctors] Off;
end

select @hasDummyRow=count(*) from [dbo].[DimClinics] where [ClinicID]=0;
if @hasDummyRow=0
begin
	Set Identity_Insert [dbo].[DimClinics] On;
	Insert Into [dbo].[DimClinics](
			[ClinicKey], [ClinicID], [ClinicName],
			[ClinicCity], [ClinicState], [ClinicZip])
	Values(
			0, 0, 'Unknown Clinic', 
			'Unknown', 'Unknown', '00000')
	Set Identity_Insert [dbo].[DimClinics] Off;
end

select @hasDummyRow=count(*) from [dbo].[DimPatients] where [PatientID]=0;
if @hasDummyRow=0
begin
	Set Identity_Insert [dbo].[DimPatients] On;
	Insert Into [dbo].[DimPatients](
			[PatientKey],[PatientID],[PatientFullName],
			[PatientCity],[PatientState],[PatientZipCode],
			[StartDate],[EndDate],[IsCurrent])
	Values(
			0, 0, 'Unknown Patient', 
			'Unknown', 'Unknown', '00000',
			CAST(0 AS datetime),NULL,1)
	Set Identity_Insert [dbo].[DimPatients] Off;
end

select @hasDummyRow=count(*) from [dbo].[DimProcedures] where [ProcedureID]=0;
if @hasDummyRow=0
begin
	Set Identity_Insert [dbo].[DimProcedures] On;
	Insert Into [dbo].[DimProcedures](
			[ProcedureKey],[ProcedureID],[ProcedureName],
			[ProcedureDesc],[ProcedureCharge])
	Values(
			0, 0, 'Unknown Procedure', 
			'Unknown', 0.00)
	Set Identity_Insert [dbo].[DimProcedures] Off;
end

select @hasDummyRow=count(*) from [dbo].[DimShifts] where [ShiftID]=0;
if @hasDummyRow=0
begin
	Set Identity_Insert [dbo].[DimShifts] On;
	Insert Into [dbo].[DimShifts]([ShiftKey],[ShiftID],[ShiftStart],[ShiftEnd])
	Values(0, 0, '00:00:00','00:00:00')
	Set Identity_Insert [dbo].[DimShifts] Off;
end