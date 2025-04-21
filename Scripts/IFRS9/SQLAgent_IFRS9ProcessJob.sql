USE [msdb]
GO

-- Check if job already exists and drop it
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'FinancialRiskDB_IFRS9_Process')
BEGIN
	EXEC msdb.dbo.sp_delete_job @job_name = N'FinancialRiskDB_IFRS9_Process'
END
GO

-- Create job
BEGIN TRANSACTION
DECLARE @ReturnCode INT = 0

-- Create the job
EXEC @ReturnCode = msdb.dbo.sp_add_job 
	@job_name = N'FinancialRiskDB_IFRS9_Process',
	@enabled = 1,
	@description = N'IFRS9 Compliance Process - Expected Credit Loss and Reporting',
	@category_name = N'[Uncategorized (Local)]',
	@owner_login_name = N'sa',
	@notify_email_operator_name = N'DBA',
	@notify_level_eventlog = 2,
	@notify_level_email = 2,
	@notify_level_netsend = 0,
	@notify_level_page = 0,
	@delete_level = 0

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Add job step for IFRS9 processing
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
	@job_name = N'FinancialRiskDB_IFRS9_Process',
	@step_name = N'Execute IFRS9 Process',
	@step_id = 1,
	@cmdexec_success_code = 0,
	@on_success_action = 3, -- Go to next step
	@on_success_step_id = 0,
	@on_fail_action = 2, -- Quit with failure
	@on_fail_step_id = 0,
	@retry_attempts = 1,
	@retry_interval = 5, -- 5 minutes
	@os_run_priority = 0,
	@subsystem = N'TSQL',
	@command = N'
	DECLARE @AsOfDate DATE = CAST(GETDATE() AS DATE);
	DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
	
	-- Execute the IFRS 9 process
	EXEC IFRS9.usp_ExecuteProcessIFRS9
		@AsOfDate = @AsOfDate,
		@BatchID = @BatchID,
		@IncludeAllScenarios = 1,
		@CalculateMovement = 1;
	
	-- Log job execution
	INSERT INTO Production.ETLLog (ProcedureName, StepName, Status, Message)
	VALUES (''SQLAgent_IFRS9Job'', ''Execute IFRS9 Process'', ''Success'', 
		''IFRS9 process executed via SQL Agent job. BatchID: '' + CAST(@BatchID AS VARCHAR(50)));
	',
	@database_name = N'FinancialRiskDB',
	@flags = 0

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Add job step for refreshing reporting views
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
	@job_name = N'FinancialRiskDB_IFRS9_Process',
	@step_name = N'Refresh IFRS9 Reporting Views',
	@step_id = 2,
	@cmdexec_success_code = 0,
	@on_success_action = 1, -- Quit with success
	@on_success_step_id = 0,
	@on_fail_action = 2, -- Quit with failure
	@on_fail_step_id = 0,
	@retry_attempts = 1,
	@retry_interval = 1, -- 1 minute
	@os_run_priority = 0,
	@subsystem = N'TSQL',
	@command = N'
	-- Refresh IFRS9 reporting views
	EXEC IFRS9.usp_RefreshReportingViews;
	
	-- Log job execution
	INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
	VALUES (''SQLAgent_IFRS9Job'', ''Refresh IFRS9 Reporting Views'', ''Success'');
	',
	@database_name = N'FinancialRiskDB',
	@flags = 0

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Set job starting step
EXEC @ReturnCode = msdb.dbo.sp_update_job 
	@job_name = N'FinancialRiskDB_IFRS9_Process',
	@start_step_id = 1

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Create monthly schedule
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
	@job_name = N'FinancialRiskDB_IFRS9_Process',
	@name = N'IFRS9 Monthly Process',
	@enabled = 1,
	@freq_type = 16, -- Monthly
	@freq_interval = 28, -- 28th day of month
	@freq_subday_type = 1, -- At the specified time
	@freq_subday_interval = 0,
	@freq_relative_interval = 0,
	@freq_recurrence_factor = 1, -- Every month
	@active_start_date = 20200101,
	@active_end_date = 99991231,
	@active_start_time = 10000, -- 1:00 AM
	@active_end_time = 235959

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver 
	@job_name = N'FinancialRiskDB_IFRS9_Process',
	@server_name = N'(local)'

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

COMMIT TRANSACTION
GOTO EndSave

QuitWithRollback:
	IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO
