/* 
This script sets up SQL Agent jobs to run the ETL process 
*/

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

USE msdb;
GO


-- Validate that the variables are set
IF '$(DataPath)' = 'UNDEFINED' OR '$(LogPath)' = 'UNDEFINED'
BEGIN
    RAISERROR('Required SQLCMD variables (DataPath, LogPath) not set.', 16, 1)
    SET NOEXEC ON
END
GO

-- Set the configuration values from the variables passed by PowerShell
EXEC [$(Database)].Config.usp_SetConfigValue 
	@ConfigKey = 'Database', 
	@ConfigValue = '$(Database)',
	@Description = 'Database name';

EXEC [$(Database)].Config.usp_SetConfigValue 
    @ConfigKey = 'DataPath', 
    @ConfigValue = '$(DataPath)',
	@Description = 'Data path';
    
EXEC [$(Database)].Config.usp_SetConfigValue 
    @ConfigKey = 'LogPath', 
    @ConfigValue = '$(LogPath)',
	@Description = 'Log path';

-- Create SQL Agent job
BEGIN TRANSACTION
    DECLARE @ReturnCode INT = 0
    DECLARE @JobName NVARCHAR(128) = '$(Database) - Staging ETL'
    
    -- Delete job if it exists
    IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = @JobName)
    BEGIN
        EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 1
    END
    
    
	-- Get operator name from configuration
	DECLARE @OperatorName NVARCHAR(128);
	EXEC [$(Database)].Config.usp_GetConfigValue 
		@ConfigKey = 'NotificationOperator', 
		@ConfigValue = @OperatorName OUTPUT;

	IF @OperatorName IS NULL OR LTRIM(RTRIM(@OperatorName)) = ''
	BEGIN
		RAISERROR('Notification operator not configured. Please run Config.usp_SetConfigValue to set NotificationOperator', 16, 1)
		SET NOEXEC ON
	END

	-- Create the job
    EXEC @ReturnCode = msdb.dbo.sp_add_job 
        @job_name = @JobName,
        @description = 'Loads financial risk data into staging tables', 

        @owner_login_name = N'sa',
        @enabled = 1,
        @notify_level_eventlog = 2,
        @notify_level_email = 2,
        @notify_email_operator_name = @OperatorName
        
    -- Add job step
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
        @job_name = @JobName,
        @step_name = N'Execute ETL',
        @subsystem = N'TSQL',
        @command = N'
        -- Get configuration values
        DECLARE @DataPath NVARCHAR(500), @LogPath NVARCHAR(500);
        
        EXEC [$(Database)].Config.usp_GetConfigValue 
            @ConfigKey = ''DataPath'', 
            @ConfigValue = @DataPath OUTPUT;
            
        EXEC [$(Database)].Config.usp_GetConfigValue 
            @ConfigKey = ''LogPath'', 
            @ConfigValue = @LogPath OUTPUT;

		-- Escape any potential special characters in paths
		SET @DataPath = REPLACE(@DataPath, '''', '''''');
		SET @LogPath = REPLACE(@LogPath, '''', '''''');
        
        -- Execute ETL with configuration values
        EXEC [$(Database)].Staging.usp_ExecuteETL 
            @DataPath = @DataPath, 
            @LogPath = @LogPath, 
            @ValidateAfterLoad = 1;',
        @database_name = N'$(Database)',
        @retry_attempts = 3,
        @retry_interval = 5
        
    -- Add schedule (daily at 1 AM)
    DECLARE @ScheduleName NVARCHAR(128) = 'Daily 1AM'
    
    -- Create schedule if it doesn't exist
    IF NOT EXISTS (SELECT name FROM msdb.dbo.sysschedules WHERE name = @ScheduleName)
    BEGIN
        EXEC @ReturnCode = msdb.dbo.sp_add_schedule 
            @schedule_name = @ScheduleName,
            @freq_type = 4, -- Daily
            @freq_interval = 1,
            @active_start_time = 010000 -- 1:00 AM
    END
        
    -- Attach schedule to job
    EXEC @ReturnCode = msdb.dbo.sp_attach_schedule 
        @job_name = @JobName,
        @schedule_name = @ScheduleName
        
    -- Add the job server
    EXEC @ReturnCode = msdb.dbo.sp_add_jobserver 
        @job_name = @JobName
        
IF (@@ERROR <> 0 OR @ReturnCode <> 0) ROLLBACK TRANSACTION
COMMIT TRANSACTION
GO

USE [$(Database)];
GO