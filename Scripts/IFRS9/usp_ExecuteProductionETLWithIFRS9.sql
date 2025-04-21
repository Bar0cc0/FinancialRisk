CREATE OR ALTER PROCEDURE Production.usp_ExecuteProductionETLWithIFRS9
	@Database NVARCHAR(128),
	@DataPath NVARCHAR(500),
	@LogPath NVARCHAR(500),
	@ValidateAfterLoad BIT = 1,
	@EnableMemoryOptimized BIT = 1,
	@EnableColumnstoreIndexes BIT = 1,
	@RebuildIndexes BIT = 0,
	@RunIFRS9 BIT = 1
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

	-- Variables for tracking execution
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @StepStartTime DATETIME;
	DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @CurrentStep NVARCHAR(100);
	DECLARE @RowCount INT = 0;
	DECLARE @BatchSize INT = 10000; 
	DECLARE @MaxID INT, @MinID INT, @CurrentID INT;
	DECLARE @ConfigValue NVARCHAR(500);
	DECLARE @Success BIT = 1;

	-- Set database context explicitly
	SET @Database = ISNULL(@Database, DB_NAME());
	DECLARE @SQL NVARCHAR(200) = N'USE [' + @Database + N'];';
	EXEC sp_executesql @SQL;

	-- Get configuration values
	IF @DataPath IS NULL
	BEGIN
		EXEC [$(Database)].Config.usp_GetConfigValue 
			@ConfigKey = 'DataPath', 
			@ConfigValue = @ConfigValue OUTPUT;
		SET @DataPath = @ConfigValue;
	END
	
	IF @LogPath IS NULL
	BEGIN
		EXEC [$(Database)].Config.usp_GetConfigValue 
			@ConfigKey = 'LogPath', 
			@ConfigValue = @ConfigValue OUTPUT;
		SET @LogPath = @ConfigValue;
	END

	-- Get DBA operator details
	DECLARE @OperatorName NVARCHAR(128);
	DECLARE @OperatorEmail NVARCHAR(128);

	EXEC [$(Database)].Config.usp_GetConfigValue 
		@ConfigKey = 'NotificationOperator', 
		@ConfigValue = @OperatorName OUTPUT;

	EXEC [$(Database)].Config.usp_GetConfigValue 
		@ConfigKey = 'NotificationEmail', 
		@ConfigValue = @OperatorEmail OUTPUT;
	

	BEGIN TRY
		-- Create Production schema if it doesn't exist
		IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Production')
		BEGIN
			EXEC('CREATE SCHEMA Production');
		END;

		-- Check if the ETLLog table exists in current database context
		IF NOT EXISTS (SELECT 1 FROM sys.objects o 
					JOIN sys.schemas s ON o.schema_id = s.schema_id
					WHERE o.name = 'ETLLog' AND s.name = 'Production' AND o.type = 'U')
		BEGIN
			-- Create ETLLog table if it doesn't exist
			CREATE TABLE Production.ETLLog (
				LogID INT IDENTITY(1,1) PRIMARY KEY,
				ProcedureName NVARCHAR(255) NULL,
				StepName NVARCHAR(255) NULL,
				Status NVARCHAR(50) NULL,
				ExecutionDateTime DATETIME DEFAULT GETDATE(),
				ErrorMessage NVARCHAR(MAX) NULL,
				ExecutionTimeSeconds INT NULL,
				BatchID UNIQUEIDENTIFIER NULL,
				Message NVARCHAR(MAX) NULL,
				RowsProcessed INT NULL
			);
		END;

		-- Initialize ETL process
		SET @CurrentStep = 'Initialize';
				
		-- Log ETL start
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_ExecuteProductionETLWithIFRS9', @CurrentStep, 'Starting Production ETL with IFRS9');

		-- Execute the standard ETL process
		EXEC Production.usp_ExecuteProductionETL
			@Database = @Database,
			@DataPath = @DataPath,
			@LogPath = @LogPath,
			@ValidateAfterLoad = @ValidateAfterLoad,
			@EnableMemoryOptimized = @EnableMemoryOptimized,
			@EnableColumnstoreIndexes = @EnableColumnstoreIndexes,
			@RebuildIndexes = @RebuildIndexes;

		-- If IFRS9 is enabled, execute the IFRS9 process
		IF @RunIFRS9 = 1
		BEGIN
			-- =================================
			-- Step: Execute IFRS 9 processes
			-- =================================
			SET @CurrentStep = 'ExecuteIFRS9Process';
			SET @StepStartTime = GETDATE();

			-- Check if the IFRS 9 schema and procedures exist
			IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'IFRS9') AND 
			EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteProcessIFRS9' AND schema_id = SCHEMA_ID('IFRS9'))
			BEGIN
				-- Execute IFRS 9 process
				DECLARE @AsOfDate DATE = CAST(GETDATE() AS DATE);
				DECLARE @IFRS9BatchID UNIQUEIDENTIFIER = NEWID();
				
				EXEC IFRS9.usp_ExecuteProcessIFRS9
					@AsOfDate = @AsOfDate,
					@BatchID = @IFRS9BatchID,
					@IncludeAllScenarios = 1,
					@CalculateMovement = 1;

				-- Log completion
				INSERT INTO Production.ETLLog 
					(ProcedureName, StepName, Status, ExecutionTimeSeconds)
				VALUES 
					('usp_ExecuteProductionETLWithIFRS9', @CurrentStep, 'Success',
					DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			END
			ELSE
			BEGIN
				-- Log skipped status
				INSERT INTO Production.ETLLog 
					(ProcedureName, StepName, Status, ExecutionTimeSeconds)
				VALUES 
					('usp_ExecuteProductionETLWithIFRS9', @CurrentStep, 'Skipped - IFRS9 schema or procedures not found',
					DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			END

			-- Add IFRS 9 data to reporting views
			SET @CurrentStep = 'RefreshIFRS9Views';
			SET @StepStartTime = GETDATE();

			-- Check if the refresh procedure exists
			IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_RefreshReportingViews' AND schema_id = SCHEMA_ID('IFRS9'))
			BEGIN
				-- Refresh IFRS9 reporting views
				EXEC IFRS9.usp_RefreshReportingViews;

				-- Log completion
				INSERT INTO Production.ETLLog 
					(ProcedureName, StepName, Status, ExecutionTimeSeconds)
				VALUES 
					('usp_ExecuteProductionETLWithIFRS9', @CurrentStep, 'Success',
					DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			END
			ELSE
			BEGIN
				-- Log skipped status
				INSERT INTO Production.ETLLog 
					(ProcedureName, StepName, Status, ExecutionTimeSeconds)
				VALUES 
					('usp_ExecuteProductionETLWithIFRS9', @CurrentStep, 'Skipped - IFRS9 view refresh procedure not found',
					DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			END
		END

		-- Log completion of the ETL process with IFRS9
		INSERT INTO Production.ETLLog 
			(ProcedureName, Status, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETLWithIFRS9', 'Success', 
			DATEDIFF(SECOND, @StartTime, GETDATE()));
			
		-- Return success flag
		SELECT 'Success' AS Status, 
			DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTime,
			@BatchID AS BatchID;
	
	END TRY
	BEGIN CATCH
		-- Get error details
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
			
		-- Log the error including which step failed
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, ErrorMessage)
		VALUES 
			('usp_ExecuteProductionETLWithIFRS9', @CurrentStep, 'Failed', 
			'Error ' + CAST(ERROR_NUMBER() AS VARCHAR) + ': ' + @ErrorMessage);
			
		-- Send email notification for critical ETL failure
		DECLARE @EmailSubject NVARCHAR(255) = 'Production ETL Failure (IFRS9): ' + @CurrentStep;
		DECLARE @EmailBody NVARCHAR(MAX) = 'The production ETL process with IFRS9 failed at step [' + @CurrentStep + 
										'] with error: ' + @ErrorMessage;
										
		-- Failure notification email to DBA operator
		EXEC msdb.dbo.sp_send_dbmail
			@recipients = @OperatorEmail,
			@subject = @EmailSubject,
			@body = @EmailBody;
			
		-- Return error information
		SELECT 'Error' AS Status, 
			@CurrentStep AS FailedStep,
			@ErrorMessage AS ErrorMessage,
			DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTime,
			@BatchID AS BatchID;
			
		-- Re-throw with original severity if it's a serious error
		IF @ErrorSeverity >= 16
			RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH;
END;
GO
