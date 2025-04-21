/* 
This script creates the Master ETL procedure for orchestrating the staging processes
Last updated: 2025-01-01
*/

-- Add explicit database context
USE [$(Database)];
GO

-- Create Staging schema if it doesn't exist (for safety)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Staging')
BEGIN
	EXEC('CREATE SCHEMA Staging');
	PRINT 'Created Staging schema';
END
GO


-- Helper procedure to clean up old ETL logs
CREATE OR ALTER PROCEDURE Staging.usp_PurgeOldLogs
	@RetentionDays INT = 30
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @CutoffDate DATETIME = DATEADD(DAY, -@RetentionDays, GETDATE());
	DECLARE @DeletedCount INT;
	
	-- Delete old log entries
	DELETE FROM Staging.ETLLog
	WHERE [ExecutionDateTime] < @CutoffDate;
	
	SET @DeletedCount = @@ROWCOUNT;
	
	-- Log the cleanup operation
	INSERT INTO Staging.ETLLog (ProcedureName, Status, RowsProcessed)
	VALUES ('Log Maintenance', 'Purged Old Logs', @DeletedCount);
	
	-- Return count of deleted rows
	SELECT @DeletedCount AS DeletedLogEntries;
END;
GO

-- Helper procedure to monitor ETL execution
CREATE OR ALTER PROCEDURE Staging.usp_ETLMonitoringDashboard AS
BEGIN
	SELECT TOP 100 
		ProcedureName, Status, 
		AVG(ExecutionTimeSeconds) AS AvgExecutionTime,
		MAX(ExecutionTimeSeconds) AS MaxExecutionTime
	FROM Staging.ETLLog
	GROUP BY ProcedureName, Status
	ORDER BY ProcedureName, Status;
END;
GO


-- =========================================================
-- Master ETL procedure for staging data
-- =========================================================
CREATE OR ALTER PROCEDURE Staging.usp_ExecuteStagingETL
	@Database NVARCHAR(128) = NULL,
	@DataPath NVARCHAR(500) = NULL, 
	@LogPath NVARCHAR(500) = NULL,
	@ValidateAfterLoad BIT = 1
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	SET XACT_ABORT ON;

	-- Variables for tracking execution
	DECLARE @CurrentStep NVARCHAR(128);
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @StepStartTime DATETIME;
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @ConfigValue NVARCHAR(500);
	DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();

	-- Set database context
	SET @Database = ISNULL(@Database, DB_NAME());

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

	-- Get dba operator details
	DECLARE @OperatorName NVARCHAR(128);
	DECLARE @OperatorEmail NVARCHAR(128);

	EXEC [$(Database)].Config.usp_GetConfigValue 
		@ConfigKey = 'NotificationOperator', 
		@ConfigValue = @OperatorName OUTPUT;

	EXEC [$(Database)].Config.usp_GetConfigValue 
		@ConfigKey = 'NotificationEmail', 
		@ConfigValue = @OperatorEmail OUTPUT;
	
	
	BEGIN TRY
		-- Check if the ETLLog table exists
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Staging'))
		BEGIN
			RAISERROR('Staging.ETLLog table does not exist.', 16, 1);
			RETURN;
		END;

		-- Initialize ETL process
		SET @CurrentStep = 'Initialize';

		-- Log ETL start
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_ExecuteETL', @CurrentStep, 'Starting Staging ETL');
		
		-- Log configuration being used
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status, ErrorMessage)
		VALUES ('usp_ExecuteETL', 'Configuration', 'Info', 
				'Using paths - Data: ' + @DataPath + ', Log: ' + @LogPath);


		-- =============================
		-- Step 1: Create staging tables
		-- =============================
		IF @@TRANCOUNT = 0
			BEGIN TRANSACTION
		SET @CurrentStep = 'CreateStagingTables';
		SET @StepStartTime = GETDATE();

		-- Check if the staging tables need to be created
		IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_CreateStagingTables' AND schema_id = SCHEMA_ID('Staging'))
		BEGIN
			-- Execute the procedure to create staging tables
			EXEC Staging.usp_CreateStagingTables
				@LogPath = @LogPath;
		END

		-- Log completion
		INSERT INTO Staging.ETLLog
			(ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES
			('usp_ExecuteETL', @CurrentStep, 'Success', 
			DATEDIFF(SECOND, @StepStartTime, GETDATE()));

		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION;


		-- ===========================
		-- Step 2: Load staging tables
		-- ===========================
		
		/*	
			THIS PART IS DELEGATED TO ImportData.ps1 
			TO AVOID AUTHORIZATION ISSUES OUTSIDE OF THE SQL SERVER CONTEXT.	
		*/
		IF @@TRANCOUNT = 0
			BEGIN TRANSACTION
		SET @CurrentStep = 'LoadStagingTables';
		SET @StepStartTime = GETDATE();

		-- Get scripts path if not provided
		DECLARE @ScriptsPath NVARCHAR(500);
		EXEC [$(Database)].Config.usp_GetConfigValue 
			@ConfigKey = 'ScriptsPath', 
			@ConfigValue = @ScriptsPath OUTPUT;

		-- Define file-table mappings
		CREATE TABLE #FileTableMappings (
			FileName VARCHAR(100),
			TableName VARCHAR(100),
			OrderID INT
		);

		-- Insert mappings
		INSERT INTO #FileTableMappings (FileName, TableName, OrderID)
		VALUES 
			('Loan_cooked.csv', 'Staging.Loan', 1),
			('Fraud_cooked.csv', 'Staging.Fraud', 2),
			('Market_cooked.csv', 'Staging.Market', 3),
			('Macro_cooked.csv', 'Staging.Macro', 4);

		-- Process each file-table mapping
		DECLARE @FileName VARCHAR(100), @TableName VARCHAR(100), @OrderID INT;
		DECLARE @FilePath VARCHAR(500), @ServerName VARCHAR(100), @DatabaseName VARCHAR(100);
		DECLARE @PowerShellCmd NVARCHAR(1000), @OutputMsg NVARCHAR(1000);

		-- Set server and database names
		SELECT @ServerName = @@SERVERNAME, @DatabaseName = DB_NAME();

		DECLARE mapping_cursor CURSOR FOR 
		SELECT FileName, TableName, OrderID 
		FROM #FileTableMappings 
		ORDER BY OrderID;

		OPEN mapping_cursor;
		FETCH NEXT FROM mapping_cursor INTO @FileName, @TableName, @OrderID;

		WHILE @@FETCH_STATUS = 0
		BEGIN
			-- Construct file path
			SET @FilePath = CASE 
				WHEN RIGHT(@DataPath, 1) = '\' THEN @DataPath + @FileName
				ELSE @DataPath + '\' + @FileName
			END;
			
			-- Log the start of importing this file
			INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status, FileName)
			VALUES ('usp_ExecuteETL', 'Importing ' + @TableName, 'Started', @FileName);
			
			-- Construct PowerShell command
			SET @PowerShellCmd = 'powershell.exe -ExecutionPolicy Bypass -File "' + @ScriptsPath + 
							'\Staging\ImportData.ps1" -TableName "' + @TableName + 
							'" -FilePath "' + @FilePath + '" -ServerInstance "' + 
							@ServerName + '" -Database "' + @DatabaseName + '"';
			
			-- Execute PowerShell script
			BEGIN TRY
				-- Create table to store output
				CREATE TABLE #PSOutput (Output NVARCHAR(MAX));
				
				-- Execute command and capture output
				INSERT INTO #PSOutput
				EXEC xp_cmdshell @PowerShellCmd;
				
				-- Check for errors in output
				IF EXISTS (SELECT * FROM #PSOutput WHERE Output LIKE '%ERROR%')
				BEGIN
					SELECT @OutputMsg = (
						SELECT TOP 1 Output FROM #PSOutput
						WHERE Output LIKE '%ERROR%'
					);
					
					-- Log error
					INSERT INTO Staging.ETLLog 
						(ProcedureName, StepName, Status, ErrorMessage, FileName)
					VALUES 
						('usp_ExecuteETL', 'Import ' + @TableName, 'Failed', @OutputMsg, @FileName);
				END
				ELSE
				BEGIN
					-- Get row count
					DECLARE @RowCount INT;
					-- Parse the row count from the PowerShell success message
					SELECT @RowCount = TRY_CAST(
						SUBSTRING(
							Output,
							PATINDEX('%Successfully imported %', Output) + 20,  -- Skip "Successfully imported "
							CHARINDEX(' row', SUBSTRING(Output, PATINDEX('%Successfully imported %', Output) + 20, 50)) - 1
						) AS INT
					)
					FROM #PSOutput
					WHERE Output LIKE '%Successfully imported % row%';

					-- Fallback if parsing fails: get count directly from the table
					IF @RowCount IS NULL
					BEGIN
						DECLARE @SqlCmd NVARCHAR(500) = N'SELECT @Count = COUNT(*) FROM ' + @TableName;
						EXEC sp_executesql @SqlCmd, N'@Count INT OUTPUT', @RowCount OUTPUT;
					END
					
					-- Log success
					INSERT INTO Staging.ETLLog 
						(ProcedureName, StepName, Status, RowsProcessed, FileName)
					VALUES 
						('usp_ExecuteETL', 'Import ' + @TableName, 'Success', @RowCount, @FileName);
				END
				
				-- Clean up
				DROP TABLE #PSOutput;
			END TRY
			BEGIN CATCH
				-- Log error
				INSERT INTO Staging.ETLLog 
					(ProcedureName, StepName, Status, ErrorMessage, FileName)
				VALUES 
					('usp_ExecuteETL', 'Import ' + @TableName, 'Failed', 
					ERROR_MESSAGE(), @FileName);
			END CATCH
			
			FETCH NEXT FROM mapping_cursor INTO @FileName, @TableName, @OrderID;
		END

		CLOSE mapping_cursor;
		DEALLOCATE mapping_cursor;

		-- Log completion of all imports
		INSERT INTO Staging.ETLLog
			(ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES
			('usp_ExecuteETL', @CurrentStep, 'Completed', 
			DATEDIFF(SECOND, @StepStartTime, GETDATE()));

		-- Clean up
		DROP TABLE #FileTableMappings;

		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION;


		-- =====================
		-- Step 3: Validate data
		-- ===================== 
		IF @@TRANCOUNT = 0
			BEGIN TRANSACTION
		SET @CurrentStep = 'ValidateStagingData';
		SET @StepStartTime = GETDATE();

		IF @ValidateAfterLoad = 1
		BEGIN
			-- Check if the required procedure exists
			IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ValidateStagingData' AND schema_id = SCHEMA_ID('Staging'))
			BEGIN
				RAISERROR('Required procedure usp_ValidateStagingData does not exist.', 16, 1);
				RETURN;
			END

			-- Execute the procedure to validate staging data
			EXEC Staging.usp_ValidateStagingData
				@LogPath = @LogPath,
				@BatchID = @BatchID;
		END

		-- Log completion
		INSERT INTO Staging.ETLLog
			(ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES
			('usp_ExecuteETL', @CurrentStep, 'Success', 
			DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		
		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION;  
			
	END TRY
	BEGIN CATCH
		-- Roll back transaction in case of error
		IF XACT_STATE() = -1 
			ROLLBACK TRANSACTION;
		
		-- Get error details
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
		
		-- Log the error including which step failed
		INSERT INTO Staging.ETLLog
			(ProcedureName, StepName, Status, ErrorMessage)
		VALUES
			('usp_ExecuteETL', @CurrentStep, 'Failed', 
			'Error ' + CAST(ERROR_NUMBER() AS VARCHAR) + ': ' + @ErrorMessage);
			
		-- Re-throw with original severity if it's a serious error
		IF @ErrorSeverity >= 16
			RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH;
END;
GO

-- ===================================================
-- Only execute if environment hasn't been created yet
/*
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteStagingETL' AND schema_id = SCHEMA_ID('Staging'))
BEGIN
	EXEC Staging.usp_ExecuteStagingETL
		@DataPath = N'$(DataPath)',
		@LogPath = N'$(LogPath)';
END
*/