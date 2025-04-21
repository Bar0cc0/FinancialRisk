/* 
This script creates the Master ETL procedure for orchestrating the production processes
Last updated: 2025-01-01
*/


-- Helper procedure to clean up old ETL logs
CREATE OR ALTER PROCEDURE Production.usp_PurgeOldLogs
	@RetentionDays INT = 30
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @CutoffDate DATETIME = DATEADD(DAY, -@RetentionDays, GETDATE());
	DECLARE @DeletedCount INT;
	
	-- Delete old log entries
	DELETE FROM Production.ETLLog
	WHERE [ExecutionDateTime] < @CutoffDate;
	
	SET @DeletedCount = @@ROWCOUNT;
	
	-- Log the cleanup operation
	INSERT INTO Production.ETLLog (ProcedureName, Status, RowsProcessed)
	VALUES ('Log Maintenance', 'Purged Old Logs', @DeletedCount);
	
	-- Return count of deleted rows
	SELECT @DeletedCount AS DeletedLogEntries;
END;
GO

-- Helper procedure to monitor ETL execution
CREATE OR ALTER PROCEDURE Production.usp_ETLMonitoringDashboard AS
BEGIN
	SELECT TOP 100 
		ProcedureName, Status, 
		AVG(ExecutionTimeSeconds) AS AvgExecutionTime,
		MAX(ExecutionTimeSeconds) AS MaxExecutionTime
	FROM Production.ETLLog
	GROUP BY ProcedureName, Status
	ORDER BY ProcedureName, Status;
END;
GO


-- =========================================================
-- Master ETL procedure for production data
-- =========================================================
CREATE OR ALTER PROCEDURE Production.usp_ExecuteProductionETL
	@Database NVARCHAR(128),
	@DataPath NVARCHAR(500),
	@LogPath NVARCHAR(500),
	@ValidateAfterLoad BIT = 1,
	@EnableMemoryOptimized BIT = 1,
	@EnableColumnstoreIndexes BIT = 1,
	@RebuildIndexes BIT = 0
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
		-- Check if the ETLLog table exists
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Production'))
		BEGIN
			RAISERROR('Production.ETLLog table does not exist.', 16, 1);
			RETURN;
		END;

		-- Initialize ETL process
		SET @CurrentStep = 'Initialize';
				
		-- Log ETL start
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_ExecuteProductionETL', @CurrentStep, 'Starting Production ETL');

		-- Log configuration being used
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status, ErrorMessage)
		VALUES ('usp_ExecuteProductionETL', 'Configuration', 'Info', 
				'Using paths - Data: ' + @DataPath + ', Log: ' + @LogPath);

		
		-- ======================================
		-- Step 1: Run pre-production validations
		-- ======================================
		SET @CurrentStep = 'PreProductionValidation';
		SET @StepStartTime = GETDATE();

		EXEC Production.usp_PreProductionValidation 
			@LogPath = @LogPath,
			@ThresholdPercent = 5.0, 
			@FailOnError = 0;
				
		-- Check if validation has run successfully
		IF NOT EXISTS (
			SELECT 1 FROM Staging.ETLLog 
			WHERE ProcedureName = 'usp_PreProductionValidation'
			AND Status IN ('Success', 'Warning')
			AND ExecutionDateTime > DATEADD(DAY, -1, GETDATE())
		)
		BEGIN
			RAISERROR('No recent pre-production validation found.', 16, 1);
		END
		
		-- Check if required staging tables have data
		IF (SELECT COUNT(*) FROM Staging.Loan) = 0
		BEGIN
			RAISERROR('Loan table is empty. No data to load.', 16, 1);
		END
		
		-- Log pre-production validation completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETL', @CurrentStep, 'Success', 
			0, DATEDIFF(SECOND, @StepStartTime, GETDATE()));

	

		-- ================================
		-- Step 2: Create production tables
		-- ================================
		--IF @@TRANCOUNT = 0
		--	BEGIN TRANSACTION		
		SET @CurrentStep = 'CreateProductionTables';
		SET @StepStartTime = GETDATE();
		
		-- Check if production tables need to be created
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCustomer' AND schema_id = SCHEMA_ID('Production'))
		BEGIN
			-- Execute the stored procedure to create production tables
			EXEC Production.usp_CreateProductionTables
				@EnableMemoryOptimized = @EnableMemoryOptimized,
				@EnableColumnstoreIndexes = @EnableColumnstoreIndexes,
				@CreateExtendedProperties = 1,
				@LogPath = @LogPath;
				
			-- Populate date dimension
			EXEC Production.usp_PopulateDateDimension;
		END
		
		-- Log completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETL', @CurrentStep, 'Created', 
			DATEDIFF(SECOND, @StepStartTime, GETDATE()));

		--IF @@TRANCOUNT > 0
		--	COMMIT TRANSACTION;


		-- ==============================
		-- Step 2: Load production tables
		-- ==============================
		--IF @@TRANCOUNT = 0
		--	BEGIN TRANSACTION
		SET @CurrentStep = 'LoadProductionTables';
		SET @StepStartTime = GETDATE();

		-- Check if the required procedure exists
		IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_LoadProductionTables' AND schema_id = SCHEMA_ID('Production'))
		BEGIN
			RAISERROR('Required procedure usp_LoadProductionTables does not exist.', 16, 1);
			RETURN;
		END

		-- Execute the procedure to load production tables
		EXEC Production.usp_LoadProductionTables 
			@BatchID = @BatchID,
			@DataPath = @DataPath, 
			@LogPath = @LogPath;

		-- Log completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, Status, RowsProcessed, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETL', 'Success',	
			@@ROWCOUNT, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		
		--IF @@TRANCOUNT > 0
		--	COMMIT TRANSACTION;


		-- =================================
		-- Step 3: Calculate derived metrics
		-- =================================
		--IF @@TRANCOUNT = 0
		--	BEGIN TRANSACTION;
		SET @CurrentStep = 'CalculateDerivedMetrics';
		SET @StepStartTime = GETDATE();

		-- Get current date ID for risk calculations
		DECLARE @CurrentDateID INT;
		SELECT @CurrentDateID = MAX(DateID) 
		FROM Production.DimDate 
		WHERE FullDate <= CONVERT(DATE, GETDATE());

		-- Check if the required tables for risk calculations are populated
		IF NOT EXISTS (SELECT 1 FROM Production.FactCredit) OR
			NOT EXISTS (SELECT 1 FROM Production.FactCustomer) OR
			NOT EXISTS (SELECT 1 FROM Production.FactLoan) OR
			NOT EXISTS (SELECT 1 FROM Production.FactMarket) OR
			NOT EXISTS (SELECT 1 FROM Production.FactFraud) OR
			NOT EXISTS (SELECT 1 FROM Production.FactEconomicIndicators)
		BEGIN
			RAISERROR('Required tables for risk calculations are empty. Check data loading.', 16, 1);
			RETURN;
		END

		-- Call the risk score calculation procedure
		EXEC Production.usp_UpdateRiskScores @DateID = @CurrentDateID;

		-- Log completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETL', @CurrentStep, 'Success',
			@@ROWCOUNT, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		
		--IF @@TRANCOUNT > 0
		--	COMMIT TRANSACTION;

		-- =====================================
		-- Step 4: Rebuild indexes if requested
		-- =====================================
		--IF @@TRANCOUNT = 0
		--	BEGIN TRANSACTION;
		SET @CurrentStep = 'RebuildIndexes';
		SET @StepStartTime = GETDATE();

		IF @RebuildIndexes = 1
		BEGIN
			SET @CurrentStep = 'RebuildIndexes';
			SET @StepStartTime = GETDATE();
			
			-- Execute the index rebuild procedure
			EXEC Production.usp_RebuildIndexes;
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, Status, ExecutionTimeSeconds)
			VALUES 
				('Indexes', 'Rebuilt', 
				DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		END
		
		-- Log completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETL', @CurrentStep, 'Success',
			DATEDIFF(SECOND, @StepStartTime, GETDATE()));

		--IF @@TRANCOUNT > 0
		--	COMMIT TRANSACTION;

		-- ==========================================
		-- Step 5: Post-load validation if requested
		-- ==========================================
		--IF @@TRANCOUNT = 0
		--	BEGIN TRANSACTION;
		SET @CurrentStep = 'PostLoadValidation';
		SET @StepStartTime = GETDATE();
		DECLARE @ValidationBatchId UNIQUEIDENTIFIER = NEWID();

		IF @ValidateAfterLoad = 1
		BEGIN
			SET @CurrentStep = 'PostLoadValidation';
			SET @StepStartTime = GETDATE();
			
			-- Perform basic data quality checks on production data
			-- Count of customers in DimCustomer vs. FactCustomer
			DECLARE @DimCustomerCount INT, @FactCustomerCount INT;
			
			SELECT @DimCustomerCount = COUNT(*) 
			FROM Production.DimCustomer 
			WHERE IsCurrent = 1;
			
			SELECT @FactCustomerCount = COUNT(DISTINCT CustomerSK) 
			FROM Production.FactCustomer;
			
			-- If counts differ significantly, log a warning
			IF ABS(@DimCustomerCount - @FactCustomerCount) > (@DimCustomerCount * 0.05) -- 5% threshold
			BEGIN
				INSERT INTO Production.ETLLog 
					(ProcedureName, Status, ErrorMessage)
				VALUES 
					('Validation', 'Warning', 
					'Customer count mismatch: ' + CAST(@DimCustomerCount AS VARCHAR) + 
					' in dimension vs ' + CAST(@FactCustomerCount AS VARCHAR) + ' in fact');
				
				-- Add validation log entry - THIS WAS MISSING
				INSERT INTO Production.ValidationLog
					(BatchID, ExecutionDateTime, ValidationCategory, DataSource, ErrorType, Severity, ErrorCount, IsBlocking)
				VALUES
					(@ValidationBatchId, GETDATE(), 'Data Consistency', 'DimCustomer/FactCustomer', 
					'Customer count mismatch', 'Warning', ABS(@DimCustomerCount - @FactCustomerCount), 0);
			END
			ELSE 
			BEGIN
				-- Add successful validation entry - THIS WAS MISSING
				INSERT INTO Production.ValidationLog
					(BatchID, ExecutionDateTime, ValidationCategory, DataSource, ErrorType, Severity, ErrorCount, IsBlocking)
				VALUES
					(@ValidationBatchId, GETDATE(), 'Data Consistency', 'DimCustomer/FactCustomer', 
					'Information', 'Info', 0, 0);
			END
			
			-- Log completion
			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, ExecutionTimeSeconds)
			VALUES 
				('usp_ExecuteProductionETL', @CurrentStep, 'Success', 
				DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		END
		
		-- Log completion of the ETL process
		INSERT INTO Production.ETLLog 
			(ProcedureName, Status, ExecutionTimeSeconds)
		VALUES 
			('usp_ExecuteProductionETL', 'Success', 
			DATEDIFF(SECOND, @StartTime, GETDATE()));
			
		-- Return success flag
		SELECT 'Success' AS Status, 
			DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTime,
			@BatchID AS BatchID;

		--IF @@TRANCOUNT > 0
		--	COMMIT TRANSACTION;
	
	END TRY
	BEGIN CATCH
		-- Roll back transaction in case of error
		--IF XACT_STATE() = -1 
		--	ROLLBACK TRANSACTION;
			
		-- Get error details
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
			
		-- Log the error including which step failed
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, ErrorMessage)
		VALUES 
			('usp_ExecuteProductionETL', @CurrentStep, 'Failed', 
			'Error ' + CAST(ERROR_NUMBER() AS VARCHAR) + ': ' + @ErrorMessage);
			
		-- Send email notification for critical ETL failure
		DECLARE @EmailSubject NVARCHAR(255) = 'Production ETL Failure: ' + @CurrentStep;
		DECLARE @EmailBody NVARCHAR(MAX) = 'The production ETL process failed at step [' + @CurrentStep + 
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


-- ===================================================
-- Only execute if environment hasn't been created yet
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteProductionETL' AND schema_id = SCHEMA_ID('Production'))
BEGIN
	EXEC Production.usp_ExecuteProductionETL
		@DataPath = N'$(DataPath)',
		@LogPath = N'$(LogPath)';
END