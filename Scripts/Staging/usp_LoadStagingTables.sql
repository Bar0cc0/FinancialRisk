/* 
This script creates a stored procedure to load data into staging tables from cooked CSV files.
Last updated: 2025-01-01
*/

EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;

EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;


GO

CREATE OR ALTER PROCEDURE Staging.usp_LoadStagingTables
	@BatchID UNIQUEIDENTIFIER = NULL,
	@DataPath NVARCHAR(500) = NULL,
	@LogPath NVARCHAR(500) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Variables for execution tracking
	DECLARE @CurrentStep NVARCHAR(128);
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @CurrentFile NVARCHAR(100);
	DECLARE @RowsProcessed INT;
	DECLARE @ConfigValue NVARCHAR(500);
	DECLARE @SQL NVARCHAR(MAX);

	-- Create a new batch ID if one wasn't provided
	SET @BatchID = ISNULL(@BatchID, NEWID());
	
	-- Get configuration values if not provided
	IF @DataPath IS NULL
	BEGIN
		EXEC Config.usp_GetConfigValue 
			@ConfigKey = 'DataPath', 
			@ConfigValue = @ConfigValue OUTPUT;
		SET @DataPath = @ConfigValue;
	END
	
	IF @LogPath IS NULL
	BEGIN
		EXEC Config.usp_GetConfigValue 
			@ConfigKey = 'LogPath', 
			@ConfigValue = @ConfigValue OUTPUT;
		SET @LogPath = @ConfigValue;
	END

	BEGIN TRY

		-- Log the start of the load process
		INSERT INTO Staging.ETLLog (ProcedureName, Status)
		VALUES ('usp_LoadStagingTables', 'Started');

		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION;
			
		-- Begin transaction
		IF @@TRANCOUNT = 0
		BEGIN TRANSACTION;
				
			-- ===================================
			-- Step 1: Truncate all staging tables
			-- ===================================
			SET @CurrentStep = 'TruncateTables';
			SET @StartTime = GETDATE();
			
			TRUNCATE TABLE Staging.Loan;
			TRUNCATE TABLE Staging.Fraud;
			TRUNCATE TABLE Staging.Market;
			TRUNCATE TABLE Staging.Macro;
			
			INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status, ExecutionTimeSeconds)
			VALUES ('usp_LoadStagingTables', @CurrentStep, 'Success', 
					DATEDIFF(SECOND, @StartTime, GETDATE()));
		
			-- ======================
			-- Step 2: Load Loan data
			-- ======================
			SET @CurrentFile = 'Loan_cooked.csv';
			SET @CurrentStep = 'LoadLoanData';
			SET @StartTime = GETDATE();

			-- Check if the file exists
			DECLARE @FileExists BIT;
			DECLARE @FileExistsInt INT;
			DECLARE @FullPath NVARCHAR(1000);
			SET @FullPath = CASE 
					WHEN RIGHT(@DataPath, 1) = '\' THEN '\\localhost\FinancialRisk' + @CurrentFile --@DataPath + @CurrentFile
					ELSE '\\localhost\FinancialRisk'+ '\' + @CurrentFile --@DataPath + '\' + @CurrentFile
				END;
			EXEC master.dbo.xp_fileexist @FullPath, @FileExistsInt OUTPUT;
			SET @FileExists = CAST(@FileExistsInt AS BIT);

			IF @FileExists = 0
			BEGIN
				SET @ErrorMessage = 'File ' + @FullPath + ' does not exist';
				THROW 50001, @ErrorMessage, 1;
			END
			
			-- Perform the bulk insert
			SET @SQL = '
			BULK INSERT Staging.Loan
			FROM ''' + @FullPath + '''
			WITH (
				FORMAT = ''CSV'',
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''\n'',
				TABLOCK,
				ERRORFILE = ''' + @LogPath + 'loan_errors.txt' + ''',
				MAXERRORS = 10
			)';
			
			EXEC sp_executesql @SQL;
			
			SET @RowsProcessed = @@ROWCOUNT;
			
			-- Log completion
			INSERT INTO Staging.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds, FileName)
			VALUES 
				('usp_LoadStagingTables', @CurrentStep, 'Loaded', 
				@RowsProcessed, DATEDIFF(SECOND, @StartTime, GETDATE()), @CurrentFile);
		

			-- =======================
			-- Step 3: Load Fraud data
			-- =======================
			SET @CurrentFile = 'Fraud_cooked.csv';
			SET @CurrentStep = 'LoadFraudData';
			SET @StartTime = GETDATE();

			-- Check if the file exists
			SET @FullPath = CASE 
					WHEN RIGHT(@DataPath, 1) = '\' THEN '\\localhost\FinancialRisk' + @CurrentFile --@DataPath + @CurrentFile
					ELSE '\\localhost\FinancialRisk'+ '\' + @CurrentFile --@DataPath + '\' + @CurrentFile
				END;
			EXEC master.dbo.xp_fileexist @FullPath, @FileExistsInt OUTPUT;
			SET @FileExists = CAST(@FileExistsInt AS BIT);

			IF @FileExists = 0
			BEGIN
				SET @ErrorMessage = 'File ' + @FullPath + ' does not exist';
				THROW 50001, @ErrorMessage, 1;
			END
			
			-- Perform the bulk insert
			SET @SQL = '
			BULK INSERT Staging.Fraud
			FROM ''' + @FullPath + '''
			WITH (
				FORMAT = ''CSV'',
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''\n'',
				TABLOCK,
				ERRORFILE = ''' + @LogPath + 'fraud_errors.txt' + ''',
				MAXERRORS = 10
			)';
			
			EXEC sp_executesql @SQL;
			
			SET @RowsProcessed = @@ROWCOUNT;
			
			-- Log completion
			INSERT INTO Staging.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds, FileName)
			VALUES 
				('usp_LoadStagingTables', @CurrentStep, 'Loaded', 
				@RowsProcessed, DATEDIFF(SECOND, @StartTime, GETDATE()), @CurrentFile);
		

			-- ========================
			-- Step 4: Load Market data
			-- ========================
			SET @CurrentFile = 'Market_cooked.csv';
			SET @CurrentStep = 'LoadMarketData';
			SET @StartTime = GETDATE();
			
			-- Check if the file exists
			SET @FullPath = CASE 
					WHEN RIGHT(@DataPath, 1) = '\' THEN '\\localhost\FinancialRisk' + @CurrentFile --@DataPath + @CurrentFile
					ELSE '\\localhost\FinancialRisk'+ '\' + @CurrentFile --@DataPath + '\' + @CurrentFile
				END;
			EXEC master.dbo.xp_fileexist @FullPath, @FileExistsInt OUTPUT;
			SET @FileExists = CAST(@FileExistsInt AS BIT);

			IF @FileExists = 0
			BEGIN
				SET @ErrorMessage = 'File ' + @FullPath + ' does not exist';
				THROW 50001, @ErrorMessage, 1;
			END

			-- Perform the bulk insert
			SET @SQL = '
			BULK INSERT Staging.Market
			FROM ''' + @FullPath + '''
			WITH (
				FORMAT = ''CSV'',
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''\n'',
				TABLOCK,
				ERRORFILE = ''' + @LogPath + 'market_errors.txt' + ''',
				MAXERRORS = 10
			)';
			
			EXEC sp_executesql @SQL;
			
			SET @RowsProcessed = @@ROWCOUNT;
			
			-- Log completion
			INSERT INTO Staging.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds, FileName)
			VALUES 
				('usp_LoadStagingTables', @CurrentStep, 'Loaded', 
				@RowsProcessed, DATEDIFF(SECOND, @StartTime, GETDATE()), @CurrentFile);
		

		-- =======================
		-- Step 5: Load Macro data
		-- =======================
		SET @CurrentFile = 'Macro_cooked.csv';
		SET @CurrentStep = 'LoadMacroData';
		SET @StartTime = GETDATE();

		-- Check if the file exists
		SET @FullPath = CASE 
				WHEN RIGHT(@DataPath, 1) = '\' THEN '\\localhost\FinancialRisk' + @CurrentFile --@DataPath + @CurrentFile
				ELSE '\\localhost\FinancialRisk'+ '\' + @CurrentFile --@DataPath + '\' + @CurrentFile
			END;
		EXEC master.dbo.xp_fileexist @FullPath, @FileExistsInt OUTPUT;
		SET @FileExists = CAST(@FileExistsInt AS BIT);

		IF @FileExists = 0
		BEGIN
			SET @ErrorMessage = 'File ' + @FullPath + ' does not exist';
			THROW 50001, @ErrorMessage, 1;
		END
		
		-- Perform the bulk insert
		SET @SQL = '
		BULK INSERT Staging.Macro
		FROM ''' + @FullPath + '''
		WITH (
			FORMAT = ''CSV'',
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''\n'',
			TABLOCK,
			ERRORFILE = ''' + @LogPath + 'macro_errors.txt' + ''',
			MAXERRORS = 10
		)';
		
		EXEC sp_executesql @SQL;
		
		SET @RowsProcessed = @@ROWCOUNT;
		
		-- Log completion
		INSERT INTO Staging.ETLLog 
			(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds, FileName)
		VALUES 
			('usp_LoadStagingTables', @CurrentStep, 'Loaded', 
			@RowsProcessed, DATEDIFF(SECOND, @StartTime, GETDATE()), @CurrentFile);
		

		COMMIT TRANSACTION;
		
		-- Log successful completion
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES ('usp_LoadStagingTables', 'StagingLoad', 'Completed Successfully',
				DATEDIFF(SECOND, @StartTime, GETDATE()));
		
		-- Return summary information
		SELECT 
			'Load Complete' AS Status,
			(SELECT COUNT(*) FROM Staging.Loan) AS LoanRows,
			(SELECT COUNT(*) FROM Staging.Fraud) AS FraudRows,
			(SELECT COUNT(*) FROM Staging.Market) AS MarketRows,
			(SELECT COUNT(*) FROM Staging.Macro) AS MacroRows;
	
	END TRY
	BEGIN CATCH
		-- Roll back transaction if it exists
		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION;
			
		-- Get error information
		SELECT @ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
		
		-- Log the error
		INSERT INTO Staging.ETLLog
			(ProcedureName, StepName, Status, ErrorMessage, FileName)
		VALUES
			('usp_LoadStagingTables', @CurrentStep, 'Failed', @ErrorMessage, @CurrentFile);
			
		-- Re-throw the error
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH;
END;
GO
