/* 
This script creates a stored procedure for pre-production data validation. 
Last updated: 2025-01-01
*/



CREATE OR ALTER PROCEDURE Production.usp_PreProductionValidation
	@LogPath NVARCHAR(500) = NULL,
	@ThresholdPercent DECIMAL(5,2) = 5.0, -- Maximum % difference allowed
	@FailOnError BIT = 0                  -- 1 to abort production load on validation failure
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Variables for tracking validation status
	DECLARE @HasErrors BIT = 0;
	DECLARE @HasWarnings BIT = 0;
	DECLARE @ErrorMessage NVARCHAR(4000) = '';
	DECLARE @ValidationDate DATETIME = GETDATE();
	DECLARE @ValidationBatchId UNIQUEIDENTIFIER = NEWID();
	DECLARE @ConfigValue NVARCHAR(500);
	
	-- Get configuration settings from the database
	IF @LogPath IS NULL
	BEGIN
		EXEC [$(Database)].Config.usp_GetConfigValue 
			@ConfigKey = 'LogPath', 
			@ConfigValue = @ConfigValue OUTPUT;
		SET @LogPath = @ConfigValue;
	END

	-- Record validation start
	INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
	VALUES ('usp_PreProductionValidation', 'Post-load validation', 'Created');
	
	-- Create temp table to hold validation results
	CREATE TABLE #ValidationResults (
		ValidationID INT IDENTITY(1,1),
		ValidationCategory VARCHAR(50) NOT NULL,
		ValidationCheck NVARCHAR(100) NOT NULL,
		Status VARCHAR(10) NOT NULL,  -- 'PASS', 'WARN', 'FAIL'
		Description NVARCHAR(MAX) NULL,
		AffectedRows INT NULL,
		Severity VARCHAR(20) NULL     -- 'Critical', 'High', 'Medium', 'Low'
	);
	
	-- ====================================
	-- 1. VALIDATION: ETL PROCESS INTEGRITY
	-- ====================================
	
	-- Check if all staging tables exist
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Loan' AND schema_id = SCHEMA_ID('Staging'))
	OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Fraud' AND schema_id = SCHEMA_ID('Staging'))
	OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Market' AND schema_id = SCHEMA_ID('Staging'))
	OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Macro' AND schema_id = SCHEMA_ID('Staging'))
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('ETL Process', 'Required Tables', 'FAIL', 'One or more required staging tables are missing', 'Critical');
		
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('ETL Process', 'Required Tables', 'PASS', 'All required staging tables exist', 'Low');
	END
	
	-- Check if staging ETL completed successfully
	IF NOT EXISTS (
		SELECT 1 FROM Staging.ETLLog 
		WHERE ProcedureName = 'usp_ExecuteETL' 
		AND StepName = 'StagingLoad' 
		AND Status = 'Success'
		AND ExecutionDateTime > DATEADD(DAY, -1, GETDATE())
	)
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('ETL Process', 'ETL Completion', 'WARN', 'No successful staging ETL completion found in the last 24 hours', 'Medium');
		
		SET @HasWarnings = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('ETL Process', 'ETL Completion', 'PASS', 'Staging ETL completed successfully within the last 24 hours', 'Low');
	END
	
	-- ================================
	-- 2. VALIDATION: DATA COMPLETENESS
	-- ================================
	
	-- Check if staging tables have rows
	DECLARE @LoanCount INT, @FraudCount INT, @MarketCount INT, @MacroCount INT;
	
	SELECT @LoanCount = COUNT(*) FROM Staging.Loan;
	SELECT @FraudCount = COUNT(*) FROM Staging.Fraud;
	SELECT @MarketCount = COUNT(*) FROM Staging.Market;
	SELECT @MacroCount = COUNT(*) FROM Staging.Macro;
	
	-- Loan data validation
	IF @LoanCount = 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Completeness', 'Loan Data', 'FAIL', 'Loan table is empty', 0, 'Critical');
		
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		-- Get historical average count
		DECLARE @AvgLoanCount INT;
		SELECT @AvgLoanCount = AVG(RowsProcessed)
		FROM Staging.ETLLog
		WHERE StepName = 'LoadLoanData'
		AND Status = 'Loaded'
		AND ExecutionDateTime > DATEADD(MONTH, -1, GETDATE());
		
		-- Check if count within expected range
		IF @AvgLoanCount > 0 AND ABS(@LoanCount - @AvgLoanCount) > (@AvgLoanCount * @ThresholdPercent / 100)
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Loan Data Count', 'WARN', 
				'Current loan count (' + CAST(@LoanCount AS VARCHAR) + ') differs from average (' + 
				CAST(@AvgLoanCount AS VARCHAR) + ') by more than ' + CAST(@ThresholdPercent AS VARCHAR) + '%',
				@LoanCount, 'Medium');
				
			SET @HasWarnings = 1;
		END
		ELSE
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Loan Data Count', 'PASS', 
				'Loan count within expected range: ' + CAST(@LoanCount AS VARCHAR) + ' records',
				@LoanCount, 'Low');
		END
	END
	
	-- Fraud data completeness validation
	IF @FraudCount = 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Completeness', 'Fraud Data', 'FAIL', 'Fraud table is empty', 0, 'Critical');
		
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		-- Get historical average count
		DECLARE @AvgFraudCount INT;
		SELECT @AvgFraudCount = AVG(RowsProcessed)
		FROM Staging.ETLLog
		WHERE StepName = 'LoadFraudData'
		AND Status = 'Loaded'
		AND ExecutionDateTime > DATEADD(MONTH, -1, GETDATE());
		
		-- Check if count within expected range
		IF @AvgFraudCount > 0 AND ABS(@FraudCount - @AvgFraudCount) > (@AvgFraudCount * @ThresholdPercent / 100)
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Fraud Data Count', 'WARN', 
				'Current fraud count (' + CAST(@FraudCount AS VARCHAR) + ') differs from average (' + 
				CAST(@AvgFraudCount AS VARCHAR) + ') by more than ' + CAST(@ThresholdPercent AS VARCHAR) + '%',
				@FraudCount, 'Medium');
				
			SET @HasWarnings = 1;
		END
		ELSE
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Fraud Data Count', 'PASS', 
				'Fraud count within expected range: ' + CAST(@FraudCount AS VARCHAR) + ' records',
				@FraudCount, 'Low');
		END
	END

	-- Market data completeness validation
	IF @MarketCount = 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Completeness', 'Market Data', 'FAIL', 'Market table is empty', 0, 'Critical');
		
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		-- Get historical average count
		DECLARE @AvgMarketCount INT;
		SELECT @AvgMarketCount = AVG(RowsProcessed)
		FROM Staging.ETLLog
		WHERE StepName = 'LoadMarketData'
		AND Status = 'Loaded'
		AND ExecutionDateTime > DATEADD(MONTH, -1, GETDATE());
		
		-- Check if count within expected range
		IF @AvgMarketCount > 0 AND ABS(@MarketCount - @AvgMarketCount) > (@AvgMarketCount * @ThresholdPercent / 100)
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Market Data Count', 'WARN', 
				'Current market count (' + CAST(@MarketCount AS VARCHAR) + ') differs from average (' + 
				CAST(@AvgMarketCount AS VARCHAR) + ') by more than ' + CAST(@ThresholdPercent AS VARCHAR) + '%',
				@MarketCount, 'Medium');
				
			SET @HasWarnings = 1;
		END
		ELSE
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Market Data Count', 'PASS', 
				'Market count within expected range: ' + CAST(@MarketCount AS VARCHAR) + ' records',
				@MarketCount, 'Low');
		END
	END

	-- Macro data completeness validation
	IF @MacroCount = 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Completeness', 'Macro Data', 'FAIL', 'Macro table is empty', 0, 'Critical');
		
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		-- Get historical average count
		DECLARE @AvgMacroCount INT;
		SELECT @AvgMacroCount = AVG(RowsProcessed)
		FROM Staging.ETLLog
		WHERE StepName = 'LoadMacroData'
		AND Status = 'Loaded'
		AND ExecutionDateTime > DATEADD(MONTH, -1, GETDATE());
		
		-- Check if count within expected range
		IF @AvgMacroCount > 0 AND ABS(@MacroCount - @AvgMacroCount) > (@AvgMacroCount * @ThresholdPercent / 100)
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Macro Data Count', 'WARN', 
				'Current macro count (' + CAST(@MacroCount AS VARCHAR) + ') differs from average (' + 
				CAST(@AvgMacroCount AS VARCHAR) + ') by more than ' + CAST(@ThresholdPercent AS VARCHAR) + '%',
				@MacroCount, 'Medium');
				
			SET @HasWarnings = 1;
		END
		ELSE
		BEGIN
			INSERT INTO #ValidationResults 
				(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
			VALUES
				('Data Completeness', 'Macro Data Count', 'PASS', 
				'Macro count within expected range: ' + CAST(@MacroCount AS VARCHAR) + ' records',
				@MacroCount, 'Low');
		END
	END

	-- Data quality checks for Fraud data
	DECLARE @InvalidFraudCount INT;
	SELECT @InvalidFraudCount = COUNT(*)
		FROM Staging.Fraud
		WHERE TransactionDate > GETDATE();

	IF @InvalidFraudCount > 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Quality', 'Future Fraud Dates', 'FAIL', 
			'Found ' + CAST(@InvalidFraudCount AS VARCHAR) + ' fraud records with future dates',
			@InvalidFraudCount, 'High');
			
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Data Quality', 'Future Fraud Dates', 'PASS', 'No fraud records with future dates', 'Low');
	END

	-- Data quality checks for Macro data
	DECLARE @InvalidMacroCount INT;

	-- Check for negative unemployment rates
	SELECT @InvalidMacroCount = COUNT(*)
	FROM Staging.Macro
	WHERE UnemploymentRate < 0;

	IF @InvalidMacroCount > 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Quality', 'Negative Unemployment Rates', 'FAIL', 
			'Found ' + CAST(@InvalidMacroCount AS VARCHAR) + ' macro records with negative unemployment rates',
			@InvalidMacroCount, 'High');
			
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Data Quality', 'Negative Unemployment Rates', 'PASS', 'No negative unemployment rates found', 'Low');
	END

	-- Statistical pattern checks
	-- Fraud detection rate
	DECLARE @CurrentFraudRate DECIMAL(5,2);

	-- Calculate current fraud rate (fraud cases per loan)
	SELECT @CurrentFraudRate = CAST(@FraudCount AS DECIMAL(10,2)) / NULLIF(@LoanCount, 0) * 100;

	-- Get historical fraud rate from ETL logs
	DECLARE @HistoricalFraudRate DECIMAL(5,2);
	SELECT @HistoricalFraudRate = AVG(CAST(f.RowsProcessed AS DECIMAL(10,2)) / NULLIF(l.RowsProcessed, 0) * 100)
	FROM (
		SELECT ExecutionDateTime, RowsProcessed
		FROM Staging.ETLLog
		WHERE StepName = 'LoadFraudData'
		AND Status = 'Loaded'
		AND ExecutionDateTime > DATEADD(MONTH, -1, GETDATE())
	) f
	JOIN (
		SELECT ExecutionDateTime, RowsProcessed
		FROM Staging.ETLLog
		WHERE StepName = 'LoadLoanData'
		AND Status = 'Loaded'
		AND ExecutionDateTime > DATEADD(MONTH, -1, GETDATE())
	) l ON DATEDIFF(HOUR, f.ExecutionDateTime, l.ExecutionDateTime) BETWEEN -12 AND 12;

	-- Check if fraud rate differs significantly
	IF @HistoricalFraudRate > 0 AND 
	ABS(@CurrentFraudRate - @HistoricalFraudRate) > (@HistoricalFraudRate * @ThresholdPercent / 100)
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Statistical Patterns', 'Fraud Rate', 'WARN',
			'Current fraud rate (' + FORMAT(@CurrentFraudRate, 'N2') + '%' + 
			') differs from historical rate (' + FORMAT(@HistoricalFraudRate, 'N2') + '%' + 
			') by more than ' + CAST(@ThresholdPercent AS VARCHAR) + '%',
			'Medium');
			
		SET @HasWarnings = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Statistical Patterns', 'Fraud Rate', 'PASS',
			'Fraud rate (' + FORMAT(@CurrentFraudRate, 'N2') + '%' + 
			') within expected range of historical average',
			'Low');
	END

	-- Check economic indicators for consistency
	DECLARE @CurrentUnemployment DECIMAL(5,2);
	SELECT @CurrentUnemployment = AVG(UnemploymentRate) FROM Staging.Macro;

	-- Check for volatility within current unemployment data
	DECLARE @MinUnemployment DECIMAL(5,2), @MaxUnemployment DECIMAL(5,2);
		
	SELECT 
		@MinUnemployment = MIN(UnemploymentRate),
		@MaxUnemployment = MAX(UnemploymentRate)
	FROM Staging.Macro;

	IF @MaxUnemployment - @MinUnemployment > 5 -- More than 5 percentage points difference is suspicious
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Statistical Patterns', 'Unemployment Volatility', 'WARN',
			'High unemployment volatility detected: range from ' + FORMAT(@MinUnemployment, 'N2') + 
			'% to ' + FORMAT(@MaxUnemployment, 'N2') + '%',
			'Medium');
			
		SET @HasWarnings = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Statistical Patterns', 'Unemployment Volatility', 'PASS',
			'Unemployment rates within reasonable range: ' + FORMAT(@MinUnemployment, 'N2') + 
			'% to ' + FORMAT(@MaxUnemployment, 'N2') + '%',
			'Low');
	END
	
	
	-- ============================
	-- 3. VALIDATION: DATA QUALITY
	-- ============================
	
	-- Check for potential data quality issues in Staging.Loan
	DECLARE @InvalidLoanCount INT;
	
	-- Find negative loan amounts
	SELECT @InvalidLoanCount = COUNT(*)
	FROM Staging.Loan
	WHERE LoanAmount <= 0;
	
	IF @InvalidLoanCount > 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Quality', 'Negative Loan Amounts', 'FAIL', 
			'Found ' + CAST(@InvalidLoanCount AS VARCHAR) + ' loans with negative or zero amounts',
			@InvalidLoanCount, 'High');
			
		SET @HasErrors = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Data Quality', 'Negative Loan Amounts', 'PASS', 'No loans with negative or zero amounts found', 'Low');
	END
	
	-- Check for unreasonable interest rates
	SELECT @InvalidLoanCount = COUNT(*)
	FROM Staging.Loan
	WHERE InterestRate > 30 OR InterestRate < 0;
	
	IF @InvalidLoanCount > 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Data Quality', 'Unusual Interest Rates', 'WARN', 
			'Found ' + CAST(@InvalidLoanCount AS VARCHAR) + ' loans with interest rates < 0% or > 30%',
			@InvalidLoanCount, 'Medium');
			
		SET @HasWarnings = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Data Quality', 'Unusual Interest Rates', 'PASS', 'All interest rates within reasonable range', 'Low');
	END
	
	-- Check for referential integrity (i.e. Customer IDs exist across systems)
	DECLARE @OrphanedIds INT;
	
	SELECT @OrphanedIds = COUNT(DISTINCT f.CustomerID)
	FROM Staging.Fraud f
	LEFT JOIN Staging.Loan l ON f.CustomerID = l.CustomerID
	WHERE l.CustomerID IS NULL;
	
	IF @OrphanedIds > 0
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, AffectedRows, Severity)
		VALUES
			('Referential Integrity', 'Orphaned Customer IDs', 'WARN', 
			'Found ' + CAST(@OrphanedIds AS VARCHAR) + ' customers in Fraud table not in Loan table',
			@OrphanedIds, 'Medium');
			
		SET @HasWarnings = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Referential Integrity', 'Orphaned Customer IDs', 'PASS', 'All Customer IDs have matching records', 'Low');
	END
	

	-- ===================================
	-- 4. VALIDATION: STATISTICAL PATTERNS
	-- ===================================
	
	-- Check for unusual patterns in loan amounts (e.g. unexpected distributions)
	DECLARE @CurrentAvgLoanAmount DECIMAL(18,2), @HistoricalAvgLoanAmount DECIMAL(18,2);
	
	-- Get current average 
	SELECT @CurrentAvgLoanAmount = AVG(LoanAmount) FROM Staging.Loan;
	
	-- Get historical average from a previous successful ETL run
	SELECT @HistoricalAvgLoanAmount = AVG(LoanAmount)
	FROM Staging.Loan;
		
	-- Check if average differs significantly
	IF @HistoricalAvgLoanAmount > 0 AND 
	ABS(@CurrentAvgLoanAmount - @HistoricalAvgLoanAmount) > (@HistoricalAvgLoanAmount * @ThresholdPercent / 100)
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Statistical Patterns', 'Loan Amount Distribution', 'WARN',
			'Current average loan amount ($' + FORMAT(@CurrentAvgLoanAmount, 'N2') + 
			') differs from historical average ($' + FORMAT(@HistoricalAvgLoanAmount, 'N2') + 
			') by more than ' + CAST(@ThresholdPercent AS VARCHAR) + '%',
			'Medium');
			
		SET @HasWarnings = 1;
	END
	ELSE
	BEGIN
		INSERT INTO #ValidationResults 
			(ValidationCategory, ValidationCheck, Status, Description, Severity)
		VALUES
			('Statistical Patterns', 'Loan Amount Distribution', 'PASS',
			'Average loan amount ($' + FORMAT(@CurrentAvgLoanAmount, 'N2') + 
			') within expected range of historical average',
			'Low');
	END
	

	-- ==========================
	-- 5. SAVE VALIDATION RESULTS
	-- ==========================
	
	-- Create a permanent record of validation results
	INSERT INTO Staging.ValidationLog (
		BatchID, 
		ExecutionDateTime,
		ValidationCategory,
		DataSource,
		ErrorType,
		Severity,
		ErrorCount,
		SampleValues,
		IsBlocking
	)
	SELECT
		@ValidationBatchId,
		@ValidationDate,
		ValidationCategory,
		ValidationCheck,
		Description,
		Severity,
		AffectedRows,
		NULL, -- SampleValues would be populated with actual examples in a production system
		CASE WHEN Status = 'FAIL' AND Severity = 'Critical' THEN 1 ELSE 0 END
	FROM #ValidationResults
	WHERE Status <> 'PASS';
	

	-- ======================================
	-- 6. DETERMINE OVERALL VALIDATION STATUS
	-- ======================================
	
	-- Return summary results to the calling process
	IF @HasErrors = 1
	BEGIN
		IF @FailOnError = 1
		BEGIN
			-- Format error message for display
			SELECT @ErrorMessage = STRING_AGG(ValidationCheck + ': ' + Description, CHAR(13))
			FROM #ValidationResults
			WHERE Status = 'FAIL';
			
			-- Log validation failure
			INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status, ErrorMessage)
			VALUES ('usp_PreProductionValidation', 'Post-load validation', 'Failed', @ErrorMessage);
			
			-- Return results
			SELECT 'FAIL' AS Status, 'Critical validation errors detected' AS Message;
			
			-- Fail with error
			RAISERROR('Pre-production validation failed: %s', 16, 1, @ErrorMessage);
			RETURN;
		END
		ELSE
		BEGIN
			-- Log validation warning
			INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status, ErrorMessage)
			VALUES ('usp_PreProductionValidation', 'Post-load validation', 'Warning', 'Validation errors detected but proceeding due to @FailOnError=0');
			
			-- Return results with warning
			SELECT 'WARN' AS Status, 'Validation errors detected but continuing due to override setting' AS Message;
		END
	END
	ELSE IF @HasWarnings = 1
	BEGIN
		-- Log validation with warnings
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_PreProductionValidation', 'Post-load validation', 'Warning');
		
		-- Return results with warning
		SELECT 'WARN' AS Status, 'Validation completed with warnings' AS Message;
	END
	ELSE
	BEGIN
		-- Log successful validation
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_PreProductionValidation', 'Post-load validation', 'Success');
		
		-- Return success results
		SELECT 'PASS' AS Status, 'All validation checks passed successfully' AS Message;
	END
	
	-- Return detailed validation results
	SELECT 
		ValidationCategory,
		ValidationCheck,
		Status,
		Description,
		AffectedRows,
		Severity
	FROM #ValidationResults
	ORDER BY 
		CASE Status 
			WHEN 'FAIL' THEN 1 
			WHEN 'WARN' THEN 2 
			WHEN 'PASS' THEN 3 
		END,
		CASE Severity
			WHEN 'Critical' THEN 1
			WHEN 'High' THEN 2
			WHEN 'Medium' THEN 3
			WHEN 'Low' THEN 4
		END;
		
	-- Clean up
	DROP TABLE #ValidationResults;
END;
GO