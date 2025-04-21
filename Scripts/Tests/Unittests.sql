-- ===========================================
-- Unit tests for Financial Risk ETL Pipeline
-- ===========================================
SET NOCOUNT ON;

PRINT '===============================================';
PRINT 'UNIT TESTING - FINANCIAL RISK ETL PIPELINE';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';

BEGIN TRY
	-- Test 1: Staging environment creation
	PRINT 'Test 1: Creating staging environment...';
	EXEC Staging.usp_CreateStagingTables
		@LogPath = N'$(LogPath)';

	-- Verify staging tables creation
	SELECT 'Staging Tables' AS TestCase, 
		TABLE_NAME, 
		TABLE_SCHEMA
	FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_SCHEMA = 'Staging'
	ORDER BY TABLE_NAME;

	-- Test 2: Data loading to staging with a small sample
	PRINT 'Test 2: Loading sample data to staging...';
	EXEC Staging.usp_LoadStagingTables 
		@DataPath = N'$(DataPath)',
		@LogPath = N'$(LogPath)',
		@ScriptsPath = N'$(ScriptsPath)';

	-- Verify data was loaded
	SELECT 'Loan Data' AS TestCase, COUNT(*) AS RecordCount FROM Staging.Loan;
	SELECT 'Market Data' AS TestCase, COUNT(*) AS RecordCount FROM Staging.Market;
	SELECT 'Fraud Data' AS TestCase, COUNT(*) AS RecordCount FROM Staging.Fraud;
	SELECT 'Macro Data' AS TestCase, COUNT(*) AS RecordCount FROM Staging.Macro;

	-- Test 3: Data validation
	PRINT 'Test 3: Validating staging data...';
	DECLARE @UnitTestBatchID UNIQUEIDENTIFIER = NEWID();
	PRINT 'Unit Test BatchID: ' + CAST(@UnitTestBatchID AS VARCHAR(50));
	
	EXEC Staging.usp_ValidateStagingData
		@LogPath = N'$(LogPath)',
		@BatchID = @UnitTestBatchID;

	-- Verify validation results
	SELECT TOP 10
		'Validation Results' AS TestCase, 
		ValidationCategory,
		DataSource,
		ErrorType,
		Severity,
		ErrorCount,
		ExecutionDateTime
	FROM Staging.ValidationLog 
	ORDER BY ExecutionDateTime DESC;

	-- Test 4: Production schema creation
	PRINT 'Test 4: Creating production tables...';
	
	-- Turn off system versioning on temporal tables first
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCustomer' 
	AND schema_id = SCHEMA_ID('Production') 
	AND temporal_type = 2) -- 2 = SYSTEM_VERSIONED_TEMPORAL_TABLE
	BEGIN
		ALTER TABLE Production.DimCustomer SET (SYSTEM_VERSIONING = OFF);
	END

	-- Disable foreign key constraints before recreation
	EXEC sp_executesql N'
		DECLARE @SQL NVARCHAR(MAX) = N'''';
		SELECT @SQL = @SQL + N''ALTER TABLE '' + QUOTENAME(s.name) + N''.'' + QUOTENAME(t.name) + N'' NOCHECK CONSTRAINT ALL; ''
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = N''Production'';
		EXEC sp_executesql @SQL;
		';

	-- Drop tables in the correct order if they exist
	-- First, drop all fact tables (which reference dimension tables)
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactRiskProfile' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactRiskProfile;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactLoan' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactLoan;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactCustomer' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactCustomer;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactCredit' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactCredit;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactMarket' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactMarket;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactAccount' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactAccount;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactEconomicIndicators' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.FactEconomicIndicators;

	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactFraud' AND schema_id = SCHEMA_ID('Production'))
	BEGIN
		-- Drop any foreign keys first
		DECLARE @DropFK NVARCHAR(MAX) = N'';
		SELECT @DropFK = @DropFK + N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + 
								N'.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + 
								N' DROP CONSTRAINT ' + QUOTENAME(name) + N'; '
		FROM sys.foreign_keys
		WHERE referenced_object_id = OBJECT_ID('Production.FactFraud');
		
		IF LEN(@DropFK) > 0
			EXEC sp_executesql @DropFK;
			
		DROP TABLE Production.FactFraud;
	END
	
	-- Then drop dimensions with foreign keys to other dimensions
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimTransaction' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimTransaction;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimAccount' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimAccount;

	-- Drop all remaining dimension tables
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCustomer' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimCustomer;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCustomerHistory' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimCustomerHistory;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimLoanType' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimLoanType;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimTransactionType' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimTransactionType;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCountry' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimCountry;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimMarketIndex' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimMarketIndex;
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimDate' AND schema_id = SCHEMA_ID('Production'))
		DROP TABLE Production.DimDate;

	-- Now create the production tables
	EXEC Production.usp_CreateProductionTables
		@LogPath = N'$(LogPath)',
		@EnableMemoryOptimized = 0,
		@EnableColumnstoreIndexes = 0;

	-- Verify production tables
	SELECT 'Production Tables' AS TestCase,
		TABLE_NAME,
		TABLE_SCHEMA
	FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_SCHEMA = 'Production'
	ORDER BY TABLE_NAME;

	-- Test 5: Date dimension population
	PRINT 'Test 5: Populating date dimension...';
	EXEC Production.usp_PopulateDateDimension
		@StartDate = '2023-01-01',
		@EndDate = '2025-12-31';

	-- Verify date dimension
	SELECT 'Date Dimension' AS TestCase,
		MIN(FullDate) AS MinDate,
		MAX(FullDate) AS MaxDate,
		COUNT(*) AS TotalDates
	FROM Production.DimDate;

	-- Test 6: Loading dimensions and facts
	PRINT 'Test 6: Loading dimensions and facts...';
	DECLARE @LoadBatchID UNIQUEIDENTIFIER = NEWID();
	
	EXEC Production.usp_LoadProductionTables 
		@BatchID = @LoadBatchID,
		@LogPath = N'$(LogPath)',
		@LoadDimensions = 1,
		@LoadFacts = 1;

	-- Verify dimension and fact loads
	SELECT 'DimCustomer' AS TestTable, COUNT(*) AS RecordCount FROM Production.DimCustomer;
	SELECT 'DimLoanType' AS TestTable, COUNT(*) AS RecordCount FROM Production.DimLoanType;
	SELECT 'DimAccount' AS TestTable, COUNT(*) AS RecordCount FROM Production.DimAccount;
	SELECT 'FactLoan' AS TestTable, COUNT(*) AS RecordCount FROM Production.FactLoan;
	SELECT 'FactAccount' AS TestTable, COUNT(*) AS RecordCount FROM Production.FactAccount;
	SELECT 'FactCredit' AS TestTable, COUNT(*) AS RecordCount FROM Production.FactCredit;

	-- Test 7: Risk score calculation for a sample customer
	PRINT 'Test 7: Testing risk score calculation...';
	
	-- Only run if we have customer data
	IF EXISTS (SELECT 1 FROM Production.DimCustomer) AND EXISTS (SELECT 1 FROM Production.DimDate)
	BEGIN
		DECLARE @CustomerSK INT = (SELECT TOP 1 CustomerSK FROM Production.DimCustomer WHERE IsCurrent = 1);
		DECLARE @DateID INT = (SELECT TOP 1 DateID FROM Production.DimDate WHERE FullDate <= GETDATE());
		
		IF @CustomerSK IS NOT NULL AND @DateID IS NOT NULL
		BEGIN
			SELECT 
				@CustomerSK AS CustomerSK, 
				@DateID AS DateID, 
				Production.CalculateCreditRiskScore(@CustomerSK, @DateID) AS CreditRiskScore,
				Production.CalculateFraudRiskScore(@CustomerSK, @DateID) AS FraudRiskScore,
				Production.CalculateMarketRiskScore(@CustomerSK, @DateID) AS MarketRiskScore,
				Production.CalculateAggregateRiskScore(@CustomerSK, @DateID) AS AggregateRiskScore;
				
			-- Test 8: Update Risk Scores for all customers
			PRINT 'Test 8: Updating risk scores...';
			EXEC Production.usp_UpdateRiskScores @DateID = @DateID;
			
			-- Verify risk profile data
			SELECT 
				'Risk Profiles' AS TestCase,
				COUNT(*) AS TotalProfiles,
				MIN(AggregateRiskScore) AS MinRiskScore,
				MAX(AggregateRiskScore) AS MaxRiskScore,
				AVG(AggregateRiskScore) AS AvgRiskScore
			FROM Production.FactRiskProfile;
		END
		ELSE
		BEGIN
			PRINT 'Skipping risk score tests - no valid customer and/or date found';
		END
	END
	ELSE
	BEGIN
		PRINT 'Skipping risk score tests - customer or date dimension empty';
	END
	
	-- Test 9: Check ETL logging
	SELECT TOP 10 
		'ETL Log' AS TestCase,
		ProcedureName,
		StepName,
		Status,
		RowsProcessed,
		ExecutionTimeSeconds,
		ExecutionDateTime
	FROM Production.ETLLog
	ORDER BY ExecutionDateTime DESC;
	
	PRINT 'Unit tests completed successfully!';
END TRY
BEGIN CATCH
	PRINT 'Unit test failed: ' + ERROR_MESSAGE();
END CATCH