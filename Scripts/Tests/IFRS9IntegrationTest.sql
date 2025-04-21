SET NOCOUNT ON;

PRINT '===============================================';
PRINT 'IFRS9 INTEGRATION TEST';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';

DECLARE @ErrorCount INT = 0;
DECLARE @TestCount INT = 0;

BEGIN TRY
	-- Test 1: Verify IFRS9 schema exists
	SET @TestCount += 1;
	IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'IFRS9')
		PRINT 'Test 1: IFRS9 schema exists - PASSED';
	ELSE
	BEGIN
		PRINT 'Test 1: IFRS9 schema does not exist - FAILED';
		SET @ErrorCount += 1;
	END

	-- Test 2: Verify core tables exist
	SET @TestCount += 1;
	IF (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('IFRS9')) >= 5
		PRINT 'Test 2: IFRS9 core tables exist - PASSED';
	ELSE
	BEGIN
		PRINT 'Test 2: Missing IFRS9 core tables - FAILED';
		SET @ErrorCount += 1;
	END

	-- Test 3: Verify core procedures exist
	SET @TestCount += 1;
	IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteProcessIFRS9' AND schema_id = SCHEMA_ID('IFRS9'))
		PRINT 'Test 3: Core IFRS9 procedures exist - PASSED';
	ELSE
	BEGIN
		PRINT 'Test 3: Missing core IFRS9 procedures - FAILED';
		SET @ErrorCount += 1;
	END

	-- Test 4: Verify ETL integration exists
	SET @TestCount += 1;
	IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteProductionETLWithIFRS9' AND schema_id = SCHEMA_ID('Production'))
		PRINT 'Test 4: ETL integration procedure exists - PASSED';
	ELSE
	BEGIN
		PRINT 'Test 4: ETL integration procedure does not exist - FAILED';
		SET @ErrorCount += 1;
	END

	-- Test 5: Execute a basic ECL staging classification
	SET @TestCount += 1;
	BEGIN TRY
		-- Sample execution of ECL staging
		DECLARE @SampleDate DATE = GETDATE();
		DECLARE @SampleBatchID UNIQUEIDENTIFIER = NEWID();
		
		-- Execute classification with error handling
		BEGIN TRY
			EXEC IFRS9.usp_ClassifyECLStaging
				@AsOfDate = @SampleDate,
				@BatchID = @SampleBatchID;
				
			PRINT 'Test 5: ECL staging classification execution - PASSED';
		END TRY
		BEGIN CATCH
			PRINT 'Test 5: ECL staging classification execution - FAILED';
			PRINT '       Error: ' + ERROR_MESSAGE();
			SET @ErrorCount += 1;
		END CATCH
	END TRY
	BEGIN CATCH
		PRINT 'Test 5: ECL staging classification execution - FAILED';
		PRINT '       Error: ' + ERROR_MESSAGE();
		SET @ErrorCount += 1;
	END CATCH

	-- Test 6: Check for views
	SET @TestCount += 1;
	IF EXISTS (SELECT 1 FROM sys.views WHERE schema_id = SCHEMA_ID('IFRS9'))
		PRINT 'Test 6: IFRS9 views exist - PASSED';
	ELSE
	BEGIN
		PRINT 'Test 6: No IFRS9 views found - FAILED';
		SET @ErrorCount += 1;
	END

	-- Test 7: Verify SQL Agent job exists
	SET @TestCount += 1;
	IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'FinancialRiskDB_IFRS9_Process')
		PRINT 'Test 7: IFRS9 SQL Agent job exists - PASSED';
	ELSE
	BEGIN
		PRINT 'Test 7: IFRS9 SQL Agent job does not exist - FAILED';
		SET @ErrorCount += 1;
	END

	-- Summary
	PRINT '===============================================';
	PRINT 'IFRS9 Integration Test Summary';
	PRINT '-----------------------------------------------';
	PRINT 'Total Tests: ' + CAST(@TestCount AS VARCHAR);
	PRINT 'Passed: ' + CAST(@TestCount - @ErrorCount AS VARCHAR);
	PRINT 'Failed: ' + CAST(@ErrorCount AS VARCHAR);
	
	IF @ErrorCount = 0
		PRINT 'OVERALL RESULT: PASSED';
	ELSE
		PRINT 'OVERALL RESULT: FAILED - See above for details';
	
	PRINT '===============================================';
END TRY
BEGIN CATCH
	PRINT 'Test execution failed with error: ' + ERROR_MESSAGE();
END CATCH
