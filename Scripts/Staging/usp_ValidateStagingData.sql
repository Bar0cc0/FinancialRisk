/* 
This script creates a stored procedure to validate data in the staging area.
Last updated: 2025-01-01
*/



CREATE OR ALTER PROCEDURE Staging.usp_ValidateStagingData
	@LogPath NVARCHAR(500),
	@BatchID UNIQUEIDENTIFIER = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;

	-- Set default values for BatchID
	SET @BatchID = ISNULL(@BatchID, NEWID());
	
	-- Variables for tracking execution
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @StartTime DATETIME = GETDATE();
	
	-- Table variable for storing validation errors
	DECLARE @ValidationErrors TABLE (
		DataSource VARCHAR(50),
		ErrorType VARCHAR(100),
		ErrorCount INT
	);
	
	
	BEGIN TRY
		-- Insert start log entry
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_ValidateStagingData', 'Pre-load validation', 'Started');
		
		-- ==============================
		-- Basic Data Quality Validations
		-- ==============================
		
		-- 1. Missing key values
		-- Check for null CustomerIDs in loan data
		INSERT INTO @ValidationErrors
		SELECT 'Loan', 'Null CustomerIDs', COUNT(*)
		FROM Staging.Loan 
		WHERE CustomerID IS NULL;
		
		-- Check for null TransactionIDs in fraud data
		INSERT INTO @ValidationErrors
		SELECT 'Fraud', 'Null TransactionIDs', COUNT(*)
		FROM Staging.Fraud
		WHERE TransactionID IS NULL;
		
		-- 2. Range validations
		-- Check for negative interest rates
		INSERT INTO @ValidationErrors
		SELECT 'Loan', 'Negative Interest Rates', COUNT(*)
		FROM Staging.Loan 
		WHERE InterestRate < 0;
		
		-- Check for very high loan amounts (potential outliers)
		INSERT INTO @ValidationErrors
		SELECT 'Loan', 'Suspiciously High Loan Amounts', COUNT(*)
		FROM Staging.Loan 
		WHERE LoanAmount > 1000000; -- $1 million threshold
		
		-- =========================
		-- Business Rule Validations
		-- =========================
		
		-- 1. Monthly payment validation
		INSERT INTO @ValidationErrors
		SELECT 'Loan', 'Invalid Monthly Payment', COUNT(*)
		FROM Staging.Loan
		WHERE MonthlyPayment > LoanAmount;
		
		-- 2. Duplicate checking
		INSERT INTO @ValidationErrors
		SELECT 'Fraud', 'Duplicate Transactions', COUNT(*) - COUNT(DISTINCT TransactionID)
		FROM Staging.Fraud
		WHERE TransactionID IS NOT NULL;
		
		-- 3. Date range validations
		INSERT INTO @ValidationErrors
		SELECT 'Market', 'Future Dates', COUNT(*)
		FROM Staging.Market
		WHERE CAST(MarketDate AS DATE) > CAST(GETDATE() AS DATE);
		
		-- ================================
		-- Advanced Statistical Validations
		-- ================================
		
		-- 1. Outlier detection (Z-Score)
		WITH LoanStats AS (
			SELECT 
				AVG(InterestRate) AS AvgRate,
				STDEV(InterestRate) AS StdDevRate
			FROM Staging.Loan
			WHERE InterestRate IS NOT NULL
		)
		INSERT INTO @ValidationErrors
		SELECT 'Loan', 'Statistical Interest Rate Outliers', COUNT(*)
		FROM Staging.Loan l
		CROSS JOIN LoanStats s
		WHERE ABS(l.InterestRate - s.AvgRate) > 3 * s.StdDevRate;
		
		-- ======================
		-- Log validation results
		-- ======================
		INSERT INTO Staging.ValidationLog
			(BatchID, ValidationCategory, DataSource, ErrorType, ErrorCount, SampleValues)
		SELECT 
			@BatchID,
			-- Categorize the validation errors
			CASE
				WHEN ErrorType LIKE '%Null%' THEN 'Missing Values'
				WHEN ErrorType LIKE '%Negative%' THEN 'Range Validation'
				WHEN ErrorType LIKE '%High%' THEN 'Outlier Detection'
				WHEN ErrorType LIKE '%Invalid%' THEN 'Business Rules'
				WHEN ErrorType LIKE '%Duplicate%' THEN 'Uniqueness Check'
				WHEN ErrorType LIKE '%Future%' THEN 'Date Validation'
				WHEN ErrorType LIKE '%Statistical%' THEN 'Statistical Validation'
				ELSE 'Other'
			END AS ValidationCategory,
			DataSource,
			ErrorType,
			ErrorCount,
			CASE
				-- Sample the first 5 problematic values for each error type
				WHEN DataSource = 'Loan' AND ErrorType = 'Null CustomerIDs' THEN
					(SELECT TOP 5 CONCAT('LoanID: ', LoanID) AS loanID FROM Staging.Loan WHERE CustomerID IS NULL FOR JSON PATH)
				WHEN DataSource = 'Loan' AND ErrorType = 'Negative Interest Rates' THEN
					(SELECT TOP 5 CONCAT('LoanID: ', LoanID, ', Rate: ', InterestRate) AS loanID
					FROM Staging.Loan WHERE InterestRate < 0 FOR JSON PATH)
				ELSE NULL
			END AS SampleValues
		FROM @ValidationErrors
		WHERE ErrorCount > 0;
		
		-- Create summary message for ETL log
		DECLARE @ValidationMsg NVARCHAR(MAX) = '';
			
		SELECT @ValidationMsg = @ValidationMsg + 
			DataSource + ': ' + ErrorType + ' - ' + CAST(ErrorCount AS VARCHAR) + CHAR(13)
		FROM @ValidationErrors
		WHERE ErrorCount > 0;
		
		-- Log overall validation status
		IF EXISTS (SELECT 1 FROM @ValidationErrors WHERE ErrorCount > 0)
		BEGIN
			INSERT INTO Staging.ETLLog 
				(ProcedureName, StepName, Status, ErrorMessage, ExecutionTimeSeconds)
			VALUES 
				('usp_ValidateStagingData', 'Pre-load validation', 'Warning', 
				@ValidationMsg, DATEDIFF(SECOND, @StartTime, GETDATE()));
		END
		ELSE
		BEGIN
			INSERT INTO Staging.ETLLog 
				(ProcedureName, StepName, Status, ExecutionTimeSeconds)
			VALUES 
				('usp_ValidateStagingData', 'Pre-load validation', 'Success', 
				DATEDIFF(SECOND, @StartTime, GETDATE()));
		END
		
		-- Return validation summary
		SELECT 
			'Validation Complete' AS Status,
			(SELECT COUNT(*) FROM @ValidationErrors WHERE ErrorCount > 0) AS TotalIssueTypes,
			(SELECT SUM(ErrorCount) FROM @ValidationErrors) AS TotalIssueCount;
		
		-- Return detailed validation issues
		SELECT 
			DataSource,
			ErrorType,
			ErrorCount
		FROM @ValidationErrors
		WHERE ErrorCount > 0
		ORDER BY ErrorCount DESC;
	
		-- Return overall validation status (0 = Success, >0 = Warning/Failures)
		DECLARE @ReturnValue INT = (SELECT COUNT(*) FROM @ValidationErrors WHERE ErrorCount > 0);
		RETURN @ReturnValue;
	
	END TRY
	BEGIN CATCH
		-- Get error information
		SELECT @ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
		
		-- Log the error
		INSERT INTO Staging.ETLLog
			(ProcedureName, StepName, Status, ErrorMessage)
		VALUES
			('usp_ValidateStagingData', 'Pre-load validation', 'Failed', @ErrorMessage);
			
		-- Re-throw the error
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH;
END;
GO
