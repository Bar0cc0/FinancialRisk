/* 
This script creates a procedure for creating the Staging schema tables.
Last updated: 2025-01-01
*/

-- Create helper procedures for job control
CREATE OR ALTER PROCEDURE Staging.usp_StartJobStep
	@JobName VARCHAR(100),
	@StepName VARCHAR(100),
	@BatchID UNIQUEIDENTIFIER = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	IF @BatchID IS NULL
	BEGIN
		-- Get the latest batch ID for this job
		SELECT TOP 1 @BatchID = BatchID 
		FROM Staging.JobControl 
		WHERE JobName = @JobName 
		ORDER BY CreatedDate DESC;
	END
	
	-- Mark step as started
	UPDATE Staging.JobControl
	SET Status = 'Running',
		StartDateTime = GETDATE()
	WHERE JobName = @JobName
	AND StepName = @StepName
	AND BatchID = @BatchID;
	
	-- Return the BatchID
	SELECT @BatchID AS BatchID;
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_CompleteJobStep
	@JobName VARCHAR(100),
	@StepName VARCHAR(100),
	@BatchID UNIQUEIDENTIFIER,
	@Status VARCHAR(20) = 'Success',
	@RowsProcessed INT = NULL,
	@ErrorMessage NVARCHAR(4000) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	
	-- Mark step as completed
	UPDATE Staging.JobControl
	SET Status = @Status,
		EndDateTime = GETDATE(),
		RowsProcessed = @RowsProcessed,
		ErrorMessage = @ErrorMessage
	WHERE JobName = @JobName
	AND StepName = @StepName
	AND BatchID = @BatchID;
	
	-- If error and retries available, update status
	IF @Status = 'Failed' 
	BEGIN
		DECLARE @RetryCount INT, @MaxRetries INT;
		
		SELECT @RetryCount = RetryCount, @MaxRetries = MaxRetries
		FROM Staging.JobControl
		WHERE JobName = @JobName
		AND StepName = @StepName
		AND BatchID = @BatchID;
		
		IF @RetryCount < @MaxRetries
		BEGIN
			UPDATE Staging.JobControl
			SET Status = 'Retry',
				RetryCount = RetryCount + 1
			WHERE JobName = @JobName
			AND StepName = @StepName
			AND BatchID = @BatchID;
		END
	END
END;
GO



-- ======================================
-- Procedure to create the staging tables
-- ======================================
CREATE OR ALTER PROCEDURE Staging.usp_CreateStagingTables
	@LogPath NVARCHAR(500)
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Variables for tracking execution
	DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
	DECLARE @CurrentStep NVARCHAR(128);
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @StepStartTime DATETIME;
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @TableCount INT = 0;
	DECLARE @IndexCount INT = 0;
	
	BEGIN TRY
		
		-- Enable xp_cmdshell if needed for external operations
		IF NOT EXISTS (SELECT * FROM sys.configurations 
						WHERE name = 'xp_cmdshell' AND value_in_use = 1)
		BEGIN
			EXEC sp_configure 'show advanced options', 1;  
			RECONFIGURE;
			EXEC sp_configure 'xp_cmdshell', 1;
			RECONFIGURE;
		END;

		-- ======================================
		-- 1. Create ETL control framework tables
		-- ======================================
		
		-- 1.1 ETL Logging table
		SET @CurrentStep = 'ETLLog';

		IF OBJECT_ID('Staging.ETLLog') IS NOT NULL
			DROP TABLE Staging.ETLLog;    

		CREATE TABLE Staging.ETLLog (
			LogID INT IDENTITY(1,1) PRIMARY KEY,
			ProcedureName VARCHAR(128) NOT NULL,
			StepName VARCHAR(128) NULL,
			ExecutionDateTime DATETIME NOT NULL DEFAULT GETDATE(),
			Status VARCHAR(20) NOT NULL,
			ErrorMessage NVARCHAR(4000) NULL,
			RowsProcessed INT NULL,
			ExecutionTimeSeconds INT NULL,
			FileName VARCHAR(255) NULL,
			Environment VARCHAR(50) NULL
		);
		
		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateStagingTables', @CurrentStep, 'Success');
		
		SET @TableCount += 1;

		-- 1.2 Job Control table for tracking pipeline execution
		SET @CurrentStep = 'JobControl';

		IF OBJECT_ID('Staging.JobControl') IS NOT NULL
			DROP TABLE Staging.JobControl;    

		CREATE TABLE Staging.JobControl (
			JobID INT IDENTITY(1,1) PRIMARY KEY,
			JobName VARCHAR(100) NOT NULL,
			BatchID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
			StepName VARCHAR(100) NULL,
			StepOrder INT NULL,
			StartDateTime DATETIME NULL,
			EndDateTime DATETIME NULL,
			Status VARCHAR(20) NOT NULL DEFAULT 'Pending',
			RowsProcessed INT NULL,
			ErrorMessage NVARCHAR(4000) NULL,
			RetryCount INT NOT NULL DEFAULT 0,
			MaxRetries INT NOT NULL DEFAULT 3,
			CreatedDate DATETIME NOT NULL DEFAULT GETDATE(),
			CONSTRAINT UQ_JobControl_BatchStep UNIQUE (BatchID, StepName)
		);

		INSERT INTO Staging.JobControl (JobName, StepName, Status)
		VALUES ('usp_CreateStagingTables', @CurrentStep, 'Success');

		SET @TableCount += 1;
		
		-- 1.3 ETL Validation Results table
		SET @CurrentStep = 'ValidationLog';

		IF OBJECT_ID('Staging.ValidationLog') IS NOT NULL
			DROP TABLE Staging.ValidationLog;    
		
		CREATE TABLE Staging.ValidationLog (
			LogID INT IDENTITY(1,1) PRIMARY KEY,
			BatchID UNIQUEIDENTIFIER NOT NULL,
			ExecutionDateTime DATETIME NOT NULL DEFAULT GETDATE(),
			ValidationCategory VARCHAR(50) NOT NULL,
			DataSource VARCHAR(50) NOT NULL,
			ErrorType VARCHAR(100) NOT NULL,
			Severity VARCHAR(20) NOT NULL DEFAULT 'Warning', -- Critical, Error, Warning, Info
			ErrorCount INT NULL,
			SampleValues NVARCHAR(MAX) NULL,
			IsBlocking INT NOT NULL DEFAULT 0   -- Determines if pipeline should stop
		);

		INSERT INTO Staging.ValidationLog (BatchID, ValidationCategory, DataSource, ErrorType, ErrorCount, IsBlocking)
		VALUES (@BatchID, 'Setup', 'System', 'Information', 0, 0);

		SET @TableCount += 1;
		
		-- Begin transaction for atomic operation
		IF @@TRANCOUNT = 0
		BEGIN TRANSACTION;
		-- ======================================================
		-- 2. Create staging tables matching cooked CSV structure
		-- ======================================================
		
		-- 2.1 Staging Loan table
		SET @CurrentStep = 'Loan';

		IF OBJECT_ID('Staging.Loan') IS NOT NULL
			DROP TABLE Staging.Loan;    
		
		CREATE TABLE Staging.Loan (
			-- Business keys 
			CustomerID INT NOT NULL,
			LoanID NVARCHAR(50) NOT NULL,
			
			-- Customer attributes referenced in DimCustomer
			Age INT NOT NULL,
			JobTitle VARCHAR(50) NULL,
			EmploymentStatus INT NOT NULL DEFAULT 0, -- 0 = Unemployed, 1 = Employed
			HomeOwnershipStatus VARCHAR(50) NOT NULL,
			MaritalStatus VARCHAR(50) NOT NULL,
			PreviousLoanDefault INT NOT NULL DEFAULT 0,

			-- Customer financial attributes referenced in FactCustomer
			AnnualIncome DECIMAL(18,2) NOT NULL,
			TotalAssets DECIMAL(18,2) NOT NULL,
			TotalLiabilities DECIMAL(18,2) NOT NULL,
			AnnualExpenses DECIMAL(18,2) NULL,
			MonthlySavings DECIMAL(18,2) NULL,
			AnnualBonus DECIMAL(18,2),
			DebtToIncomeRatio DECIMAL(18,2) NULL,
			PaymentHistoryYears INT,
			JobTenureMonths INT,
			NumDependents INT NULL,

			-- Loan attributes referenced in DimLoanType
			LoanType NVARCHAR(50) NOT NULL,  -- {Personal, Auto, Home, Student, Business}

			-- Loan attributes referenced in FactLoan
			LoanDate DATE NOT NULL,
			LoanAmount DECIMAL(18,2) NOT NULL,
			LoanDurationMonths INT NOT NULL,
			InterestRate DECIMAL(18,2) NOT NULL,
			DelayedPayments INT NOT NULL DEFAULT 0,
			Balance DECIMAL(18,2) NOT NULL,
			MonthlyPayment DECIMAL(18,2) NOT NULL,
			LoanToValueRatio DECIMAL(18,2) NULL,
			PaymentStatus NVARCHAR(20) NOT NULL DEFAULT 'On Time', -- {On Time, Late, Defaulted}
			
			-- Account attributes referenced in DimAccount
			AccountID NVARCHAR(50) NOT NULL,
			AccountType NVARCHAR(50) NOT NULL,		-- {Checking, Savings, Investment, Retirement, Emergency Fund, Credit Card}
			PaymentBehavior NVARCHAR(50) NOT NULL, 	-- {High spent Medium value, Low spent High value, etc.}
			
			-- Account attributes referenced in FactAccount
			AccountBalance DECIMAL(18,2) NULL,

			-- Transaction attributes referenced in FactCredit 
			CreditScore INT NULL,
			NumBankAccounts INT,
			NumCreditCards INT,
			NumLoans INT,
			NumCreditInquiries INT,
			CreditUtilizationRatio DECIMAL(18,2),
			CreditHistoryLengthMonths INT,
			CreditMixRatio DECIMAL(18,2),
			
			-- Data lineage columns
			DataSourceFile NVARCHAR(255) NULL,
			LoadBatchID UNIQUEIDENTIFIER NULL,
			LoadDate DATETIME NULL DEFAULT GETDATE(),
			LastUpdated DATETIME NULL
		);

		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateStagingTables', @CurrentStep, 'Success');

		SET @TableCount += 1;


		-- 2.2 Staging Fraud table
		SET @CurrentStep = 'Fraud';

		IF OBJECT_ID('Staging.Fraud') IS NOT NULL
			DROP TABLE Staging.Fraud;
			
		CREATE TABLE Staging.Fraud (
			-- Business keys
			TransactionID INT NOT NULL,
			CustomerID INT NOT NULL,  
			
			-- Transaction attributes referenced in DimTransaction & DimTransactionType
			TransactionDate DATETIME NOT NULL,
			TransactionTypeName NVARCHAR(50) NOT NULL,  -- {Purchase, Withdrawal, Deposit, Transfer}

			-- Fraud indicators referenced in FactFraud
			DistanceFromHome FLOAT NULL,
			DistanceFromLastTransaction FLOAT NULL,
			RatioToMedianTransactionAmount FLOAT NULL,
			IsOnlineTransaction INT NOT NULL DEFAULT 0, -- 0 = No, 1 = Yes
			IsUsedChip INT DEFAULT 0, 	-- 0 = No, 1 = Yes
			IsUsedPIN INT DEFAULT 0, 	-- 0 = No, 1 = Yes
			IsFraudulent INT DEFAULT 0,	-- 0 = No, 1 = Yes
			
			-- Data lineage columns
			DataSourceFile NVARCHAR(255) NULL,
			LoadBatchID UNIQUEIDENTIFIER NULL,
			LoadDate DATETIME NULL DEFAULT GETDATE(),
			LastUpdated DATETIME NULL
		);

		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateStagingTables', @CurrentStep, 'Success');

		SET @TableCount += 1;


		-- 2.3 Staging Market table
		SET @CurrentStep = 'Market';

		IF OBJECT_ID('Staging.Market') IS NOT NULL
			DROP TABLE Staging.Market;
			
		CREATE TABLE Staging.Market (
			-- Business keys
			MarketID INT IDENTITY(1,1) PRIMARY KEY,
			MarketDate DATE NOT NULL,  

			-- Market attributes referenced in DimMarketIndex
			MarketName NVARCHAR(100) NOT NULL,  -- {S&P 500, NASDAQ, Dow Jones, etc.}

			-- Fields referenced in FactMarket
			OpenValue DECIMAL(18,2) NOT NULL,
			CloseValue DECIMAL(18,2) NOT NULL,
			HighestValue DECIMAL(18,2) NULL,
			LowestValue DECIMAL(18,2) NULL,
			Volume DECIMAL(18,2) NULL,
			InterestRate FLOAT,
			ExchangeRate FLOAT,
			GoldPrice DECIMAL(18,2),
			OilPrice DECIMAL(18,2),
			VIX FLOAT NULL,
			TEDSpread FLOAT NULL,
			EFFR FLOAT NULL,
			
			-- Data lineage columns
			DataSourceFile NVARCHAR(255) NULL,
			LoadBatchID UNIQUEIDENTIFIER NULL,
			LoadDate DATETIME NULL DEFAULT GETDATE(),
			LastUpdated DATETIME NULL
		);

		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateStagingTables', @CurrentStep, 'Success');

		SET @TableCount += 1;


		-- 2.4 Staging Macro table
		SET @CurrentStep = 'Macro';

		IF OBJECT_ID('Staging.Macro') IS NOT NULL
			DROP TABLE Staging.Macro;
			
		CREATE TABLE Staging.Macro (
			-- Business keys
			MacroID INT IDENTITY(1,1) PRIMARY KEY,
			ReportDate DATE NOT NULL,
			
			-- Country information referenced in DimCountry
			CountryName NVARCHAR(100) NOT NULL, 
			
			-- Economic indicators referenced directly in production load
			UnemploymentRate DECIMAL(18,2) NOT NULL, 
			GDP INT NOT NULL,
			DebtRatio DECIMAL(18,2) NOT NULL,
			DeficitRatio DECIMAL(18,2) NOT NULL,
			InflationRate DECIMAL(18,2) NOT NULL,
			ConsumerPriceIndex DECIMAL(18,2) NULL, 
			HousePriceIndex DECIMAL(18,2) NULL,
			
			-- Data lineage columns
			DataSourceFile NVARCHAR(255) NULL,
			LoadBatchID UNIQUEIDENTIFIER NULL,
			LoadDate DATETIME NULL DEFAULT GETDATE(),
			LastUpdated DATETIME NULL
		);

		INSERT INTO Staging.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateStagingTables', @CurrentStep, 'Success');

		SET @TableCount += 1;


		-- =================================
		-- 3. Create indexes for performance
		-- =================================
		
		-- Loan table indexes
		IF OBJECT_ID('Staging.IX_Staging_Loan_CustomerID') IS NOT NULL
			DROP INDEX IX_Staging_Loan_CustomerID ON Staging.Loan;
		CREATE NONCLUSTERED INDEX IX_Staging_Loan_CustomerID 
			ON Staging.Loan(CustomerID);

		IF OBJECT_ID('Staging.IX_Staging_Loan_LoanID') IS NOT NULL
			DROP INDEX IX_Staging_Loan_LoanID ON Staging.Loan;
		CREATE NONCLUSTERED INDEX IX_Staging_Loan_LoanID 
			ON Staging.Loan(LoanID);

		IF OBJECT_ID('Staging.IX_Staging_Loan_LoanDate') IS NOT NULL
			DROP INDEX IX_Staging_Loan_LoanDate ON Staging.Loan;
		CREATE NONCLUSTERED INDEX IX_Staging_Loan_LoanDate
			ON Staging.Loan(LoanDate);
		
		SET @IndexCount += 3;

		-- Fraud table indexes
		IF OBJECT_ID('Staging.IX_Staging_Fraud_CustomerID') IS NOT NULL
			DROP INDEX IX_Staging_Fraud_CustomerID ON Staging.Fraud;
		CREATE NONCLUSTERED INDEX IX_Staging_Fraud_CustomerID 
			ON Staging.Fraud(CustomerID);
		
		IF OBJECT_ID('Staging.IX_Staging_Fraud_TransactionDate') IS NOT NULL
			DROP INDEX IX_Staging_Fraud_TransactionDate ON Staging.Fraud;
		CREATE NONCLUSTERED INDEX IX_Staging_Fraud_TransactionDate 
			ON Staging.Fraud(TransactionDate);

		SET @IndexCount += 2;

		-- Market table indexes
		IF OBJECT_ID('Staging.IX_Staging_Market_MarketID') IS NOT NULL
			DROP INDEX IX_Staging_Market_MarketID ON Staging.Market;
		CREATE NONCLUSTERED INDEX IX_Staging_Market_MarketID 
			ON Staging.Market(MarketID);

		IF OBJECT_ID('Staging.IX_Staging_Market_MarketDate') IS NOT NULL
			DROP INDEX IX_Staging_Market_MarketDate ON Staging.Market;
		CREATE NONCLUSTERED INDEX IX_Staging_Market_MarketDate 
			ON Staging.Market(MarketDate);

		SET @IndexCount += 2;

		-- Macro table indexes
		IF OBJECT_ID('Staging.IX_Staging_Macro_ReportDate') IS NOT NULL
			DROP INDEX IX_Staging_Macro_ReportDate ON Staging.Macro;
		CREATE NONCLUSTERED INDEX IX_Staging_Macro_ReportDate 
			ON Staging.Macro(ReportDate);

		IF OBJECT_ID('Staging.IX_Staging_Macro_Country') IS NOT NULL
			DROP INDEX IX_Staging_Macro_Country ON Staging.Macro;
		CREATE NONCLUSTERED INDEX IX_Staging_Macro_Country
			ON Staging.Macro(CountryName);

		SET @IndexCount += 2;

		COMMIT TRANSACTION;

		-- =====================================
		-- 4. Create initial job control records
		-- =====================================
		
		INSERT INTO Staging.JobControl (JobName, BatchID, StepName, StepOrder)
		VALUES 
			('ETLStagingLoad', @BatchID, 'Initialize', 1),
			('ETLStagingLoad', @BatchID, 'LoadStagingLoan', 2),
			('ETLStagingLoad', @BatchID, 'LoadStagingFraud', 3),
			('ETLStagingLoad', @BatchID, 'LoadStagingMarket', 4),
			('ETLStagingLoad', @BatchID, 'LoadStagingMacro', 5),
			('ETLStagingLoad', @BatchID, 'ValidateData', 6),
			('ETLStagingLoad', @BatchID, 'Complete', 7);
		

		-- ===================================
		-- 22. Log the completion of the setup
		-- ===================================
		-- Record successful completion
		INSERT INTO Staging.ETLLog 
			(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
		VALUES 
			('usp_CreateStagingTables', 'Complete', 'Success', 
			@@ROWCOUNT, DATEDIFF(SECOND, @StartTime, GETDATE()));
		
		-- Return success message
		SELECT 'Staging environment created successfully' AS Status, 
			@TableCount AS TablesCreated,
			@IndexCount AS IndexesCreated,
			DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTimeSeconds;
	
	END TRY
	BEGIN CATCH
		-- Roll back transaction in case of error
		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION;
			
		-- Get error details
		SELECT @ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
		
		-- Log the error
		INSERT INTO Staging.ETLLog 
			(ProcedureName, StepName, Status, ErrorMessage)
		VALUES 
			('usp_CreateStagingTables', @CurrentStep, 'Failed', @ErrorMessage);
			
		-- Re-throw the error
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH;
END;
GO


-- ===================================================
-- Only execute if environment hasn't been created yet
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Staging'))
BEGIN
    EXEC Staging.usp_CreateStagingTables
		@LogPath = N'$(LogPath)';
END
GO

