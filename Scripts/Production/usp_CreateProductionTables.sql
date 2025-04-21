/* 
This script creates a procedure for creating the Production schema tables.
Last updated: 2025-01-01
*/


-- Create helper procedures for job control
CREATE OR ALTER PROCEDURE Production.usp_StartJobStep
	@JobName VARCHAR(100),
	@StepName VARCHAR(100),
	@BatchID UNIQUEIDENTIFIER = NULL
AS
BEGIN
	SET NOCOUNT ON;
	
	IF @BatchID IS NULL
	BEGIN
		-- Get the latest batch ID for this job
		SELECT TOP 1 @BatchID = BatchID 
		FROM Production.JobControl 
		WHERE JobName = @JobName 
		ORDER BY CreatedDate DESC;
	END
	
	-- Mark step as started
	UPDATE Production.JobControl
	SET Status = 'Running',
		StartDateTime = GETDATE()
	WHERE JobName = @JobName
	AND StepName = @StepName
	AND BatchID = @BatchID;
	
	-- Return the BatchID
	SELECT @BatchID AS BatchID;
END;
GO

CREATE OR ALTER PROCEDURE Production.usp_CompleteJobStep
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
	UPDATE Production.JobControl
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
		FROM Production.JobControl
		WHERE JobName = @JobName
		AND StepName = @StepName
		AND BatchID = @BatchID;
		
		IF @RetryCount < @MaxRetries
		BEGIN
			UPDATE Production.JobControl
			SET Status = 'Retry',
				RetryCount = RetryCount + 1
			WHERE JobName = @JobName
			AND StepName = @StepName
			AND BatchID = @BatchID;
		END
	END
END;
GO

-- Create helper procedure for populating the date dimension
CREATE OR ALTER PROCEDURE Production.usp_PopulateDateDimension
	@StartDate DATE = '1999-01-01',
	@EndDate DATE = '2030-12-31'
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @CurrentDate DATE = @StartDate;
	DECLARE @EndDateLoop DATE = @EndDate;
	
	-- Insert date records
	WHILE @CurrentDate <= @EndDateLoop
	BEGIN
		INSERT INTO Production.DimDate (
			DateID,
			FullDate,
			DayOfMonth,
			DayName,
			DayOfWeek,
			DayOfYear,
			WeekOfYear,
			MonthNumber,
			MonthName,
			Quarter,
			Year,
			IsWeekend,
			IsHoliday,
			HolidayName
		)
		VALUES (
			CONVERT(INT, CONVERT(VARCHAR, @CurrentDate, 112)),
			@CurrentDate,
			DAY(@CurrentDate),
			DATENAME(WEEKDAY, @CurrentDate),
			DATEPART(WEEKDAY, @CurrentDate),
			DATEPART(DAYOFYEAR, @CurrentDate),
			DATEPART(WEEK, @CurrentDate),
			MONTH(@CurrentDate),
			DATENAME(MONTH, @CurrentDate),
			DATEPART(QUARTER, @CurrentDate),
			YEAR(@CurrentDate),
			CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END,
			0,  -- Default IsHoliday value, update for specific holidays
			NULL  -- Default HolidayName value
		);
		
		-- Move to next day
		SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
	END;
	
	-- Update specific holidays
	-- New Year's Day
	UPDATE Production.DimDate
	SET IsHoliday = 1, HolidayName = 'New Year''s Day'
	WHERE MonthNumber = 1 AND DayOfMonth = 1;
	
	-- Independence Day
	UPDATE Production.DimDate
	SET IsHoliday = 1, HolidayName = 'Independence Day'
	WHERE MonthNumber = 7 AND DayOfMonth = 4;
	
	-- Christmas Day
	UPDATE Production.DimDate
	SET IsHoliday = 1, HolidayName = 'Christmas Day'
	WHERE MonthNumber = 12 AND DayOfMonth = 25;
	
	-- Additional holidays could be added
	
	-- Return count of records created
	SELECT COUNT(*) AS DateRecordsCreated FROM Production.DimDate;
END;
GO




-- =========================================
-- Procedure to create the Production tables
-- =========================================
CREATE OR ALTER PROCEDURE Production.usp_CreateProductionTables
	@EnableMemoryOptimized BIT = 0,     -- Enable memory-optimized tables
	@EnableColumnstoreIndexes BIT = 1,  -- Enable columnstore indexes
	@CreateExtendedProperties BIT = 1,  -- Create documentation
	@LogPath NVARCHAR(500) = NULL      
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Variables for tracking execution
	DECLARE @CurrentStep NVARCHAR(128);
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @StepStartTime DATETIME;
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @TableCount INT = 0;
	DECLARE @IndexCount INT = 0;
	DECLARE @MemoryOptimizedOption NVARCHAR(100);
	
	-- Set memory-optimized option based on parameter
	SELECT @MemoryOptimizedOption = CASE
		WHEN @EnableMemoryOptimized = 1 THEN 'MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA'
		ELSE 'MEMORY_OPTIMIZED = OFF'
	END;

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
		
		IF OBJECT_ID('Production.ETLLog') IS NOT NULL
			DROP TABLE Production.ETLLog;
			
		CREATE TABLE Production.ETLLog (
			LogID INT IDENTITY(1,1) PRIMARY KEY,
			ProcedureName NVARCHAR(255),
			StepName NVARCHAR(255),
			ExecutionDateTime DATETIME DEFAULT GETDATE(),
			Status NVARCHAR(50),
			ErrorMessage NVARCHAR(MAX),
			RowsProcessed INT NULL,
			ExecutionTimeSeconds INT NULL,
			FileName VARCHAR(255) NULL,
			Environment VARCHAR(50) NULL
		);
	
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');

		SET @TableCount += 1;

		
		-- 1.2 Job Control table for tracking pipeline execution
		SET @CurrentStep = 'JobControl';

		IF OBJECT_ID('Production.JobControl') IS NOT NULL
			DROP TABLE Production.JobControl;    

		CREATE TABLE Production.JobControl (
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
		
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');

		SET @TableCount += 1;

		-- 1.3 ETL Validation Results table
		SET @CurrentStep = 'ValidationLog';

		IF OBJECT_ID('Production.ValidationLog') IS NOT NULL
			DROP TABLE Production.ValidationLog;    
		
		CREATE TABLE Production.ValidationLog (
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

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');

		SET @TableCount += 1;


		-- Begin transaction for atomic table creation
		IF @@TRANCOUNT = 0
		BEGIN TRANSACTION;
		-- =======================
		-- 2. Create DimDate table
		-- =======================
		SET @CurrentStep = 'DimDate';
		
		IF OBJECT_ID('Production.DimDate') IS NOT NULL
			DROP TABLE Production.DimDate;
			
		CREATE TABLE Production.DimDate (
			DateID INT PRIMARY KEY,
			FullDate DATE,
			DayOfMonth INT,
			DayName NVARCHAR(10),
			DayOfWeek INT,
			DayOfYear INT,
			WeekOfYear INT,
			MonthNumber INT,
			MonthName NVARCHAR(10),
			Quarter INT,
			Year INT,
			IsWeekend BIT,
			IsHoliday BIT,
			HolidayName NVARCHAR(50) NULL,
			IsBusinessDay AS (CASE WHEN IsWeekend = 0 AND IsHoliday = 0 THEN 1 ELSE 0 END) PERSISTED
		);
		
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;
		

		-- ===================================================
		-- 3. Create DimCustomer table with Type 2 SCD support
		-- ===================================================
		SET @CurrentStep = 'DimCustomer';
		
		DECLARE @CustomerTableSQL NVARCHAR(MAX);
		
		-- Create the customer table with or without memory optimization
		SET @CustomerTableSQL = '
		IF OBJECT_ID(''Production.DimCustomer'') IS NOT NULL
			DROP TABLE Production.DimCustomer;
			
		CREATE TABLE Production.DimCustomer (
			CustomerSK INT IDENTITY(1,1) PRIMARY KEY,
			CustomerID INT NOT NULL,
			Age INT NOT NULL,
			JobTitle VARCHAR(50) NULL,
			EmploymentStatus INT NOT NULL DEFAULT 0,
			HomeOwnershipStatus VARCHAR(50) NOT NULL,
			MaritalStatus VARCHAR(50) NOT NULL,
			PreviousLoanDefault INT NOT NULL DEFAULT 0,
			IsCurrent INT NOT NULL DEFAULT 1,
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			EffectiveStartDate DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
			EffectiveEndDate DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
			PERIOD FOR SYSTEM_TIME (EffectiveStartDate, EffectiveEndDate)
		)
		WITH (
			SYSTEM_VERSIONING = ON (
				HISTORY_TABLE = Production.DimCustomerHistory, 
				DATA_CONSISTENCY_CHECK = ON),
			' + @MemoryOptimizedOption + '
		);';
		
		EXEC sp_executesql @CustomerTableSQL;
		
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;
		

		-- ============================
		-- 4. Create FactCustomer table
		-- ============================
		SET @CurrentStep = 'FactCustomer';
		
		IF OBJECT_ID('Production.FactCustomer') IS NOT NULL
			DROP TABLE Production.FactCustomer;
			
		CREATE TABLE Production.FactCustomer (
			CustomerSK INT,
			DateSK INT,  
			AnnualIncome DECIMAL(18,2) NOT NULL,
			TotalAssets DECIMAL(18,2) NOT NULL,
			TotalLiabilities DECIMAL(18,2) NOT NULL,
			AnnualExpenses DECIMAL(18,2) NULL,
			MonthlySavings DECIMAL(18,2) NULL,
			AnnualBonus DECIMAL(18,2),
			DebtToIncomeRatio FLOAT NULL,
			PaymentHistoryYears INT,
			JobTenureMonths INT,
			NumDependents INT NULL,
			NetWorth AS (TotalAssets - TotalLiabilities) PERSISTED,
			SavingsRatio AS (MonthlySavings * 12 / NULLIF(AnnualIncome, 0)) PERSISTED,
			LiquidityRatio AS (TotalAssets / NULLIF(TotalLiabilities, 0)) PERSISTED,
			FinancialStressIndex AS (
				CASE
					WHEN DebtToIncomeRatio > 0.43 THEN 100
					WHEN DebtToIncomeRatio > 0.36 THEN 80
					WHEN DebtToIncomeRatio > 0.28 THEN 60
					WHEN DebtToIncomeRatio > 0.20 THEN 40
					ELSE 20
				END) PERSISTED,
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_FactCustomer PRIMARY KEY (CustomerSK, DateSK),
			CONSTRAINT CK_IncomePositive CHECK (AnnualIncome >= 0),
			FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
			FOREIGN KEY (DateSK) REFERENCES Production.DimDate(DateID)
		);
		
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;
		

		-- ===========================
		-- 5. Create DimLoanType table
		-- ===========================
		SET @CurrentStep = 'DimLoanType';
		
		IF OBJECT_ID('Production.DimLoanType') IS NOT NULL
			DROP TABLE Production.DimLoanType;
			
		CREATE TABLE Production.DimLoanType (
			LoanTypeID INT IDENTITY(1,1) NOT NULL,
			LoanType VARCHAR(50) NOT NULL,	-- {Mortgage, Auto Loan, Personal, Student, Credit Builder, Debt Consolidation, Home Equity, Business, Payday, Other}
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_DimLoanType PRIMARY KEY (LoanTypeID)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ========================
		-- 6. Create FactLoan table
		-- ========================
		SET @CurrentStep = 'FactLoan';
		
		IF OBJECT_ID('Production.FactLoan') IS NOT NULL
			DROP TABLE Production.FactLoan;
			
		CREATE TABLE Production.FactLoan (
			LoanID INT IDENTITY(1,1) NOT NULL,
			LoanDateID INT NOT NULL,
			CustomerSK INT NOT NULL,
			LoanTypeID INT NOT NULL, 
			LoanAmount DECIMAL(18,2) NOT NULL,
			LoanDurationMonths INT NOT NULL,
			InterestRate FLOAT NOT NULL,
			DelayedPayments INT NOT NULL DEFAULT 0,
			Balance DECIMAL(18,2) NOT NULL,
			MonthlyPayment DECIMAL(18,2) NOT NULL,
			LoanToValueRatio DECIMAL(18,2) NULL,
			PaymentStatus NVARCHAR(20) NOT NULL DEFAULT 'On Time', 	-- {On Time, Late, Defaulted}
			TotalInterestAmount AS (LoanAmount * (POWER(1 + (InterestRate/100/12), LoanDurationMonths) - 1)) PERSISTED,
			PaymentProgress AS (1 - (Balance / LoanAmount)) PERSISTED,
			RiskWeightedBalance AS (
				CASE 
					WHEN PaymentStatus = 'On Time' THEN Balance * 1.0
					WHEN PaymentStatus = 'Late' THEN Balance * 1.2
					WHEN PaymentStatus = 'Defaulted' THEN Balance * 1.5
					ELSE Balance
				END) PERSISTED,
			MonthsRemaining AS (CEILING(Balance / MonthlyPayment)) PERSISTED,
			CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
			ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_FactLoan PRIMARY KEY NONCLUSTERED (LoanID),
			CONSTRAINT CK_InterestRate CHECK (InterestRate >= 0 AND InterestRate <= 30),
			CONSTRAINT CK_LoanAmount CHECK (LoanAmount > 0),
			CONSTRAINT CK_LoanDuration CHECK (LoanDurationMonths > 0),
			FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
			FOREIGN KEY (LoanTypeID) REFERENCES Production.DimLoanType(LoanTypeID),
			FOREIGN KEY (LoanDateID) REFERENCES Production.DimDate(DateID)
		);
		
		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;
		

		-- ==========================
		-- 7. Create DimAccount table
		-- ==========================
		SET @CurrentStep = 'DimAccount';
		
		IF OBJECT_ID('Production.DimAccount') IS NOT NULL
			DROP TABLE Production.DimAccount;
			
		CREATE TABLE Production.DimAccount (
			AccountSK INT IDENTITY(1,1),
			CustomerSK INT,
			AccountID NVARCHAR(50) NOT NULL,
			AccountType NVARCHAR(50),		-- {Checking, Savings, Investment, Retirement, Emergency Fund, Credit Card}
			PaymentBehavior NVARCHAR(50), 	-- {High spent Medium value, Low spent High value, etc.}
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_DimAccount PRIMARY KEY (AccountSK),
			FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ===========================
		-- 8. Create FactAccount table
		-- ===========================
		SET @CurrentStep = 'FactAccount';
		
		IF OBJECT_ID('Production.FactAccount') IS NOT NULL
			DROP TABLE Production.FactAccount;
			
		CREATE TABLE Production.FactAccount (
			AccountSK INT,
			AccountID NVARCHAR(50),
			AccountBalance DECIMAL(18,2),
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ==========================
		-- 9. Create FactCredit table
		-- ==========================
		SET @CurrentStep = 'FactCredit';
		
		IF OBJECT_ID('Production.FactCredit') IS NOT NULL
			DROP TABLE Production.FactCredit;
			
		CREATE TABLE Production.FactCredit (
			CreditID INT,
			CustomerSK INT,
			AccountSK INT,
			CreditScore INT,
			NumBankAccounts INT,
			NumCreditCards INT,
			NumLoans INT,
			NumCreditInquiries INT,
			CreditUtilizationRatio FLOAT,
			CreditHistoryLengthMonths INT,
			CreditMixRatio FLOAT,
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_FactCredit PRIMARY KEY (CustomerSK, CreditID),
			FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
			FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ================================
		-- 10. Create FactRiskProfile table
		-- ================================
		SET @CurrentStep = 'FactRiskProfile';
		
		IF OBJECT_ID('Production.FactRiskProfile') IS NOT NULL
			DROP TABLE Production.FactRiskProfile;
			
		CREATE TABLE Production.FactRiskProfile (
			CustomerSK INT,
			DateID INT,
			CreditRiskScore DECIMAL(18,2),
			FraudRiskScore DECIMAL(18,2),
			MarketRiskScore DECIMAL(18,2),
			AggregateRiskScore DECIMAL(18,2),
			RiskTrend NVARCHAR(10) DEFAULT 'Stable',	-- Increasing, Stable, Decreasing
			RiskCategory NVARCHAR(50),					-- Low, Medium, High, Very High
			PreviousAggregateRiskScore DECIMAL(18,2),
			RiskChangeAmount AS (AggregateRiskScore - PreviousAggregateRiskScore) PERSISTED,
			RiskChangePercent AS (CASE 
								WHEN PreviousAggregateRiskScore IS NULL OR PreviousAggregateRiskScore = 0 THEN 0
								ELSE ((AggregateRiskScore - PreviousAggregateRiskScore) / PreviousAggregateRiskScore) * 100
								END) PERSISTED,
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_FactRiskProfile PRIMARY KEY (CustomerSK, DateID),
			CONSTRAINT FK_FactRiskProfile_Customer FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
			CONSTRAINT FK_FactRiskProfile_Date FOREIGN KEY (DateID) REFERENCES Production.DimDate(DateID)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ===================================
		-- 11. Create DimTransactionType table
		-- ===================================
		SET @CurrentStep = 'DimTransactionType';
		
		IF OBJECT_ID('Production.DimTransactionType') IS NOT NULL
			DROP TABLE Production.DimTransactionType;
			
		CREATE TABLE Production.DimTransactionType (
			TransactionTypeID INT IDENTITY(1,1),
			TransactionTypeName NVARCHAR(50),	-- {Purchase, Withdrawal, Deposit, Transfer}
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_DimTransactionType PRIMARY KEY (TransactionTypeID)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ===============================
		-- 12. Create DimTransaction table
		-- ===============================
		SET @CurrentStep = 'DimTransaction';
		
		IF OBJECT_ID('Production.DimTransaction') IS NOT NULL
			DROP TABLE Production.DimTransaction;
			
		CREATE TABLE Production.DimTransaction (
			TransactionID INT NOT NULL,
			TransactionDateID INT,
			TransactionTypeID INT,
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_DimTransaction PRIMARY KEY (TransactionID),
			FOREIGN KEY (TransactionDateID) REFERENCES Production.DimDate(DateID),
			FOREIGN KEY (TransactionTypeID) REFERENCES Production.DimTransactionType(TransactionTypeID)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;

		
		-- ===========================
		-- 13. Create DimCountry table
		-- ===========================
		SET @CurrentStep = 'DimCountry';
		
		IF OBJECT_ID('Production.DimCountry') IS NOT NULL
			DROP TABLE Production.DimCountry;
			
		CREATE TABLE Production.DimCountry (
			CountryID INT IDENTITY(1,1),
			CountryName NVARCHAR(100),
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_DimCountry PRIMARY KEY (CountryID)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ==========================
		-- 14. Create FactFraud table
		-- ==========================
		SET @CurrentStep = 'FactFraud';
		
		IF OBJECT_ID('Production.FactFraud') IS NOT NULL
			DROP TABLE Production.FactFraud;
			
		CREATE TABLE Production.FactFraud (
			FraudID INT IDENTITY(1,1),
			TransactionID INT,
			TransactionDateID INT,
			CustomerSK INT,
			DistanceFromHome FLOAT NULL,
			DistanceFromLastTransaction FLOAT NULL,
			RatioToMedianTransactionAmount FLOAT NULL,
			IsOnlineTransaction INT NOT NULL DEFAULT 0, -- 0 = No, 1 = Yes
			IsUsedChip INT DEFAULT 0, 	-- 0 = No, 1 = Yes
			IsUsedPIN INT DEFAULT 0, 	-- 0 = No, 1 = Yes
			IsFraudulent INT DEFAULT 0,	-- 0 = No, 1 = Yes
			BatchID UNIQUEIDENTIFIER NULL,	-- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_FactFraud PRIMARY KEY (FraudID),
			FOREIGN KEY (TransactionID) REFERENCES Production.DimTransaction(TransactionID),
			FOREIGN KEY (TransactionDateID) REFERENCES Production.DimDate(DateID),
			FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ===============================
		-- 15. Create DimMarketIndex table
		-- ===============================
		SET @CurrentStep = 'DimMarketIndex';
		
		IF OBJECT_ID('Production.DimMarketIndex') IS NOT NULL
			DROP TABLE Production.DimMarketIndex;
			
		CREATE TABLE Production.DimMarketIndex (
			MarketIndexSK INT IDENTITY(1,1),
			MarketName NVARCHAR(100) NOT NULL, -- {S&P 500, NASDAQ, Dow Jones, etc.}
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_DimMarketIndex PRIMARY KEY (MarketIndexSK)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;


		-- ===========================
		-- 16. Create FactMarket table
		-- ===========================
		SET @CurrentStep = 'FactMarket';
		
		IF OBJECT_ID('Production.FactMarket') IS NOT NULL
			DROP TABLE Production.FactMarket;
			
		CREATE TABLE Production.FactMarket (
			MarketID INT,
			DateID INT,
			MarketIndexSK INT,
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
			DailyChangePercent AS (
				CASE 
					WHEN OpenValue > 0 THEN ((CloseValue - OpenValue) / OpenValue) * 100 
					ELSE 0 
				END) PERSISTED,
			PriceRange AS (HighestValue - LowestValue) PERSISTED,
			VolatilityRatio AS (
				CASE 
					WHEN OpenValue > 0 THEN (HighestValue - LowestValue) / OpenValue
					ELSE 0 
				END) PERSISTED,
			BatchID UNIQUEIDENTIFIER NULL,  -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(),
			CONSTRAINT PK_FactMarket PRIMARY KEY NONCLUSTERED (MarketID),
			FOREIGN KEY (DateID) REFERENCES Production.DimDate(DateID),
			FOREIGN KEY (MarketIndexSK) REFERENCES Production.DimMarketIndex(MarketIndexSK)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;



		-- =============================
		-- 17. Create FactEconomic table
		-- =============================
		SET @CurrentStep = 'FactEconomicIndicators';
		
		IF OBJECT_ID('Production.FactEconomicIndicators') IS NOT NULL
			DROP TABLE Production.FactEconomicIndicators;
			
		CREATE TABLE Production.FactEconomicIndicators (
			EconomicID INT,
			ReportDateID INT,
			CountryID INT,
			UnemploymentRate DECIMAL(18,2) NOT NULL,
			GDP INT NOT NULL,
			DebtRatio DECIMAL(18,2) NOT NULL,
			DeficitRatio DECIMAL(18,2) NOT NULL,
			InflationRate DECIMAL(18,2) NOT NULL,
			ConsumerPriceIndex DECIMAL(18,2) NULL,
			HousePriceIndex DECIMAL(18,2) NULL,
			BatchID UNIQUEIDENTIFIER NULL, -- For tracking ETL loads
			LoadDate DATETIME DEFAULT GETDATE(), 
			CONSTRAINT PK_FactEconomicIndicators PRIMARY KEY (EconomicID, ReportDateID),
			FOREIGN KEY (ReportDateID) REFERENCES Production.DimDate(DateID),
			FOREIGN KEY (CountryID) REFERENCES Production.DimCountry(CountryID)
		);

		INSERT INTO Production.ETLLog (ProcedureName, StepName, Status)
		VALUES ('usp_CreateProductionTables', @CurrentStep, 'Created');
		
		SET @TableCount += 1;
		


		COMMIT TRANSACTION;

		-- ==================================
		-- 18. Create indexes for performance
		-- ==================================

		
		-- Clustered indexes
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'CIX_FactLoan_Date')
			DROP INDEX CIX_FactLoan_Date ON Production.FactLoan;
		CREATE CLUSTERED INDEX CIX_FactLoan_Date 
			ON Production.FactLoan(LoanDateID);
		
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'CIX_FactMarket_Date')
			DROP INDEX CIX_FactMarket_Date ON Production.FactMarket;
		CREATE CLUSTERED INDEX CIX_FactMarket_Date
			ON Production.FactMarket(DateID);

		SET @IndexCount += 2;

		-- Composites and non-clustered indexes
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimDate_YearMonthDay')
			DROP INDEX IX_DimDate_YearMonthDay ON Production.DimDate;
		CREATE INDEX IX_DimDate_YearMonthDay 
			ON Production.DimDate(Year, MonthNumber, DayOfMonth);
		
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimDate_WeekYear')
			DROP INDEX IX_DimDate_WeekYear ON Production.DimDate;
		CREATE INDEX IX_DimDate_WeekYear
			ON Production.DimDate(Year, WeekOfYear);
		
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimCustomer_BusinessKey')
			DROP INDEX IX_DimCustomer_BusinessKey ON Production.DimCustomer;
		CREATE INDEX IX_DimCustomer_BusinessKey 
			ON Production.DimCustomer(CustomerID, IsCurrent);
		
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimCustomer_EffectiveDates')
			DROP INDEX IX_DimCustomer_EffectiveDates ON Production.DimCustomer;
		CREATE INDEX IX_DimCustomer_EffectiveDates
			ON Production.DimCustomer(EffectiveStartDate, EffectiveEndDate);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimCustomer_Demographics')
			DROP INDEX IX_DimCustomer_Demographics ON Production.DimCustomer;
		CREATE INDEX IX_DimCustomer_Demographics 
			ON Production.DimCustomer(Age, EmploymentStatus, HomeOwnershipStatus, MaritalStatus);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimAccount_AccountType')
			DROP INDEX IX_DimAccount_AccountType ON Production.DimAccount;
		CREATE INDEX IX_DimAccount_AccountType
			ON Production.DimAccount(AccountType);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimLoanType_TypeName')
			DROP INDEX IX_DimLoanType_TypeName ON Production.DimLoanType;
		CREATE INDEX IX_DimLoanType_TypeName
			ON Production.DimLoanType(LoanType);
		
		IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UQ_FactLoan_Natural_Key' AND object_id = OBJECT_ID('Production.FactLoan'))
		CREATE UNIQUE NONCLUSTERED INDEX UQ_FactLoan_Natural_Key
			ON Production.FactLoan(CustomerSK, LoanDateID, LoanTypeID, LoanAmount);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactMarket_Date')
			DROP INDEX IX_FactMarket_Date ON Production.FactMarket;		
		CREATE NONCLUSTERED INDEX IX_FactMarket_Date
			ON Production.FactMarket(DateID)
			INCLUDE (OpenValue, CloseValue, InterestRate, VIX);

		SET @IndexCount += 8;

		-- Covering and filtering indexes
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_DimCustomer_PreviousDefault')
			DROP INDEX IX_DimCustomer_PreviousDefault ON Production.DimCustomer;
		CREATE INDEX IX_DimCustomer_PreviousDefault
			ON Production.DimCustomer(PreviousLoanDefault, CustomerSK)
			INCLUDE (Age, EmploymentStatus);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactCustomer_Financial')
			DROP INDEX IX_FactCustomer_Financial ON Production.FactCustomer;
		CREATE INDEX IX_FactCustomer_Financial
			ON Production.FactCustomer(CustomerSK) 
			INCLUDE (AnnualIncome, TotalAssets, TotalLiabilities, DebtToIncomeRatio);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactCustomer_Stress')
			DROP INDEX IX_FactCustomer_Stress ON Production.FactCustomer;		
		CREATE INDEX IX_FactCustomer_Stress
			ON Production.FactCustomer(CustomerSK)
			INCLUDE (DebtToIncomeRatio, AnnualIncome, TotalLiabilities)
			WHERE DebtToIncomeRatio > 0.36;

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactLoan_CustomerLoan')
			DROP INDEX IX_FactLoan_CustomerLoan ON Production.FactLoan;
		CREATE INDEX IX_FactLoan_CustomerLoan 
			ON Production.FactLoan(CustomerSK, LoanTypeID, LoanDateID)
			INCLUDE (LoanAmount, InterestRate, MonthlyPayment);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactCredit_Customer_CreditScore')
			DROP INDEX IX_FactCredit_Customer_CreditScore ON Production.FactCredit;		
		CREATE INDEX IX_FactCredit_Customer_CreditScore 
			ON Production.FactCredit(CustomerSK)
			INCLUDE (CreditScore, CreditUtilizationRatio, NumCreditCards);
		
		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactRiskProfile_RiskTrend')
			DROP INDEX IX_FactRiskProfile_RiskTrend ON Production.FactRiskProfile;
		CREATE INDEX IX_FactRiskProfile_RiskTrend
			ON Production.FactRiskProfile(CustomerSK, DateID, AggregateRiskScore)
			INCLUDE (RiskTrend, RiskCategory);

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactCredit_Utilization')
			DROP INDEX IX_FactCredit_Utilization ON Production.FactCredit;
		CREATE INDEX IX_FactCredit_Utilization
			ON Production.FactCredit(CreditUtilizationRatio)
			INCLUDE (CustomerSK, CreditScore, CreditHistoryLengthMonths)
			WHERE CreditUtilizationRatio > 0.8;

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactFraud_FraudulentTransactions')
			DROP INDEX IX_FactFraud_FraudulentTransactions ON Production.FactFraud;
		CREATE INDEX IX_FactFraud_FraudulentTransactions
			ON Production.FactFraud(CustomerSK, TransactionID)
			WHERE IsFraudulent = 1;

		IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FactEconomicIndicators_TimeSeries')
			DROP INDEX IX_FactEconomicIndicators_TimeSeries ON Production.FactEconomicIndicators;
		CREATE INDEX IX_FactEconomicIndicators_TimeSeries
			ON Production.FactEconomicIndicators(ReportDateID, CountryID)
			INCLUDE (UnemploymentRate, GDP, InflationRate);
		
		SET @IndexCount += 10;


		-- =========================================
		-- 19. Extended properties for documentation
		-- =========================================

		IF @CreateExtendedProperties = 1
		BEGIN
			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Table to track import logs',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'ETLLog';
				
			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Date dimension table for time-based analysis',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimDate';
				
			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Type 2 SCD for customer dimension tracking changes over time',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimCustomer';
				
			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for customer financial metrics',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactCustomer';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Account dimension table for customer accounts',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimAccount';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for account balances',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactAccount';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for credit metrics',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactCredit';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Loan type dimension table for loan types',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimLoanType';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for loan metrics',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactLoan';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Transaction type dimension table for transaction types',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimTransactionType';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Transaction dimension table for transaction types',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimTransaction';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for fraud detection metrics',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactFraud';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for market risk metrics',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactMarket';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Market index dimension table for market indices',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimMarketIndex';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Country dimension table for country information',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'DimCountry';

			EXEC sp_addextendedproperty 
				@name = N'MS_Description', 
				@value = N'Fact table for economic metrics',
				@level0type = N'SCHEMA', @level0name = N'Production',
				@level1type = N'TABLE',  @level1name = N'FactEconomicIndicators';
		END
		

		-- ======================================
		-- 20. Create initial job control records
		-- ======================================
		DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
		
		INSERT INTO Production.JobControl (JobName, BatchID, StepName, StepOrder)
		VALUES 
			('ETLProductionLoad', @BatchID, 'Initialize', 1),
			('ETLProductionLoad', @BatchID, 'LoadProductionLoan', 2),
			('ETLProductionLoad', @BatchID, 'LoadProductionFraud', 3),
			('ETLProductionLoad', @BatchID, 'LoadProductionMarket', 4),
			('ETLProductionLoad', @BatchID, 'LoadProductionMacro', 5),
			('ETLProductionLoad', @BatchID, 'ValidateData', 6),
			('ETLProductionLoad', @BatchID, 'Complete', 7);

		-- ===================================
		-- 21. Log the completion of the setup
		-- ===================================
		-- Record successful completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
		VALUES 
			('usp_CreateProductionTables', 'Complete', 'Success', 
			@TableCount, DATEDIFF(SECOND, @StartTime, GETDATE()));
				
		-- Return success message
		SELECT 'Production environment created successfully' AS Status, 
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
		INSERT INTO Production.ETLLog 
			(ProcedureName, Status, ErrorMessage)
		VALUES 
			(@CurrentStep, 'Failed', @ErrorMessage);
			
		-- Re-throw the error
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH;
END;
GO


-- ===================================================
-- Only execute if environment hasn't been created yet
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Production'))
BEGIN	
	EXEC Production.usp_CreateProductionTables
		@LogPath = N'$(LogPath)';
	EXEC Production.usp_PopulateDateDimension;

END
GO


