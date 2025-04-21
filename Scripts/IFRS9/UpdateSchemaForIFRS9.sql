/*
IFRS 9 Schema Creation Script
Creates the IFRS9 schema and all necessary tables for IFRS 9 compliance
Last updated: April 20, 2025
*/

USE [$(Database)];
GO

PRINT 'Starting creation of IFRS9 schema and tables';

-- Force metadata refresh and clear procedure cache
DBCC FREEPROCCACHE; -- Clear procedure cache
DBCC FREESYSTEMCACHE('ALL'); -- Clear system cache
GO

-- Create all required schemas in isolated batches with NOWAIT option
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Staging')
BEGIN
	EXEC sp_executesql N'CREATE SCHEMA Staging';
	PRINT 'Created Staging schema';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Production')
BEGIN
	EXEC sp_executesql N'CREATE SCHEMA Production';
	PRINT 'Created Production schema';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'IFRS9')
BEGIN
	EXEC sp_executesql N'CREATE SCHEMA IFRS9';
	PRINT 'Created IFRS9 schema';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Config')
BEGIN
	EXEC sp_executesql N'CREATE SCHEMA Config';
	PRINT 'Created Config schema';
END
GO

-- Update ETLLog table
BEGIN
	-- Table exists, but check if it has the required columns
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'BatchID')
	BEGIN
		ALTER TABLE Production.ETLLog ADD BatchID UNIQUEIDENTIFIER NULL;
		PRINT 'Added BatchID column to existing Production.ETLLog table';
	END

	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'ProcedureName')
	BEGIN
		ALTER TABLE Production.ETLLog ADD ProcedureName NVARCHAR(128) NULL;
		PRINT 'Added ProcedureName column to existing Production.ETLLog table';
	END

	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'StepName')
	BEGIN
		ALTER TABLE Production.ETLLog ADD StepName NVARCHAR(100) NULL;
		PRINT 'Added StepName column to existing Production.ETLLog table';
	END

	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'Message')
	BEGIN
		ALTER TABLE Production.ETLLog ADD Message NVARCHAR(MAX) NULL;
		PRINT 'Added Message column to existing Production.ETLLog table';
	END

	PRINT 'Updated existing Production.ETLLog table with required columns';
END
GO

-- Create DimDate table if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimDate' AND schema_id = SCHEMA_ID('Production'))
BEGIN
	CREATE TABLE Production.DimDate (
		DateID INT NOT NULL PRIMARY KEY,
		FullDate DATE NOT NULL,
		DayOfMonth INT NOT NULL,
		DayName NVARCHAR(10) NOT NULL,
		DayOfWeek TINYINT NOT NULL,
		DayOfYear SMALLINT NOT NULL,
		WeekOfYear TINYINT NOT NULL,
		MonthNumber TINYINT NOT NULL,
		MonthName NVARCHAR(10) NOT NULL,
		Quarter TINYINT NOT NULL,
		Year SMALLINT NOT NULL,
		IsWeekend BIT NOT NULL,
		IsHoliday BIT NOT NULL
	);
	
	-- Add current date and surrounding dates
	DECLARE @CurrentDate DATE = GETDATE();
	DECLARE @StartDate DATE = DATEADD(YEAR, -3, @CurrentDate);
	DECLARE @EndDate DATE = DATEADD(YEAR, 3, @CurrentDate);
	DECLARE @DateInProcess DATE = @StartDate;
	
	WHILE @DateInProcess <= @EndDate
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
			IsHoliday
		)
		SELECT
			CONVERT(INT, CONVERT(VARCHAR, @DateInProcess, 112)) AS DateID,
			@DateInProcess AS FullDate,
			DAY(@DateInProcess) AS DayOfMonth,
			DATENAME(WEEKDAY, @DateInProcess) AS DayName,
			DATEPART(WEEKDAY, @DateInProcess) AS DayOfWeek,
			DATEPART(DAYOFYEAR, @DateInProcess) AS DayOfYear,
			DATEPART(WEEK, @DateInProcess) AS WeekOfYear,
			MONTH(@DateInProcess) AS MonthNumber,
			DATENAME(MONTH, @DateInProcess) AS MonthName,
			DATEPART(QUARTER, @DateInProcess) AS Quarter,
			YEAR(@DateInProcess) AS Year,
			CASE WHEN DATEPART(WEEKDAY, @DateInProcess) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend,
			0 AS IsHoliday;
			
		SET @DateInProcess = DATEADD(DAY, 1, @DateInProcess);
	END
	
	PRINT 'Created Production.DimDate table with ' + CAST(DATEDIFF(DAY, @StartDate, @EndDate) + 1 AS VARCHAR) + ' records';
END
ELSE 
BEGIN
	PRINT 'Production.DimDate table already exists';
END
GO

-- Create DimCustomer table if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCustomer' AND schema_id = SCHEMA_ID('Production'))
BEGIN
	CREATE TABLE Production.DimCustomer (
		CustomerSK INT IDENTITY(1,1) PRIMARY KEY,
		CustomerID NVARCHAR(20) NOT NULL,
		CustomerName NVARCHAR(100) NULL,
		CustomerType NVARCHAR(20) NULL,
		CreatedDate DATE NOT NULL DEFAULT GETDATE(),
		IsActive BIT NOT NULL DEFAULT 1,
		CreditScore INT NULL,
		CreditScoreDecreasePercent DECIMAL(5,2) NULL DEFAULT 0,
		BankruptcyFlag BIT NOT NULL DEFAULT 0,
		WatchListFlag BIT NOT NULL DEFAULT 0
	);
	PRINT 'Created Production.DimCustomer table';
END
ELSE 
BEGIN
	PRINT 'Production.DimCustomer table already exists';
END
GO

-- Create DimAccount table if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimAccount' AND schema_id = SCHEMA_ID('Production'))
BEGIN
	CREATE TABLE Production.DimAccount (
		AccountSK INT IDENTITY(1,1) PRIMARY KEY,
		AccountID NVARCHAR(20) NOT NULL,
		CustomerSK INT NOT NULL,
		AccountOpenDate DATE NOT NULL,
		AccountType VARCHAR(50) NULL,
		IsActive BIT NOT NULL DEFAULT 1,
		AccountClosedDate DATE NULL,
		HoldToMaturityFlag BIT NOT NULL DEFAULT 0,
		HoldToCollectAndSellFlag BIT NOT NULL DEFAULT 0,
		TradingFlag BIT NOT NULL DEFAULT 0,
		HasNonSPPIFeatures BIT NOT NULL DEFAULT 0,
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK)
	);
	PRINT 'Created Production.DimAccount table';
END
ELSE 
BEGIN
	PRINT 'Production.DimAccount table already exists';
END
GO

-- Create FactLoan table if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactLoan' AND schema_id = SCHEMA_ID('Production'))
BEGIN
	CREATE TABLE Production.FactLoan (
		LoanSK INT IDENTITY(1,1) PRIMARY KEY,
		AccountSK INT NOT NULL,
		CustomerSK INT NOT NULL,
		DateID INT NOT NULL,
		IsActive BIT NOT NULL DEFAULT 1,
		DaysPastDue INT NULL DEFAULT 0,
		IsDefaulted BIT NOT NULL DEFAULT 0,
		ForbearanceFlag BIT NOT NULL DEFAULT 0,
		ChargeOffFlag BIT NOT NULL DEFAULT 0,
		ModifiedLoanFlag BIT NOT NULL DEFAULT 0,
		MaturityDate DATE NULL,
		TermMonths INT NULL,
		CollateralType VARCHAR(50) NULL,
		LoanType VARCHAR(50) NULL,
		CurrentBalance DECIMAL(18,2) NULL,
		CreditLimit DECIMAL(18,2) NULL,
		InterestRate DECIMAL(6,3) NULL,
		FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK),
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
		FOREIGN KEY (DateID) REFERENCES Production.DimDate(DateID)
	);
	PRINT 'Created Production.FactLoan table';
END
ELSE 
BEGIN
	PRINT 'Production.FactLoan table already exists';
END
GO

-- Financial Instrument Classification
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FinancialInstrumentClassification' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	CREATE TABLE IFRS9.FinancialInstrumentClassification (
		ClassificationID INT IDENTITY(1,1) PRIMARY KEY,
		CustomerSK INT NOT NULL,
		AccountSK INT NOT NULL,
		InstrumentTypeCode VARCHAR(50) NULL,
		ClassificationCategory NVARCHAR(50) NULL,
		BusinessModelAssessment NVARCHAR(50) NULL,
		CashflowCharacteristics NVARCHAR(50) NULL,
		BusinessModel NVARCHAR(50) NULL,
		SPPICompliant BIT NOT NULL DEFAULT 1,
		AmortizedCost BIT NOT NULL DEFAULT 0,
		FairValueOCI BIT NOT NULL DEFAULT 0,
		FairValuePL BIT NOT NULL DEFAULT 0,
		EffectiveDate DATE NULL,
		ClassificationDate DATE NULL,
		LastReassessmentDate DATE NULL,
		JustificationText NVARCHAR(500) NULL,
		ClassificationReason NVARCHAR(500) NULL,
		RecordedDate DATETIME2(0) NULL DEFAULT GETDATE(),
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
		FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK)
	);
	PRINT 'Created IFRS9.FinancialInstrumentClassification table';
END
GO

-- Handle existing FinancialInstrumentClassification table with missing columns
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FinancialInstrumentClassification' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	-- Check columns used by usp_ClassifyFinancialInstruments
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'InstrumentTypeCode')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD InstrumentTypeCode VARCHAR(50) NULL;
		PRINT 'Added InstrumentTypeCode column to IFRS9.FinancialInstrumentClassification';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'ClassificationCategory')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD ClassificationCategory NVARCHAR(50) NULL;
		PRINT 'Added ClassificationCategory column to IFRS9.FinancialInstrumentClassification';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'BusinessModelAssessment')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD BusinessModelAssessment NVARCHAR(50) NULL;
		PRINT 'Added BusinessModelAssessment column to IFRS9.FinancialInstrumentClassification';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'CashflowCharacteristics')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD CashflowCharacteristics NVARCHAR(50) NULL;
		PRINT 'Added CashflowCharacteristics column to IFRS9.FinancialInstrumentClassification';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'ClassificationDate')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD ClassificationDate DATE NULL;
		PRINT 'Added ClassificationDate column to IFRS9.FinancialInstrumentClassification';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'JustificationText')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD JustificationText NVARCHAR(500) NULL;
		PRINT 'Added JustificationText column to IFRS9.FinancialInstrumentClassification';
	END
	
	-- Check other commonly needed columns
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'LastReassessmentDate')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD LastReassessmentDate DATE NULL;
		PRINT 'Added LastReassessmentDate column to IFRS9.FinancialInstrumentClassification';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.FinancialInstrumentClassification') AND name = 'RecordedDate')
	BEGIN
		ALTER TABLE IFRS9.FinancialInstrumentClassification ADD RecordedDate DATETIME2(0) NULL DEFAULT GETDATE();
		PRINT 'Added RecordedDate column to IFRS9.FinancialInstrumentClassification';
	END
END
GO

-- ECL Staging Classification
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLStaging' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	CREATE TABLE IFRS9.ECLStaging (
		StagingID INT IDENTITY(1,1) PRIMARY KEY,
		CustomerSK INT NOT NULL,
		AccountSK INT NOT NULL,
		AsOfDate DATE NOT NULL,
		ECLStage TINYINT NOT NULL DEFAULT 1,
		SignificantIncreaseInCreditRisk BIT NOT NULL DEFAULT 0,
		CreditImpaired BIT NOT NULL DEFAULT 0,
		StageChangeReason NVARCHAR(500) NULL,
		PreviousStage TINYINT NULL,
		DaysPastDue INT NULL,
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
		FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK)
	);
	
	CREATE NONCLUSTERED INDEX IX_ECLStaging_Customer_Account_Date
	ON IFRS9.ECLStaging(CustomerSK, AccountSK, AsOfDate);
	
	PRINT 'Created IFRS9.ECLStaging table';
END
GO

-- Macroeconomic Scenarios
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'MacroeconomicScenarios' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	CREATE TABLE IFRS9.MacroeconomicScenarios (
		ScenarioID INT IDENTITY(1,1) PRIMARY KEY,
		ScenarioName NVARCHAR(50) NOT NULL,
		ScenarioWeight DECIMAL(5,4) NOT NULL,
		GDPGrowth DECIMAL(5,2) NULL,
		UnemploymentRate DECIMAL(5,2) NULL,
		InterestRate DECIMAL(5,2) NULL,
		InflationRate DECIMAL(5,2) NULL,
		HousePriceIndex DECIMAL(6,2) NULL DEFAULT 100,
		EffectiveDate DATE NOT NULL,
		ExpiryDate DATE NULL,
		IsActive BIT NOT NULL DEFAULT 1
	);
	
	-- Insert initial macroeconomic scenarios
	INSERT INTO IFRS9.MacroeconomicScenarios (
		ScenarioName, ScenarioWeight, GDPGrowth, UnemploymentRate, 
		InterestRate, InflationRate, HousePriceIndex, EffectiveDate, IsActive
	)
	VALUES 
		('Base', 0.60, 2.0, 5.0, 2.5, 2.0, 100.0, CAST(GETDATE() AS DATE), 1),
		('Upside', 0.15, 3.5, 4.0, 3.0, 2.5, 105.0, CAST(GETDATE() AS DATE), 1),
		('Downside', 0.15, 0.5, 7.0, 2.0, 4.0, 95.0, CAST(GETDATE() AS DATE), 1),
		('Severe Downside', 0.10, -2.0, 9.0, 1.5, 5.5, 85.0, CAST(GETDATE() AS DATE), 1);
	
	PRINT 'Created IFRS9.MacroeconomicScenarios table with initial scenarios';
END
GO

-- ECL Parameters
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLParameters' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	CREATE TABLE IFRS9.ECLParameters (
		ParameterID INT IDENTITY(1,1) PRIMARY KEY,
		CustomerSK INT NOT NULL,
		AccountSK INT NOT NULL,
		AsOfDate DATE NOT NULL,
		PD_12Month DECIMAL(10,8) NOT NULL,
		PD_Lifetime DECIMAL(10,8) NOT NULL,
		LGD DECIMAL(10,8) NOT NULL,
		EAD DECIMAL(18,2) NOT NULL,
		EIR DECIMAL(10,8) NOT NULL,
		Maturity INT NOT NULL,
		MacroeconomicScenarioID INT NOT NULL,
		ModelVersion VARCHAR(20) NOT NULL DEFAULT '1.0',
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
		FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK),
		FOREIGN KEY (MacroeconomicScenarioID) REFERENCES IFRS9.MacroeconomicScenarios(ScenarioID)
	);
	
	CREATE NONCLUSTERED INDEX IX_ECLParameters_Customer_Account_Date
	ON IFRS9.ECLParameters(CustomerSK, AccountSK, AsOfDate);
	
	PRINT 'Created IFRS9.ECLParameters table';
END
GO

-- ECL Calculation
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLCalculation' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	CREATE TABLE IFRS9.ECLCalculation (
		CalculationID INT IDENTITY(1,1) PRIMARY KEY,
		CustomerSK INT NOT NULL,
		AccountSK INT NOT NULL,
		AsOfDate DATE NOT NULL,
		DateID INT NOT NULL, -- Using DateID (not DateSK) for consistency with DimDate
		ECLStage TINYINT NOT NULL DEFAULT 1,
		ECL_12Month DECIMAL(18,2) NOT NULL,
		ECL_Lifetime DECIMAL(18,2) NOT NULL,
		AppliedECL DECIMAL(18,2) NOT NULL,
		GrossCarryingAmount DECIMAL(18,2) NOT NULL,
		NetCarryingAmount DECIMAL(18,2) NOT NULL,
		ScenarioID INT NOT NULL,
		PD_Applied DECIMAL(10,8) NOT NULL,
		LGD_Applied DECIMAL(10,8) NOT NULL,
		EAD_Applied DECIMAL(18,2) NOT NULL,
		DiscountFactor DECIMAL(10,8) NOT NULL DEFAULT 1.0,
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
		FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK),
		FOREIGN KEY (DateID) REFERENCES Production.DimDate(DateID),
		FOREIGN KEY (ScenarioID) REFERENCES IFRS9.MacroeconomicScenarios(ScenarioID)
	);
	
	CREATE NONCLUSTERED INDEX IX_ECLCalculation_Date_Stage
	ON IFRS9.ECLCalculation(DateID, ECLStage);
	
	PRINT 'Created IFRS9.ECLCalculation table';
END
GO

-- Handle existing ECLCalculation table with potentially missing columns
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLCalculation' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	-- Check and add required columns if missing
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'AccountSK')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD AccountSK INT NOT NULL DEFAULT 1;
		PRINT 'Added AccountSK column to existing IFRS9.ECLCalculation table';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'CustomerSK')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD CustomerSK INT NOT NULL DEFAULT 1;
		PRINT 'Added CustomerSK column to existing IFRS9.ECLCalculation table';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'DateID')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD DateID INT NOT NULL DEFAULT 20250420; -- Today's date as a default
		PRINT 'Added DateID column to existing IFRS9.ECLCalculation table';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'ECLStage')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD ECLStage TINYINT NOT NULL DEFAULT 1;
		PRINT 'Added ECLStage column to existing IFRS9.ECLCalculation table';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'ScenarioID')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD ScenarioID INT NOT NULL DEFAULT 1;
		PRINT 'Added ScenarioID column to existing IFRS9.ECLCalculation table';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'PD_Applied')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD PD_Applied DECIMAL(10,8) NOT NULL DEFAULT 0.02;
		PRINT 'Added PD_Applied column to existing IFRS9.ECLCalculation table';
	END
	
	IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'LGD_Applied')
	BEGIN
		ALTER TABLE IFRS9.ECLCalculation ADD LGD_Applied DECIMAL(10,8) NOT NULL DEFAULT 0.45;
		PRINT 'Added LGD_Applied column to existing IFRS9.ECLCalculation table';
	END
END
GO

-- Forbearance Tracking
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Forbearance' AND schema_id = SCHEMA_ID('IFRS9'))
BEGIN
	CREATE TABLE IFRS9.Forbearance (
		ForbearanceID INT IDENTITY(1,1) PRIMARY KEY,
		CustomerSK INT NOT NULL,
		AccountSK INT NOT NULL,
		StartDate DATE NOT NULL,
		EndDate DATE NULL,
		ForbearanceType NVARCHAR(50) NOT NULL,
		Amount DECIMAL(18,2) NOT NULL,
		Reason VARCHAR(500) NULL,
		FOREIGN KEY (CustomerSK) REFERENCES Production.DimCustomer(CustomerSK),
		FOREIGN KEY (AccountSK) REFERENCES Production.DimAccount(AccountSK)
	);
	
	PRINT 'Created IFRS9.Forbearance table';
END
GO


PRINT 'IFRS9 schema and tables created successfully';
GO
