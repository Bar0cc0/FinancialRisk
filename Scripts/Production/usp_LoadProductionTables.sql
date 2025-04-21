/* 
This script creates a stored procedure to load data from staging tables to production tables.
Last updated: 2025-01-01
*/

EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;
EXEC sp_configure 'Ad Hoc Distributed Queries', 1; 
RECONFIGURE;
GO

CREATE OR ALTER PROCEDURE Production.usp_LoadProductionTables
	@BatchID UNIQUEIDENTIFIER = NULL,
	@LoadDimensions BIT = 1,
	@LoadFacts BIT = 1,
	@ValidateAfterLoad BIT = 1,
	@DataPath NVARCHAR(500) = NULL,
	@LogPath NVARCHAR(500) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Variables for execution tracking
	DECLARE @CurrentStep NVARCHAR(100);
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @StepStartTime DATETIME;
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;
	DECLARE @RowsProcessed INT = 0;
	DECLARE @ConfigValue NVARCHAR(500);
	
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
		INSERT INTO Production.ETLLog (ProcedureName, Status)
		VALUES ('usp_LoadProductionTables', 'Started');
				

		-- ========================
		-- 1. LOAD DIMENSION TABLES
		-- ========================
		IF @LoadDimensions = 1
		BEGIN
			-------------------
			-- 1.0 Load DimDate
			-------------------
			SET @CurrentStep = 'LoadDimDate';
			SET @StepStartTime = GETDATE();
			
			IF (SELECT COUNT(*) FROM Production.DimDate) = 0
			BEGIN
				EXEC Production.usp_PopulateDateDimension
					@StartDate = '2015-01-01',
					@EndDate = '2030-12-31';
					
				SET @RowsProcessed = @@ROWCOUNT;
				
				INSERT INTO Production.ETLLog (ProcedureName, StepName, Status, ExecutionTimeSeconds, RowsProcessed)
				VALUES ('usp_LoadProductionTables', @CurrentStep, 'Success', 
						DATEDIFF(SECOND, @StepStartTime, GETDATE()), @RowsProcessed);
			END
			ELSE
			BEGIN
				INSERT INTO Production.ETLLog (ProcedureName, StepName, Status, ExecutionTimeSeconds)
				VALUES ('usp_LoadProductionTables', @CurrentStep, 'Skipped - Already populated', 
						DATEDIFF(SECOND, @StepStartTime, GETDATE()));
        	END

			-----------------------
			-- 1.1 Load DimLoanType
			-----------------------
			SET @CurrentStep = 'LoadDimLoanType';
			SET @StepStartTime = GETDATE();
			
			MERGE INTO Production.DimLoanType AS target
			USING (
				SELECT DISTINCT TRIM(LoanType) AS LoanType 
				FROM Staging.Loan
			) AS source
			ON (UPPER(TRIM(target.LoanType)) = UPPER(TRIM(source.LoanType)))
			WHEN NOT MATCHED THEN
				INSERT (LoanType, BatchID, LoadDate)
				VALUES (source.LoanType, @BatchID, GETDATE());

			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));


			---------------------------------------
			-- 1.2 Load DimCustomer with SCD Type 2
			---------------------------------------
			SET @CurrentStep = 'LoadDimCustomer';
			SET @StepStartTime = GETDATE();
			
			-- Merge customer data with SCD Type 2 support
			MERGE Production.DimCustomer AS target
			USING (
				SELECT DISTINCT 
					CustomerID,
					Age,
					EmploymentStatus,
					JobTitle,
					HomeOwnershipStatus,
					MaritalStatus,
					PreviousLoanDefault
				FROM Staging.Loan
			) AS source
			ON (target.CustomerID = source.CustomerID AND target.IsCurrent = 1)
			
			-- When matched and at least one tracked attribute has changed
			WHEN MATCHED AND (
				target.EmploymentStatus <> source.EmploymentStatus OR
				target.HomeOwnershipStatus <> source.HomeOwnershipStatus OR
				target.MaritalStatus <> source.MaritalStatus OR
				target.Age <> source.Age OR
				target.JobTitle <> source.JobTitle OR
				target.PreviousLoanDefault <> source.PreviousLoanDefault
			) THEN
				-- Expire the current record
				UPDATE SET 
					IsCurrent = 0
					
			-- When not matched, insert new record
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					CustomerID, Age, EmploymentStatus, JobTitle,
					HomeOwnershipStatus, MaritalStatus, PreviousLoanDefault,
					IsCurrent
				)
				VALUES (
					source.CustomerID, source.Age, source.EmploymentStatus,
					source.JobTitle, source.HomeOwnershipStatus, source.MaritalStatus,
					source.PreviousLoanDefault, 1
				);
				
			SET @RowsProcessed = @@ROWCOUNT;
			
			-- Insert new records for customers with changed attributes
			INSERT INTO Production.DimCustomer (
				CustomerID, Age, EmploymentStatus, JobTitle,
				HomeOwnershipStatus, MaritalStatus, PreviousLoanDefault,
				IsCurrent
			)
			SELECT 
				source.CustomerID, source.Age, source.EmploymentStatus,
				source.JobTitle, source.HomeOwnershipStatus, source.MaritalStatus,
				source.PreviousLoanDefault, 1
			FROM (
				SELECT DISTINCT 
					CustomerID,
					Age,
					EmploymentStatus,
					JobTitle,
					HomeOwnershipStatus,
					MaritalStatus,
					PreviousLoanDefault
				FROM Staging.Loan
			) AS source
			JOIN Production.DimCustomer AS target
				ON target.CustomerID = source.CustomerID AND target.IsCurrent = 0
			WHERE NOT EXISTS (
				SELECT 1 FROM Production.DimCustomer 
				WHERE CustomerID = source.CustomerID AND IsCurrent = 1
			)
			AND (
				target.EmploymentStatus <> source.EmploymentStatus OR
				target.HomeOwnershipStatus <> source.HomeOwnershipStatus OR
				target.MaritalStatus <> source.MaritalStatus OR
				target.Age <> source.Age OR
				target.JobTitle <> source.JobTitle OR
				target.PreviousLoanDefault <> source.PreviousLoanDefault
			);
			
			SET @RowsProcessed += @@ROWCOUNT;
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			
			----------------------
			-- 1.3 Load DimCountry
			----------------------
			SET @CurrentStep = 'LoadDimCountry';
			SET @StepStartTime = GETDATE();
			
			MERGE Production.DimCountry AS target
			USING (
				SELECT DISTINCT 
					CountryName
				FROM Staging.Macro
			) AS source
			ON (target.CountryName = source.CountryName)
						
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (CountryName)
				VALUES (source.CountryName);
				
			SET @RowsProcessed = @@ROWCOUNT;
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));

			------------------------------
			-- 1.4 Load DimTransactionType
			------------------------------
			SET @CurrentStep = 'LoadDimTransaction';
			SET @StepStartTime = GETDATE();

			-- Populate transaction types first
			MERGE Production.DimTransactionType AS target
			USING (
				SELECT DISTINCT TransactionTypeName
				FROM Staging.Fraud 
			) AS source
			ON (target.TransactionTypeName = source.TransactionTypeName)
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (TransactionTypeName)
				VALUES (source.TransactionTypeName);

			-- Then populate DimTransaction
			MERGE Production.DimTransaction AS target
			USING (
				SELECT 
					sf.TransactionID,
					d.DateID AS TransactionDateID,
					tt.TransactionTypeID
				FROM Staging.Fraud sf
				JOIN Production.DimDate d ON CONVERT(DATE, sf.TransactionDate) = d.FullDate
				JOIN Production.DimTransactionType tt ON sf.TransactionTypeName = tt.TransactionTypeName
			) AS source
			ON (target.TransactionID = source.TransactionID)
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (TransactionID, TransactionDateID, TransactionTypeID)
				VALUES (source.TransactionID, source.TransactionDateID, source.TransactionTypeID);

			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES
				('usp_LoadProductionTables', @CurrentStep, 'Success',
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));

			----------------------
			-- 1.5 Load DimAccount
			----------------------
			SET @CurrentStep = 'LoadDimAccount';
			SET @StepStartTime = GETDATE();

			MERGE Production.DimAccount AS target
			USING (
				SELECT DISTINCT 
					AccountID,
					AccountType,
					PaymentBehavior,
					c.CustomerSK
				FROM Staging.Loan sl
				JOIN Production.DimCustomer c ON sl.CustomerID = c.CustomerID AND c.IsCurrent = 1
			) AS source
			ON (target.AccountID = source.AccountID)
			WHEN MATCHED THEN
				UPDATE SET
					AccountType = source.AccountType,
					PaymentBehavior = source.PaymentBehavior,
					CustomerSK = source.CustomerSK
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (AccountID, AccountType, PaymentBehavior, CustomerSK)
				VALUES (source.AccountID, source.AccountType, source.PaymentBehavior, source.CustomerSK);

			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog 
				(ProcedureName,StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES ('usp_LoadProductionTables', @CurrentStep, 'Success', 
					@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));

		END


		-- ===================
		-- 2. LOAD FACT TABLES
		-- ===================
		IF @LoadFacts = 1
		BEGIN
			--------------------
			-- 2.1 Load FactLoan
			--------------------
			SET @CurrentStep = 'LoadFactLoan';
			SET @StepStartTime = GETDATE();
			
			-- Create temporary table for staging loan data
			CREATE TABLE #StagedLoans (
				CustomerSK INT,
				LoanDateID INT,
				LoanTypeID INT,
				LoanAmount DECIMAL(18,2),
				LoanDurationMonths INT,
				InterestRate FLOAT,
				DelayedPayments INT,
				Balance DECIMAL(18,2),
				MonthlyPayment DECIMAL(18,2),
				LoanToValueRatio DECIMAL(5,2),
				PaymentStatus NVARCHAR(50),
				NaturalKeyHash VARBINARY(16)
			);

			-- Stage the loan data with hash key
			INSERT INTO #StagedLoans (
				CustomerSK, LoanDateID, LoanTypeID, LoanAmount, 
				LoanDurationMonths, InterestRate, DelayedPayments,
				Balance, MonthlyPayment, LoanToValueRatio, PaymentStatus,
				NaturalKeyHash
			)
			SELECT
				c.CustomerSK,
				d.DateID,
				lt.LoanTypeID,
				sl.LoanAmount,
				sl.LoanDurationMonths,
				sl.InterestRate,
				sl.DelayedPayments,
				sl.Balance,
				sl.MonthlyPayment,
				sl.LoanToValueRatio,
				sl.PaymentStatus,
				HASHBYTES('MD5', 
					CAST(c.CustomerSK AS VARCHAR) + '|' + 
					CAST(d.DateID AS VARCHAR) + '|' + 
					CAST(lt.LoanTypeID AS VARCHAR) + '|' + 
					CAST(sl.LoanAmount AS VARCHAR)
				) AS NaturalKeyHash
			FROM Staging.Loan sl WITH (NOLOCK)
			JOIN Production.DimDate d WITH (NOLOCK) 
				ON CONVERT(DATE, sl.LoanDate) = d.FullDate
			JOIN Production.DimCustomer c WITH (NOLOCK) 
				ON sl.CustomerID = c.CustomerID AND c.IsCurrent = 1
			JOIN Production.DimLoanType lt WITH (NOLOCK) 
				ON TRIM(sl.LoanType) = TRIM(lt.LoanType);

			-- Create unique index on staging table
			CREATE UNIQUE NONCLUSTERED INDEX IX_StagedLoans_NaturalKey 
			ON #StagedLoans(NaturalKeyHash);

			-- Then modify the insertion logic to use MERGE:
			MERGE Production.FactLoan WITH (HOLDLOCK) AS target
			USING #StagedLoans AS source
			ON (
				target.CustomerSK = source.CustomerSK AND
				target.LoanDateID = source.LoanDateID AND
				target.LoanTypeID = source.LoanTypeID AND
				target.LoanAmount = source.LoanAmount
			)
			WHEN MATCHED THEN
				UPDATE SET 
					target.Balance = source.Balance,
					target.DelayedPayments = source.DelayedPayments,
					target.PaymentStatus = source.PaymentStatus,
					target.ModifiedDate = GETDATE()
			WHEN NOT MATCHED THEN
				INSERT (
					LoanDateID, CustomerSK, LoanTypeID, LoanAmount, 
					LoanDurationMonths, InterestRate, DelayedPayments,
					Balance, MonthlyPayment, LoanToValueRatio,
					PaymentStatus, BatchID, LoadDate
				)
				VALUES (
					source.LoanDateID, source.CustomerSK, source.LoanTypeID, source.LoanAmount, 
					source.LoanDurationMonths, source.InterestRate, source.DelayedPayments,
					source.Balance, source.MonthlyPayment, source.LoanToValueRatio,
					source.PaymentStatus, @BatchID, GETDATE()
				);
			
			SET @RowsProcessed = @@ROWCOUNT;
			
			-- Update existing loans
			UPDATE f WITH (ROWLOCK)
			SET 
				f.Balance = s.Balance,
				f.DelayedPayments = s.DelayedPayments,
				f.PaymentStatus = s.PaymentStatus,
				f.ModifiedDate = GETDATE()
			FROM Production.FactLoan f
			JOIN #StagedLoans s ON
				s.CustomerSK = f.CustomerSK AND
				s.LoanDateID = f.LoanDateID AND
				s.LoanAmount = f.LoanAmount AND
				s.LoanTypeID = f.LoanTypeID;
				
			SET @RowsProcessed += @@ROWCOUNT;
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			
			-- Clean up
			DROP TABLE #StagedLoans;
			
			------------------------
			-- 2.2 Load FactCustomer
			------------------------
			SET @CurrentStep = 'LoadFactCustomer';
			SET @StepStartTime = GETDATE();
			
			-- Create staging table
			CREATE TABLE #StagedCustomers (
				CustomerSK INT,
				DateSK INT,
				AnnualIncome DECIMAL(18,2),
				TotalAssets DECIMAL(18,2),
				TotalLiabilities DECIMAL(18,2),
				AnnualExpenses DECIMAL(18,2),
				MonthlySavings DECIMAL(18,2),
				AnnualBonus DECIMAL(18,2),
				DebtToIncomeRatio FLOAT,
				PaymentHistoryYears INT,
				JobTenureMonths INT,
				NumDependents INT
			);
			
			-- Stage customer data
			INSERT INTO #StagedCustomers (
				CustomerSK, DateSK, AnnualIncome, TotalAssets, 
				TotalLiabilities, AnnualExpenses, MonthlySavings, 
				AnnualBonus, DebtToIncomeRatio, PaymentHistoryYears,
				JobTenureMonths, NumDependents
			)
			SELECT 
				c.CustomerSK,
				d.DateID,
				MAX(sl.AnnualIncome) AS AnnualIncome,
				SUM(sl.TotalAssets) AS TotalAssets,
				SUM(sl.TotalLiabilities) AS TotalLiabilities,
				SUM(sl.AnnualExpenses) AS AnnualExpenses,
				AVG(sl.MonthlySavings) AS MonthlySavings,
				SUM(sl.AnnualBonus) AS AnnualBonus,
				AVG(sl.DebtToIncomeRatio) AS DebtToIncomeRatio,
				MAX(sl.PaymentHistoryYears) AS PaymentHistoryYears,
				MAX(sl.JobTenureMonths) AS JobTenureMonths,
				MAX(sl.NumDependents) AS NumDependents
			FROM Staging.Loan sl
			JOIN Production.DimCustomer c ON sl.CustomerID = c.CustomerID AND c.IsCurrent = 1
			JOIN Production.DimDate d ON CONVERT(DATE, sl.LoanDate) = d.FullDate
			GROUP BY c.CustomerSK, d.DateID;
			
			-- Merge to fact table
			MERGE Production.FactCustomer AS target
			USING #StagedCustomers AS source
			ON (target.CustomerSK = source.CustomerSK AND target.DateSK = source.DateSK)
			
			WHEN MATCHED THEN
				UPDATE SET 
					AnnualIncome = source.AnnualIncome,
					TotalAssets = source.TotalAssets,
					TotalLiabilities = source.TotalLiabilities,
					AnnualExpenses = source.AnnualExpenses,
					MonthlySavings = source.MonthlySavings,
					AnnualBonus = source.AnnualBonus,
					DebtToIncomeRatio = source.DebtToIncomeRatio,
					PaymentHistoryYears = source.PaymentHistoryYears,
					JobTenureMonths = source.JobTenureMonths,
					NumDependents = source.NumDependents
					
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					CustomerSK, DateSK, AnnualIncome, TotalAssets, TotalLiabilities, 
					AnnualExpenses, MonthlySavings, AnnualBonus, DebtToIncomeRatio, 
					PaymentHistoryYears, JobTenureMonths, NumDependents, BatchID
				)
				VALUES (
					source.CustomerSK, source.DateSK, source.AnnualIncome, source.TotalAssets, 
					source.TotalLiabilities, source.AnnualExpenses, source.MonthlySavings, 
					source.AnnualBonus, source.DebtToIncomeRatio, source.PaymentHistoryYears, 
					source.JobTenureMonths, source.NumDependents, @BatchID
				);
				
			SET @RowsProcessed = @@ROWCOUNT;
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			
			-- Clean up
			DROP TABLE #StagedCustomers;
			
			----------------------
			-- 2.3 Load FactCredit
			----------------------
			SET @CurrentStep = 'LoadFactCredit';
			SET @StepStartTime = GETDATE();

			-- Insert into FactCredit from customer and loan data
			INSERT INTO Production.FactCredit (
				CreditID,
				CustomerSK,
				AccountSK,
				CreditScore,
				NumBankAccounts,
				NumCreditCards,
				NumLoans,
				NumCreditInquiries,
				CreditUtilizationRatio,
				CreditHistoryLengthMonths,
				CreditMixRatio,
				BatchID
			)
			SELECT 
				ROW_NUMBER() OVER (ORDER BY c.CustomerSK) AS CreditID,
				c.CustomerSK,
				a.AccountSK,
				sl.CreditScore,
				sl.NumBankAccounts,
				sl.NumCreditCards,
				sl.NumLoans,
				sl.NumCreditInquiries,
				sl.CreditUtilizationRatio,
				sl.CreditHistoryLengthMonths,
				sl.CreditMixRatio,
				@BatchID
			FROM Production.DimCustomer c
			LEFT JOIN Production.DimAccount a ON c.CustomerSK = a.CustomerSK
			JOIN Staging.Loan sl ON c.CustomerID = sl.CustomerID
			WHERE c.IsCurrent = 1
			AND NOT EXISTS (
				SELECT 1 FROM Production.FactCredit fc 
				WHERE fc.CustomerSK = c.CustomerSK
			);

			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog 
				(ProcedureName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));


			-----------------------
			-- 2.4 Load FactAccount
			-----------------------
			SET @CurrentStep = 'LoadFactAccount';
			SET @StepStartTime = GETDATE();

			-- Insert account balance data
			INSERT INTO Production.FactAccount (
				AccountSK,
				AccountID,
				AccountBalance,
				BatchID
			)
			SELECT 
				a.AccountSK,
				a.AccountID,
				sl.AccountBalance,
				@BatchID
			FROM Production.DimAccount a
			JOIN Staging.Loan sl ON a.AccountID = sl.AccountID
			LEFT JOIN Production.FactAccount fa ON a.AccountSK = fa.AccountSK
			WHERE fa.AccountSK IS NULL;

			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));


			---------------------
			-- 2.5 Load FactFraud
			---------------------
			SET @CurrentStep = 'LoadFactFraud';
			SET @StepStartTime = GETDATE();
			
			-- Create staging table
			CREATE TABLE #StagedFraud (
				FraudID INT IDENTITY(1,1),
				CustomerSK INT,
				TransactionID INT,
				DistanceFromHome FLOAT NULL,
				DistanceFromLastTransaction FLOAT NULL,
				RatioToMedianTransactionAmount FLOAT NULL,
				IsOnlineTransaction BIT,
				IsUsedChip BIT,
				IsUsedPIN BIT,
				IsFraudulent BIT
			);
			
			-- Stage fraud data
			INSERT INTO #StagedFraud (
				CustomerSK,
				TransactionID,
				DistanceFromHome,
				DistanceFromLastTransaction,
				RatioToMedianTransactionAmount,
				IsOnlineTransaction,
				IsUsedChip,
				IsUsedPIN,
				IsFraudulent
			)
			SELECT
				c.CustomerSK,
				sf.TransactionID,
				sf.DistanceFromHome,
				sf.DistanceFromLastTransaction,
				sf.RatioToMedianTransactionAmount,
				sf.IsOnlineTransaction,
				sf.IsUsedChip,
				sf.IsUsedPIN,
				sf.IsFraudulent
			FROM Staging.Fraud sf
			JOIN Production.DimCustomer c ON sf.CustomerID = c.CustomerID AND c.IsCurrent = 1
			LEFT JOIN Production.FactLoan fl ON 
				c.CustomerSK = fl.CustomerSK;
			
			-- Insert new fraud records
			INSERT INTO Production.FactFraud WITH (TABLOCK) (
				TransactionID,
				TransactionDateID,
				CustomerSK,
				DistanceFromHome,
				DistanceFromLastTransaction,
				RatioToMedianTransactionAmount,
				IsOnlineTransaction,
				IsUsedChip,
				IsUsedPIN,
				IsFraudulent,
				BatchID
			)
			SELECT
				dt.TransactionID,
				dt.TransactionDateID,
				s.CustomerSK,
				sf.DistanceFromHome,
				sf.DistanceFromLastTransaction,
				sf.RatioToMedianTransactionAmount,
				sf.IsOnlineTransaction,
				sf.IsUsedChip,
				sf.IsUsedPIN,
				sf.IsFraudulent,
				@BatchID
			FROM #StagedFraud s
			JOIN Production.DimCustomer c ON s.CustomerSK = c.CustomerSK
			JOIN Staging.Fraud sf ON sf.CustomerID = c.CustomerID
			JOIN Production.DimTransaction dt ON sf.TransactionID = dt.TransactionID
			WHERE NOT EXISTS (
				SELECT 1 FROM Production.FactFraud ff 
				WHERE ff.CustomerSK = s.CustomerSK AND ff.TransactionID = dt.TransactionID
			);
			
			SET @RowsProcessed = @@ROWCOUNT;
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
			
			-- Clean up
			DROP TABLE #StagedFraud;
			
			----------------------
			-- 2.6 Load FactMarket
			----------------------
			SET @CurrentStep = 'LoadMarketData';
			SET @StepStartTime = GETDATE();

			-- First load market dimension
			MERGE Production.DimMarketIndex AS target
			USING (
				SELECT DISTINCT MarketName
				FROM Staging.Market
			) AS source
			ON (target.MarketName = source.MarketName)
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (MarketName)
				VALUES (source.MarketName);

			-- Then load fact market data
			MERGE Production.FactMarket AS target
			USING (
				SELECT 
					ROW_NUMBER() OVER (ORDER BY d.DateID, mi.MarketIndexSK) AS MarketID,
					d.DateID,
					mi.MarketIndexSK,
					sm.OpenValue,
					sm.CloseValue,
					sm.HighestValue,  
					sm.LowestValue,   
					sm.Volume,
					sm.InterestRate,
					sm.ExchangeRate,
					sm.GoldPrice,
					sm.OilPrice,
					sm.VIX,
					sm.TEDSpread,
					sm.EFFR
				FROM Staging.Market sm
				JOIN Production.DimDate d ON CONVERT(DATE, sm.MarketDate) = d.FullDate
				JOIN Production.DimMarketIndex mi ON mi.MarketName = sm.MarketName 
			) AS source
			ON (target.DateID = source.DateID AND target.MarketIndexSK = source.MarketIndexSK)

			WHEN MATCHED THEN
				UPDATE SET 
					OpenValue = source.OpenValue,
					CloseValue = source.CloseValue,
					HighestValue = source.HighestValue,
					LowestValue = source.LowestValue,
					Volume = source.Volume,
					InterestRate = source.InterestRate,
					ExchangeRate = source.ExchangeRate,
					GoldPrice = source.GoldPrice,
					OilPrice = source.OilPrice,
					VIX = source.VIX,
					TEDSpread = source.TEDSpread,
					EFFR = source.EFFR
					
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					MarketID, DateID, MarketIndexSK, OpenValue, CloseValue, 
					HighestValue, LowestValue, Volume, InterestRate, ExchangeRate,
					GoldPrice, OilPrice, VIX, TEDSpread, EFFR
				)
				VALUES (
					source.MarketID, source.DateID, source.MarketIndexSK, source.OpenValue, source.CloseValue,
					source.HighestValue, source.LowestValue, source.Volume, source.InterestRate, source.ExchangeRate,
					source.GoldPrice, source.OilPrice, source.VIX, source.TEDSpread, source.EFFR
				);
				
			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));

			----------------------------------
			-- 2.7 Load FactEconomicIndicators
			----------------------------------
			SET @CurrentStep = 'LoadEconomicData';
			SET @StepStartTime = GETDATE();

			-- Insert economic data directly from the normalized staging structure
			INSERT INTO Production.FactEconomicIndicators (
				EconomicID, ReportDateID, CountryID, UnemploymentRate, GDP, 
				DebtRatio, DeficitRatio, InflationRate, ConsumerPriceIndex, 
				HousePriceIndex, BatchID, LoadDate
			)
			SELECT 
				ROW_NUMBER() OVER (ORDER BY d.DateID, c.CountryID) AS EconomicID,
				d.DateID AS ReportDateID,
				c.CountryID,
				m.UnemploymentRate,
				m.GDP,
				m.DebtRatio,
				m.DeficitRatio,
				m.InflationRate,
				m.ConsumerPriceIndex,
				m.HousePriceIndex,
				@BatchID,
				GETDATE()
			FROM Staging.Macro m
			JOIN Production.DimDate d ON CONVERT(DATE, m.ReportDate) = d.FullDate
			JOIN Production.DimCountry c ON m.CountryName = c.CountryName
	
			WHERE NOT EXISTS (
				SELECT 1 FROM Production.FactEconomicIndicators fe
				WHERE fe.ReportDateID = d.DateID AND fe.CountryID = c.CountryID
			);

			SET @RowsProcessed = @@ROWCOUNT;

			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', @CurrentStep, 'Success', 
				@RowsProcessed, DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		END


		-- ====================
		-- 3. VALIDATION CHECKS
		-- ====================
		IF @ValidateAfterLoad = 1
		BEGIN
			SET @CurrentStep = 'PostLoadValidation';
			SET @StepStartTime = GETDATE();
			
			-- Validate customer counts
			DECLARE @DimCustomerCount INT, @FactCustomerCount INT;
			
			SELECT @DimCustomerCount = COUNT(*) 
			FROM Production.DimCustomer 
			WHERE IsCurrent = 1;
			
			SELECT @FactCustomerCount = COUNT(DISTINCT CustomerSK) 
			FROM Production.FactCustomer;
			
			-- Log any discrepancies
			IF ABS(@DimCustomerCount - @FactCustomerCount) > (@DimCustomerCount * 0.05)
			BEGIN
				INSERT INTO Production.ETLLog 
					(ProcedureName, StepName, Status, ErrorMessage)
				VALUES 
					('usp_LoadProductionTables', @CurrentStep, 'Warning', 
					'Customer count mismatch: ' + CAST(@DimCustomerCount AS VARCHAR) + 
					' in dimension vs ' + CAST(@FactCustomerCount AS VARCHAR) + ' in fact');
			END
			
			-- Validate loan counts
			DECLARE @StagingLoanCount INT, @FactLoanCount INT;
			
			SELECT @StagingLoanCount = COUNT(*) FROM Staging.Loan;
			SELECT @FactLoanCount = COUNT(*) FROM Production.FactLoan;
			
			-- Log any discrepancies
			IF ABS(@StagingLoanCount - @FactLoanCount) > (@StagingLoanCount * 0.05)
			BEGIN
				INSERT INTO Production.ETLLog 
					(ProcedureName, StepName, Status, ErrorMessage)
				VALUES 
					('usp_LoadProductionTables', @CurrentStep, 'Warning', 
					'Loan count mismatch: ' + CAST(@StagingLoanCount AS VARCHAR) + 
					' in staging vs ' + CAST(@FactLoanCount AS VARCHAR) + ' in fact');
			END
			
			INSERT INTO Production.ETLLog 
				(ProcedureName, StepName, Status, ExecutionTimeSeconds)
			VALUES 
				('usp_LoadProductionTables', 'PostLoad Validation', 'Success', 
				DATEDIFF(SECOND, @StepStartTime, GETDATE()));
		END
		
		
		-- Log completion
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, ExecutionTimeSeconds)
		VALUES 
			('usp_LoadProductionTables', 'Production Load', 'Success', 
			DATEDIFF(SECOND, @StartTime, GETDATE()));
			
		-- Return success information
		SELECT 
			'Success' AS Status, 
			DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTimeSeconds,
			@BatchID AS BatchID;
	
	END TRY
	BEGIN CATCH			
		-- Capture error details
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
		
		-- Log the error
		INSERT INTO Production.ETLLog 
			(ProcedureName, StepName, Status, ErrorMessage)
		VALUES 
			('usp_LoadProductionTables', @CurrentStep, 'Failed', @ErrorMessage);
			
		-- Return error information
		SELECT 
			'Error' AS Status, 
			@ErrorMessage AS ErrorMessage,
			@CurrentStep AS FailedStep,
			DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTimeSeconds;
			
		-- Re-throw the error with the original severity
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState); 
	END CATCH;
END;
GO
