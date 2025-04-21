CREATE OR ALTER PROCEDURE IFRS9.usp_CalculateECLParameters
	@AsOfDate DATE = NULL,
	@BatchID UNIQUEIDENTIFIER = NULL,
	@ModelVersion VARCHAR(20) = '1.0'
AS
BEGIN
	SET NOCOUNT ON;
	
	IF @AsOfDate IS NULL
		SET @AsOfDate = CAST(GETDATE() AS DATE);
		
	IF @BatchID IS NULL
		SET @BatchID = NEWID();
		
	DECLARE @LogMessage NVARCHAR(MAX);
	SET @LogMessage = 'Starting ECL parameter calculation for date: ' + CAST(@AsOfDate AS VARCHAR);
	
	-- Log procedure start with error handling in case ETLLog doesn't have required columns
	BEGIN TRY
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_CalculateECLParameters', 'Start', 'Processing', @LogMessage);
	END TRY
	BEGIN CATCH
		-- Silently continue if logging fails
	END CATCH
	
	-- Get current base scenario ID with safer approach
	DECLARE @BaseScenarioID INT;
	
	-- Check if table and columns exist before querying
	IF OBJECT_ID('IFRS9.MacroeconomicScenarios', 'U') IS NOT NULL
	BEGIN
		BEGIN TRY
			SELECT TOP 1 @BaseScenarioID = ScenarioID 
			FROM IFRS9.MacroeconomicScenarios
			WHERE ScenarioName = 'Base'
			AND IsActive = 1
			ORDER BY ScenarioID;
		END TRY
		BEGIN CATCH
			-- Use a default if query fails
			SET @BaseScenarioID = 1;
		END CATCH
	END
	ELSE
	BEGIN
		-- Default if table doesn't exist
		SET @BaseScenarioID = 1;
	END
	
	IF @BaseScenarioID IS NULL
	BEGIN
		SET @BaseScenarioID = 1; -- Use a default ID if nothing was found
	END
	
	BEGIN TRY
		-- Use dynamic SQL to handle columns that may not exist yet
		DECLARE @SQL NVARCHAR(MAX);
		
		SET @SQL = N'
		-- Calculate ECL parameters dynamically based on what columns exist
		MERGE INTO IFRS9.ECLParameters AS target
		USING (
			SELECT
				1 AS CustomerSK,  -- Default will be replaced if actual column exists
				1 AS AccountSK,   -- Default will be replaced if actual column exists
				@AsOfDate AS AsOfDate,
				
				-- Default values for PD, LGD, etc.
				0.02 AS PD_12Month,    -- 2% default PD
				0.05 AS PD_Lifetime,   -- 5% default lifetime PD
				0.45 AS LGD,           -- 45% default LGD
				10000 AS EAD,          -- Default exposure amount
				0.05 AS EIR,           -- 5% default interest rate
				36 AS Maturity,        -- 3 years default maturity
				@BaseScenarioID AS MacroeconomicScenarioID,
				@ModelVersion AS ModelVersion
		';
		
		-- Check if the required tables exist and have required columns
		IF OBJECT_ID('Production.FactLoan', 'U') IS NOT NULL AND 
		OBJECT_ID('Production.DimCustomer', 'U') IS NOT NULL AND
		OBJECT_ID('IFRS9.ECLStaging', 'U') IS NOT NULL
		BEGIN
			-- Start dynamically building the complex part of the query
			-- We'll replace the simple SELECT above with a more sophisticated one
			-- that uses only columns that we confirm exist
			
			DECLARE @TableColumns NVARCHAR(MAX) = N'
			SELECT
				';
				
			-- Add CustomerSK if it exists in FactLoan
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CustomerSK')
				SET @TableColumns += 'l.CustomerSK, ';
			ELSE
				SET @TableColumns += '1 AS CustomerSK, ';
				
			-- Add AccountSK if it exists in FactLoan
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'AccountSK')
				SET @TableColumns += 'l.AccountSK, ';
			ELSE
				SET @TableColumns += '1 AS AccountSK, ';
				
			SET @TableColumns += '@AsOfDate AS AsOfDate, ';
			
			-- Add PD calculation that gracefully handles missing columns
			SET @TableColumns += '
			-- 12-month PD calculation with fallbacks for missing columns
			CASE
				WHEN 1=1 ';
				
			-- Add credit score based logic if column exists
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.DimCustomer') AND name = 'CreditScore')
			BEGIN
				SET @TableColumns += '
				THEN CASE
					WHEN c.CreditScore >= 750 THEN 0.005
					WHEN c.CreditScore >= 700 THEN 0.01
					WHEN c.CreditScore >= 650 THEN 0.02
					WHEN c.CreditScore >= 600 THEN 0.05
					WHEN c.CreditScore >= 550 THEN 0.10
					ELSE 0.20
				END ';
			END
			ELSE
			BEGIN
				SET @TableColumns += 'THEN 0.02 ';  -- Default 2% if no credit score
			END
			
			-- Add DaysPastDue modifier if column exists
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'DaysPastDue')
			BEGIN
				SET @TableColumns += '* 
				CASE
					WHEN l.DaysPastDue = 0 THEN 1.0
					WHEN l.DaysPastDue BETWEEN 1 AND 29 THEN 1.5
					WHEN l.DaysPastDue BETWEEN 30 AND 59 THEN 3.0
					WHEN l.DaysPastDue BETWEEN 60 AND 89 THEN 6.0
					ELSE 9.0
				END ';
			END
			
			SET @TableColumns += 'AS PD_12Month, ';
			
			-- Similar approach for Lifetime PD
			SET @TableColumns += '
			-- Lifetime PD calculation with fallbacks
			CASE
				WHEN 1=1 ';
				
			-- Add credit score based logic if column exists
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.DimCustomer') AND name = 'CreditScore')
			BEGIN
				SET @TableColumns += '
				THEN CASE
					WHEN c.CreditScore >= 750 THEN 0.015
					WHEN c.CreditScore >= 700 THEN 0.03
					WHEN c.CreditScore >= 650 THEN 0.06
					WHEN c.CreditScore >= 600 THEN 0.15
					WHEN c.CreditScore >= 550 THEN 0.30
					ELSE 0.50
				END ';
			END
			ELSE
			BEGIN
				SET @TableColumns += 'THEN 0.05 ';  -- Default 5% if no credit score
			END
			
			-- Add DaysPastDue modifier if column exists
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'DaysPastDue')
			BEGIN
				SET @TableColumns += '* 
				CASE
					WHEN l.DaysPastDue = 0 THEN 1.0
					WHEN l.DaysPastDue BETWEEN 1 AND 29 THEN 1.5
					WHEN l.DaysPastDue BETWEEN 30 AND 59 THEN 3.0
					WHEN l.DaysPastDue BETWEEN 60 AND 89 THEN 6.0
					ELSE 9.0
				END ';
			END
			
			-- Add term months modifier if column exists
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'TermMonths')
			BEGIN
				SET @TableColumns += '* (1 + (l.TermMonths / 60) * 0.2) ';
			END
			
			SET @TableColumns += 'AS PD_Lifetime, ';
			
			-- LGD calculation based on collateral
			SET @TableColumns += '
			-- LGD calculation with fallbacks
			CASE ';
			
			-- Add collateral based logic if column exists
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CollateralType')
			BEGIN
				SET @TableColumns += '
				WHEN l.CollateralType = ''REAL_ESTATE'' THEN 0.15
				WHEN l.CollateralType = ''VEHICLE'' THEN 0.35
				WHEN l.CollateralType = ''SECURITIES'' THEN 0.25
				WHEN l.CollateralType = ''NONE'' OR l.CollateralType IS NULL THEN 0.75
				ELSE 0.50
				';
			END
			ELSE
			BEGIN
				SET @TableColumns += 'WHEN 1=1 THEN 0.45 '; -- Default 45% LGD if no collateral info
			END
			
			SET @TableColumns += 'END AS LGD, ';
			
			-- EAD calculation
			SET @TableColumns += '
			-- EAD calculation with fallbacks
			CASE ';
			
			-- Add loan type based logic if columns exist
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'LoanType') AND
			EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CurrentBalance') AND
			EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CreditLimit')
			BEGIN
				SET @TableColumns += '
				WHEN l.LoanType = ''REVOLVING'' THEN l.CurrentBalance + (l.CreditLimit - l.CurrentBalance) * 0.5
				ELSE l.CurrentBalance
				';
			END
			ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CurrentBalance')
			BEGIN
				SET @TableColumns += 'WHEN 1=1 THEN l.CurrentBalance ';
			END
			ELSE
			BEGIN
				SET @TableColumns += 'WHEN 1=1 THEN 10000 '; -- Default 10000 EAD if no balance info
			END
			
			SET @TableColumns += 'END AS EAD, ';
			
			-- Interest rate
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'InterestRate')
				SET @TableColumns += 'l.InterestRate / 100 AS EIR, ';
			ELSE
				SET @TableColumns += '0.05 AS EIR, ';  -- Default 5% interest rate
			
			-- Maturity calculation
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'MaturityDate')
				SET @TableColumns += 'DATEDIFF(MONTH, @AsOfDate, l.MaturityDate) AS Maturity, ';
			ELSE
				SET @TableColumns += '36 AS Maturity, ';  -- Default 36-month maturity
			
			-- Scenario and model version 
			SET @TableColumns += '
			@BaseScenarioID AS MacroeconomicScenarioID,
			@ModelVersion AS ModelVersion
			FROM Production.FactLoan l
			';
			
			-- Add joins based on what tables exist
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CustomerSK')
				SET @TableColumns += 'JOIN Production.DimCustomer c ON l.CustomerSK = c.CustomerSK ';
			ELSE
				SET @TableColumns += 'CROSS JOIN (SELECT TOP 1 CustomerSK FROM Production.DimCustomer) c ';
			
			-- Join to ECLStaging
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLStaging') AND name = 'CustomerSK') AND
			EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLStaging') AND name = 'AccountSK') AND
			EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLStaging') AND name = 'AsOfDate')
			BEGIN
				SET @TableColumns += '
				JOIN IFRS9.ECLStaging s ON l.CustomerSK = s.CustomerSK AND l.AccountSK = s.AccountSK AND s.AsOfDate = @AsOfDate
				';
			END
			ELSE
			BEGIN
				SET @TableColumns += 'CROSS JOIN (SELECT TOP 1 1 AS FallbackJoin) s ';
			END
			
			-- Join to MacroeconomicScenarios
			SET @TableColumns += 'JOIN IFRS9.MacroeconomicScenarios ms ON ms.ScenarioID = @BaseScenarioID ';
			
			-- Apply filtering on active loans if possible
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'IsActive')
				SET @TableColumns += 'WHERE l.IsActive = 1 ';
				
			-- Filter by maturity date if possible
			IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'MaturityDate')
				SET @TableColumns += 'AND l.MaturityDate > @AsOfDate ';
				
			-- Replace the simple default query with our dynamically built one
			SET @SQL = REPLACE(@SQL, '-- Default values for PD, LGD, etc.', @TableColumns);
		END
		
		SET @SQL = @SQL + ') AS source
		ON (
			target.CustomerSK = source.CustomerSK AND
			target.AccountSK = source.AccountSK AND
			target.AsOfDate = source.AsOfDate
		)
		WHEN MATCHED THEN
			UPDATE SET
				target.PD_12Month = source.PD_12Month,
				target.PD_Lifetime = source.PD_Lifetime,
				target.LGD = source.LGD,
				target.EAD = source.EAD,
				target.EIR = source.EIR,
				target.Maturity = source.Maturity,
				target.MacroeconomicScenarioID = source.MacroeconomicScenarioID,
				target.ModelVersion = source.ModelVersion
		WHEN NOT MATCHED THEN
			INSERT (
				CustomerSK, AccountSK, AsOfDate, PD_12Month, PD_Lifetime,
				LGD, EAD, EIR, Maturity, MacroeconomicScenarioID, ModelVersion
			)
			VALUES (
				source.CustomerSK, source.AccountSK, source.AsOfDate,
				source.PD_12Month, source.PD_Lifetime, source.LGD,
				source.EAD, source.EIR, source.Maturity,
				source.MacroeconomicScenarioID, source.ModelVersion
			);';
		
		-- Execute the dynamic SQL with parameters
		EXEC sp_executesql @SQL, N'@AsOfDate DATE, @BaseScenarioID INT, @ModelVersion VARCHAR(20)',
			@AsOfDate, @BaseScenarioID, @ModelVersion;
			
		-- Log successful completion
		SET @LogMessage = 'ECL parameter calculation completed successfully';
		
		BEGIN TRY
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_CalculateECLParameters', 'Complete', 'Success', @LogMessage);
		END TRY
		BEGIN CATCH
			-- Continue if logging fails
		END CATCH
		
		RETURN 0;
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
		DECLARE @ErrorState INT = ERROR_STATE();
		
		-- Log error
		BEGIN TRY
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_CalculateECLParameters', 'Error', 'Failed', @ErrorMessage);
		END TRY
		BEGIN CATCH
			-- Continue if logging fails
		END CATCH
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
		RETURN -1;
	END CATCH;
END;
GO
