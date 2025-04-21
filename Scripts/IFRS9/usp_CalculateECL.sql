CREATE OR ALTER PROCEDURE IFRS9.usp_CalculateECL
	@AsOfDate DATE = NULL,
	@BatchID UNIQUEIDENTIFIER = NULL,
	@IncludeAllScenarios BIT = 0
AS
BEGIN
	SET NOCOUNT ON;
	
	IF @AsOfDate IS NULL
		SET @AsOfDate = CAST(GETDATE() AS DATE);
		
	IF @BatchID IS NULL
		SET @BatchID = NEWID();
		
	DECLARE @LogMessage NVARCHAR(MAX);
	SET @LogMessage = 'Starting ECL calculation for date: ' + CAST(@AsOfDate AS VARCHAR);
	
	-- Attempt to log, but continue if logging fails
	BEGIN TRY
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_CalculateECL', 'Start', 'Processing', @LogMessage);
	END TRY
	BEGIN CATCH
		-- Silently continue if logging fails
	END CATCH
	
	-- Get DateID for the AsOfDate
	DECLARE @DateID INT;
	SELECT @DateID = DateID FROM Production.DimDate WHERE FullDate = @AsOfDate;
	
	IF @DateID IS NULL
	BEGIN
		-- If date doesn't exist in DimDate, generate a DateID
		SET @DateID = CONVERT(INT, CONVERT(VARCHAR, @AsOfDate, 112));
		
		-- Log warning about missing date
		BEGIN TRY
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_CalculateECL', 'Warning', 'Processing', 
				'Date not found in DimDate: ' + CAST(@AsOfDate AS VARCHAR) + 
				'. Using generated DateID: ' + CAST(@DateID AS VARCHAR));
		END TRY
		BEGIN CATCH
			-- Continue silently if logging fails
		END CATCH
	END;
	
	BEGIN TRY
		-- Use dynamic SQL to avoid column validation errors at procedure creation time
		DECLARE @SQL NVARCHAR(MAX);
		
		-- Check that required tables exist
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLParameters' AND schema_id = SCHEMA_ID('IFRS9'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLStaging' AND schema_id = SCHEMA_ID('IFRS9'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLCalculation' AND schema_id = SCHEMA_ID('IFRS9'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'MacroeconomicScenarios' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			RAISERROR('One or more required tables do not exist. Cannot execute ECL calculation.', 16, 1);
			RETURN -1;
		END
		
		-- Delete existing calculations for the date
		SET @SQL = N'DELETE FROM IFRS9.ECLCalculation WHERE AsOfDate = @AsOfDate;';
		EXEC sp_executesql @SQL, N'@AsOfDate DATE', @AsOfDate;
		
		-- Begin constructing the dynamic SQL for the INSERT operation
		SET @SQL = N'
		-- For each active scenario, calculate ECL
		WITH ActiveScenarios AS (
			SELECT 
				ScenarioID, 
				ScenarioName,
				ScenarioWeight
			FROM IFRS9.MacroeconomicScenarios
			WHERE IsActive = 1';
			
		-- Add date filtering if columns exist
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.MacroeconomicScenarios') AND name = 'EffectiveDate')
			SET @SQL = @SQL + N'
			AND EffectiveDate <= @AsOfDate';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.MacroeconomicScenarios') AND name = 'ExpiryDate')
			SET @SQL = @SQL + N'
			AND (ExpiryDate IS NULL OR ExpiryDate >= @AsOfDate)';
			
		-- Add scenario filtering
		SET @SQL = @SQL + N'
			AND (ScenarioName = ''Base'' OR @IncludeAllScenarios = 1)
		)
		
		-- Calculate ECL for each account under each applicable scenario
		INSERT INTO IFRS9.ECLCalculation (';
		
		-- Dynamically build column list based on what exists in the table
		DECLARE @ColumnList NVARCHAR(MAX) = '';
		
		-- Check for each required column
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'CustomerSK')
			SET @ColumnList = @ColumnList + 'CustomerSK, ';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'AccountSK')
			SET @ColumnList = @ColumnList + 'AccountSK, ';
			
		SET @ColumnList = @ColumnList + 'AsOfDate, ';
		
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'DateID')
			SET @ColumnList = @ColumnList + 'DateID, ';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'ECLStage')
			SET @ColumnList = @ColumnList + 'ECLStage, ';
			
		SET @ColumnList = @ColumnList + 'ECL_12Month, ECL_Lifetime, AppliedECL, GrossCarryingAmount, NetCarryingAmount, ';
		
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'ScenarioID')
			SET @ColumnList = @ColumnList + 'ScenarioID, ';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'PD_Applied')
			SET @ColumnList = @ColumnList + 'PD_Applied, ';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'LGD_Applied')
			SET @ColumnList = @ColumnList + 'LGD_Applied, ';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'EAD_Applied')
			SET @ColumnList = @ColumnList + 'EAD_Applied, ';
			
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLCalculation') AND name = 'DiscountFactor')
			SET @ColumnList = @ColumnList + 'DiscountFactor';
		ELSE
			-- Remove trailing comma if DiscountFactor doesn't exist
			SET @ColumnList = LEFT(@ColumnList, LEN(@ColumnList) - 1);
			
		-- Add column list to the SQL
		SET @SQL = @SQL + @ColumnList + ')
		SELECT';
		
		-- Now build the SELECT portion based on available columns
		DECLARE @SelectList NVARCHAR(MAX) = '';
		
		-- Add CustomerSK if it exists in parameters table
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLParameters') AND name = 'CustomerSK')
			SET @SelectList = @SelectList + '
			p.CustomerSK,';
		ELSE
			SET @SelectList = @SelectList + '
			1 AS CustomerSK,'; -- Default value
			
		-- Add AccountSK if it exists in parameters table
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLParameters') AND name = 'AccountSK')
			SET @SelectList = @SelectList + '
			p.AccountSK,';
		ELSE
			SET @SelectList = @SelectList + '
			1 AS AccountSK,'; -- Default value
			
		SET @SelectList = @SelectList + '
			@AsOfDate,
			@DateID,';
			
		-- Add ECLStage if it exists in staging table
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLStaging') AND name = 'ECLStage')
			SET @SelectList = @SelectList + '
			s.ECLStage,';
		ELSE
			SET @SelectList = @SelectList + '
			1 AS ECLStage,'; -- Default to Stage 1
			
		-- Add 12-month ECL calculation
		SET @SelectList = @SelectList + '
			-- 12-month ECL calculation
			ROUND(p.PD_12Month * p.LGD * p.EAD * 
				CASE
					WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
					ELSE POWER(1 / (1 + p.EIR), 1.0)
				END * sc.ScenarioWeight, 2) AS ECL_12Month,
			
			-- Lifetime ECL calculation 
			ROUND(p.PD_Lifetime * p.LGD * p.EAD *
				CASE
					WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
					ELSE POWER(1 / (1 + p.EIR), p.Maturity / 24.0)
				END * sc.ScenarioWeight, 2) AS ECL_Lifetime,
			
			-- Applied ECL depends on the stage
			CASE s.ECLStage
				WHEN 1 THEN 
					ROUND(p.PD_12Month * p.LGD * p.EAD * 
						CASE
							WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
							ELSE POWER(1 / (1 + p.EIR), 1.0)
						END * sc.ScenarioWeight, 2)
				ELSE 
					ROUND(p.PD_Lifetime * p.LGD * p.EAD *
						CASE
							WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
							ELSE POWER(1 / (1 + p.EIR), p.Maturity / 24.0)
						END * sc.ScenarioWeight, 2)
			END AS AppliedECL,';
			
		-- Add carrying amount calculations based on available columns
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'CurrentBalance')
		BEGIN
			SET @SelectList = @SelectList + '
			-- Gross carrying amount
			l.CurrentBalance AS GrossCarryingAmount,
			
			-- Net carrying amount
			l.CurrentBalance - 
				CASE s.ECLStage
					WHEN 1 THEN 
						ROUND(p.PD_12Month * p.LGD * p.EAD * 
							CASE
								WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
								ELSE POWER(1 / (1 + p.EIR), 1.0)
							END * sc.ScenarioWeight, 2)
					ELSE 
						ROUND(p.PD_Lifetime * p.LGD * p.EAD *
							CASE
								WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
								ELSE POWER(1 / (1 + p.EIR), p.Maturity / 24.0)
							END * sc.ScenarioWeight, 2)
				END AS NetCarryingAmount,';
		END
		ELSE
		BEGIN
			SET @SelectList = @SelectList + '
			-- Default gross carrying amount
			10000 AS GrossCarryingAmount,
			
			-- Default net carrying amount
			10000 - 
				CASE s.ECLStage
					WHEN 1 THEN 
						ROUND(p.PD_12Month * p.LGD * p.EAD * 
							CASE
								WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
								ELSE POWER(1 / (1 + p.EIR), 1.0)
							END * sc.ScenarioWeight, 2)
					ELSE 
						ROUND(p.PD_Lifetime * p.LGD * p.EAD *
							CASE
								WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
								ELSE POWER(1 / (1 + p.EIR), p.Maturity / 24.0)
							END * sc.ScenarioWeight, 2)
				END AS NetCarryingAmount,';
		END
			
		-- Add ScenarioID
		SET @SelectList = @SelectList + '
			sc.ScenarioID,';
			
		-- Add applied risk parameters
		SET @SelectList = @SelectList + '
			-- Store applied PD based on stage
			CASE s.ECLStage
				WHEN 1 THEN p.PD_12Month
				ELSE p.PD_Lifetime
			END AS PD_Applied,
			
			p.LGD AS LGD_Applied,
			p.EAD AS EAD_Applied,';
			
		-- Add discount factor
		SET @SelectList = @SelectList + '
			-- Calculate and store the discount factor
			CASE
				WHEN s.ECLStage = 1 THEN
					CASE
						WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
						ELSE POWER(1 / (1 + p.EIR), 1.0)
					END
				ELSE
					CASE
						WHEN p.Maturity <= 12 THEN POWER(1 / (1 + p.EIR), p.Maturity / 12.0)
						ELSE POWER(1 / (1 + p.EIR), p.Maturity / 24.0)
					END
			END AS DiscountFactor';
		
		-- Table joins
		SET @SQL = @SQL + @SelectList + '
		FROM IFRS9.ECLParameters p
		JOIN IFRS9.ECLStaging s ON 
			p.CustomerSK = s.CustomerSK AND 
			p.AccountSK = s.AccountSK AND 
			p.AsOfDate = s.AsOfDate';
		
		-- Add FactLoan join if it exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactLoan' AND schema_id = SCHEMA_ID('Production'))
		BEGIN
			SET @SQL = @SQL + '
		LEFT JOIN Production.FactLoan l ON 
			p.CustomerSK = l.CustomerSK AND 
			p.AccountSK = l.AccountSK';
		END
		
		-- Join to scenarios and add WHERE clause
		SET @SQL = @SQL + '
		JOIN ActiveScenarios sc ON p.MacroeconomicScenarioID = sc.ScenarioID
		WHERE p.AsOfDate = @AsOfDate';
		
		-- Add IsActive filter if column exists
		IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'IsActive')
			SET @SQL = @SQL + '
		AND (l.IsActive = 1 OR l.IsActive IS NULL)';
			
		-- Execute the dynamic SQL with parameters
		EXEC sp_executesql @SQL, N'@AsOfDate DATE, @DateID INT, @IncludeAllScenarios BIT',
			@AsOfDate, @DateID, @IncludeAllScenarios;
			
		-- Log successful completion
		SET @LogMessage = 'ECL calculation completed successfully. Records processed: ' + 
			CAST(@@ROWCOUNT AS VARCHAR);
		
		BEGIN TRY
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_CalculateECL', 'Complete', 'Success', @LogMessage);
		END TRY
		BEGIN CATCH
			-- Continue silently if logging fails
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
			VALUES (@BatchID, 'usp_CalculateECL', 'Error', 'Failed', @ErrorMessage);
		END TRY
		BEGIN CATCH
			-- Continue silently if logging fails
		END CATCH
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
		RETURN -1;
	END CATCH;
END;
GO
