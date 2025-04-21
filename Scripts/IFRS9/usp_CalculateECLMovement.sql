CREATE OR ALTER PROCEDURE IFRS9.usp_CalculateECLMovement
	@FromDate DATE,
	@ToDate DATE,
	@BatchID UNIQUEIDENTIFIER = NULL
AS
BEGIN
	SET NOCOUNT ON;
		
	IF @BatchID IS NULL
		SET @BatchID = NEWID();
		
	DECLARE @LogMessage NVARCHAR(MAX);
	SET @LogMessage = 'Starting ECL movement analysis from ' + 
		CAST(@FromDate AS VARCHAR) + ' to ' + CAST(@ToDate AS VARCHAR);
	
	-- Log procedure start (with error handling in case ETLLog doesn't have required columns)
	BEGIN TRY
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_CalculateECLMovement', 'Start', 'Processing', @LogMessage);
	END TRY
	BEGIN CATCH
		-- Silently continue if logging fails
	END CATCH
	
	BEGIN TRY
		-- Check that required tables exist
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLMovementAnalysis' AND schema_id = SCHEMA_ID('IFRS9'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLCalculation' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			RAISERROR('One or more required tables do not exist. Cannot execute ECL movement analysis.', 16, 1);
			RETURN -1;
		END
		
		-- Delete existing movement analysis for the period
		EXEC sp_executesql N'DELETE FROM IFRS9.ECLMovementAnalysis 
		WHERE FromDate = @FromDate AND ToDate = @ToDate', 
		N'@FromDate DATE, @ToDate DATE', @FromDate, @ToDate;
		
		-- Use dynamic SQL to avoid column validation errors at creation time
		DECLARE @SQL NVARCHAR(MAX);
		
		SET @SQL = N'
		-- Insert ECL movement analysis
		INSERT INTO IFRS9.ECLMovementAnalysis (
			CustomerSK, AccountSK, FromDate, ToDate, OpeningECL, ClosingECL,
			NewLoansECL, DerecognizedLoansECL, TransferToStage1ECL, TransferToStage2ECL,
			TransferToStage3ECL, ModelParameterChangesECL, MacroeconomicChangesECL,
			OtherChangesECL, WriteOffsECL
		)
		SELECT
			COALESCE(c_from.CustomerSK, c_to.CustomerSK) AS CustomerSK,
			COALESCE(c_from.AccountSK, c_to.AccountSK) AS AccountSK,
			@FromDate AS FromDate,
			@ToDate AS ToDate,
			COALESCE(c_from.AppliedECL, 0) AS OpeningECL,
			COALESCE(c_to.AppliedECL, 0) AS ClosingECL,
			
			-- New loans (didn''t exist in FromDate)
			CASE WHEN c_from.CustomerSK IS NULL THEN COALESCE(c_to.AppliedECL, 0) ELSE 0 END AS NewLoansECL,
			
			-- Derecognized loans (don''t exist in ToDate)
			CASE WHEN c_to.CustomerSK IS NULL THEN COALESCE(c_from.AppliedECL, 0) ELSE 0 END AS DerecognizedLoansECL,';
			
		-- Add stage transfer logic, checking if ECLStaging table exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLStaging' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			SET @SQL = @SQL + N'
			-- Transfers between stages
			CASE 
				WHEN COALESCE(s_from.ECLStage, 0) <> 1 AND COALESCE(s_to.ECLStage, 0) = 1
					THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS TransferToStage1ECL,
			
			CASE 
				WHEN COALESCE(s_from.ECLStage, 0) <> 2 AND COALESCE(s_to.ECLStage, 0) = 2
					THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS TransferToStage2ECL,
			
			CASE 
				WHEN COALESCE(s_from.ECLStage, 0) <> 3 AND COALESCE(s_to.ECLStage, 0) = 3
					THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS TransferToStage3ECL,';
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + N'
			-- Default values for stage transfers if ECLStaging doesn''t exist
			0 AS TransferToStage1ECL,
			0 AS TransferToStage2ECL,
			0 AS TransferToStage3ECL,';
		END
		
		-- Add parameter changes logic if ECLParameters table exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLParameters' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			SET @SQL = @SQL + N'
			-- Model parameter changes (if stage remained the same but parameters changed)
			CASE 
				WHEN COALESCE(s_from.ECLStage, 0) = COALESCE(s_to.ECLStage, 0) AND 
					(COALESCE(p_from.PD_12Month, 0) <> COALESCE(p_to.PD_12Month, 0) OR 
					COALESCE(p_from.PD_Lifetime, 0) <> COALESCE(p_to.PD_Lifetime, 0) OR
					COALESCE(p_from.LGD, 0) <> COALESCE(p_to.LGD, 0) OR
					COALESCE(p_from.EAD, 0) <> COALESCE(p_to.EAD, 0)) 
				THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS ModelParameterChangesECL,
			
			-- Macroeconomic scenario changes
			CASE 
				WHEN COALESCE(p_from.MacroeconomicScenarioID, 0) <> COALESCE(p_to.MacroeconomicScenarioID, 0) 
				THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS MacroeconomicChangesECL,';
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + N'
			-- Default values for model and scenario changes
			0 AS ModelParameterChangesECL,
			0 AS MacroeconomicChangesECL,';
		END
		
		-- Add other changes and write-offs logic
		SET @SQL = @SQL + N'
			-- Other changes (residual)
			CASE';
		
		-- Build "other changes" logic based on table existence
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLStaging' AND schema_id = SCHEMA_ID('IFRS9')) AND
		EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLParameters' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			SET @SQL = @SQL + N' 
				WHEN COALESCE(s_from.ECLStage, 0) = COALESCE(s_to.ECLStage, 0) AND
					COALESCE(p_from.PD_12Month, 0) = COALESCE(p_to.PD_12Month, 0) AND 
					COALESCE(p_from.PD_Lifetime, 0) = COALESCE(p_to.PD_Lifetime, 0) AND
					COALESCE(p_from.LGD, 0) = COALESCE(p_to.LGD, 0) AND
					COALESCE(p_from.EAD, 0) = COALESCE(p_to.EAD, 0) AND
					COALESCE(p_from.MacroeconomicScenarioID, 0) = COALESCE(p_to.MacroeconomicScenarioID, 0) AND
					COALESCE(c_from.AppliedECL, 0) <> COALESCE(c_to.AppliedECL, 0)
				THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)';
		END
		
		SET @SQL = @SQL + N'
				WHEN c_from.CustomerSK IS NOT NULL AND c_to.CustomerSK IS NOT NULL 
					AND c_from.AccountSK = c_to.AccountSK
					AND NOT (c_from.CustomerSK IS NULL OR c_to.CustomerSK IS NULL)
					AND COALESCE(c_from.AppliedECL, 0) <> COALESCE(c_to.AppliedECL, 0)
				THEN COALESCE(c_to.AppliedECL, 0) - COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS OtherChangesECL,';
		
		-- Add write-off logic based on FactLoan existence and columns
		SET @SQL = @SQL + N'
			-- Write-offs';
		
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactLoan' AND schema_id = SCHEMA_ID('Production')) 
		BEGIN
			-- Check if both IsActive and WriteOffFlag columns exist
			DECLARE @HasIsActive BIT = 0;
			DECLARE @HasWriteOffFlag BIT = 0;
			
			SELECT @HasIsActive = CASE WHEN EXISTS (SELECT 1 FROM sys.columns 
				WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'IsActive') THEN 1 ELSE 0 END;
				
			SELECT @HasWriteOffFlag = CASE WHEN EXISTS (SELECT 1 FROM sys.columns 
				WHERE object_id = OBJECT_ID('Production.FactLoan') AND name = 'WriteOffFlag') THEN 1 ELSE 0 END;
				
			IF @HasIsActive = 1 AND @HasWriteOffFlag = 1
			BEGIN
				SET @SQL = @SQL + N'
			CASE 
				WHEN l_from.IsActive = 1 AND l_to.IsActive = 0 AND l_to.WriteOffFlag = 1
				THEN COALESCE(c_from.AppliedECL, 0)
				ELSE 0 
			END AS WriteOffsECL';
			END
			ELSE
			BEGIN
				SET @SQL = @SQL + N'
			0 AS WriteOffsECL';
			END
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + N'
			0 AS WriteOffsECL';
		END
		
		-- Add FROM clause and table joins
		SET @SQL = @SQL + N'
		FROM IFRS9.ECLCalculation c_from
		FULL OUTER JOIN IFRS9.ECLCalculation c_to ON 
			c_from.CustomerSK = c_to.CustomerSK AND
			c_from.AccountSK = c_to.AccountSK';
		
		-- Add ECLStaging joins if table exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLStaging' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			SET @SQL = @SQL + N'
		LEFT JOIN IFRS9.ECLStaging s_from ON 
			c_from.CustomerSK = s_from.CustomerSK AND
			c_from.AccountSK = s_from.AccountSK AND
			c_from.AsOfDate = s_from.AsOfDate
		LEFT JOIN IFRS9.ECLStaging s_to ON 
			c_to.CustomerSK = s_to.CustomerSK AND
			c_to.AccountSK = s_to.AccountSK AND
			c_to.AsOfDate = s_to.AsOfDate';
		END
		
		-- Add ECLParameters joins if table exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLParameters' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			SET @SQL = @SQL + N'
		LEFT JOIN IFRS9.ECLParameters p_from ON 
			c_from.CustomerSK = p_from.CustomerSK AND
			c_from.AccountSK = p_from.AccountSK AND
			c_from.AsOfDate = p_from.AsOfDate
		LEFT JOIN IFRS9.ECLParameters p_to ON 
			c_to.CustomerSK = p_to.CustomerSK AND
			c_to.AccountSK = p_to.AccountSK AND
			c_to.AsOfDate = p_to.AsOfDate';
		END
		
		-- Add FactLoan joins if table exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactLoan' AND schema_id = SCHEMA_ID('Production'))
		BEGIN
			SET @SQL = @SQL + N'
		LEFT JOIN Production.FactLoan l_from ON 
			c_from.CustomerSK = l_from.CustomerSK AND
			c_from.AccountSK = l_from.AccountSK
		LEFT JOIN Production.FactLoan l_to ON 
			c_to.CustomerSK = l_to.CustomerSK AND
			c_to.AccountSK = l_to.AccountSK';
		END
		
		-- Add WHERE clause
		SET @SQL = @SQL + N'
		WHERE (c_from.AsOfDate = @FromDate OR c_from.AsOfDate IS NULL)
		AND (c_to.AsOfDate = @ToDate OR c_to.AsOfDate IS NULL)
		AND (c_from.CustomerSK IS NOT NULL OR c_to.CustomerSK IS NOT NULL)';
		
		-- Execute the dynamic SQL
		EXEC sp_executesql @SQL, N'@FromDate DATE, @ToDate DATE',
			@FromDate, @ToDate;
			
		-- Log successful completion
		SET @LogMessage = 'ECL movement analysis completed successfully. Records processed: ' + 
			CAST(@@ROWCOUNT AS VARCHAR);
			
		BEGIN TRY
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_CalculateECLMovement', 'Complete', 'Success', @LogMessage);
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
			VALUES (@BatchID, 'usp_CalculateECLMovement', 'Error', 'Failed', @ErrorMessage);
		END TRY 
		BEGIN CATCH
			-- Continue silently if logging fails
		END CATCH
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
		RETURN -1;
	END CATCH;
END;
GO
