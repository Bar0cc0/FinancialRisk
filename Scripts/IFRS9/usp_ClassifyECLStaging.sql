CREATE OR ALTER PROCEDURE IFRS9.usp_ClassifyECLStaging
	@AsOfDate DATE = NULL,
	@BatchID UNIQUEIDENTIFIER = NULL
AS
BEGIN
	SET NOCOUNT ON;
	
	IF @AsOfDate IS NULL
		SET @AsOfDate = CAST(GETDATE() AS DATE);
		
	IF @BatchID IS NULL
		SET @BatchID = NEWID();
		
	DECLARE @LogMessage NVARCHAR(MAX);
	SET @LogMessage = 'Starting ECL staging classification for date: ' + CAST(@AsOfDate AS VARCHAR);
	
	-- Check if ETLLog has necessary columns before logging
	IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Production'))
	AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'BatchID')
	AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'Message')
	BEGIN
		-- Log procedure start
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_ClassifyECLStaging', 'Start', 'Processing', @LogMessage);
	END
	
	BEGIN TRY
		-- Use dynamic SQL to avoid column validation at creation time
		DECLARE @DynamicSQL NVARCHAR(MAX);
		
		-- Check if all required tables exist before proceeding
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimAccount' AND schema_id = SCHEMA_ID('Production'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DimCustomer' AND schema_id = SCHEMA_ID('Production'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FactLoan' AND schema_id = SCHEMA_ID('Production'))
			OR NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ECLStaging' AND schema_id = SCHEMA_ID('IFRS9'))
		BEGIN
			RAISERROR('One or more required tables do not exist. Cannot execute ECL staging classification.', 16, 1);
			RETURN -1;
		END
		
		-- First, make sure the ECLStaging table has the AccountSK column
		IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('IFRS9.ECLStaging') AND name = 'AccountSK')
		BEGIN
			RAISERROR('IFRS9.ECLStaging table is missing the AccountSK column.', 16, 1);
			RETURN -1;
		END
		
		-- Set up the dynamic SQL for the MERGE operation with safer CTE handling
		SET @DynamicSQL = N'
		-- First check if we have any previous staging data
		DECLARE @HasPreviousData BIT = 0;

		IF EXISTS (
			SELECT TOP 1 1 
			FROM IFRS9.ECLStaging 
			WHERE AsOfDate < @AsOfDate
		)
			SET @HasPreviousData = 1;

		-- Insert new staging classifications - dynamically handle first-run scenario
		MERGE INTO IFRS9.ECLStaging AS target
		USING (
			SELECT
				a.CustomerSK,
				a.AccountSK,
				@AsOfDate AS AsOfDate,
				CASE
					-- Stage 3: Credit-impaired assets
					WHEN l.IsDefaulted = 1 
					OR l.DaysPastDue >= 90 
					OR c.BankruptcyFlag = 1 
					OR l.ChargeOffFlag = 1 
						THEN 3
						
					-- Stage 2: Significant increase in credit risk
					WHEN c.CreditScoreDecreasePercent >= 15
					OR l.DaysPastDue BETWEEN 30 AND 89
					OR c.WatchListFlag = 1
					OR l.ModifiedLoanFlag = 1
					OR l.ForbearanceFlag = 1
						THEN 2
						
					-- Stage 1: All other performing loans
					ELSE 1
				END AS ECLStage,
				-- No previous stage reference for first run
				CASE WHEN @HasPreviousData = 0 THEN 1 
					ELSE (SELECT TOP 1 s.ECLStage FROM IFRS9.ECLStaging s 
						WHERE s.CustomerSK = a.CustomerSK AND s.AccountSK = a.AccountSK
						AND s.AsOfDate = (SELECT MAX(AsOfDate) FROM IFRS9.ECLStaging WHERE AsOfDate < @AsOfDate)
						)
				END AS PreviousStage,
				-- Stage change date is AsOfDate for first run
				CASE WHEN @HasPreviousData = 0 THEN @AsOfDate 
					ELSE CASE WHEN 
							(SELECT TOP 1 s.ECLStage FROM IFRS9.ECLStaging s 
							WHERE s.CustomerSK = a.CustomerSK AND s.AccountSK = a.AccountSK
							AND s.AsOfDate = (SELECT MAX(AsOfDate) FROM IFRS9.ECLStaging WHERE AsOfDate < @AsOfDate)) <>
							CASE
								WHEN l.IsDefaulted = 1 OR l.DaysPastDue >= 90 OR c.BankruptcyFlag = 1 OR l.ChargeOffFlag = 1 THEN 3
								WHEN c.CreditScoreDecreasePercent >= 15 OR l.DaysPastDue BETWEEN 30 AND 89 OR c.WatchListFlag = 1 
									OR l.ModifiedLoanFlag = 1 OR l.ForbearanceFlag = 1 THEN 2
								ELSE 1
							END
						THEN @AsOfDate
						ELSE NULL
						END
				END AS StageChangeDate,
				-- Change reason is Initial Classification for first run 
				CASE WHEN @HasPreviousData = 0 THEN ''Initial Classification'' 
					ELSE ''Classification Update'' 
				END AS StageChangeReason,
				CASE
					WHEN c.CreditScoreDecreasePercent >= 15
					OR l.DaysPastDue BETWEEN 30 AND 89
					OR c.WatchListFlag = 1
					OR l.ModifiedLoanFlag = 1
					OR l.ForbearanceFlag = 1
						THEN 1
					ELSE 0
				END AS SignificantIncreaseInCreditRisk,
				CASE
					WHEN l.IsDefaulted = 1 
					OR l.DaysPastDue >= 90 
					OR c.BankruptcyFlag = 1 
					OR l.ChargeOffFlag = 1 
						THEN 1
					ELSE 0
				END AS CreditImpaired,
				l.DaysPastDue
			FROM Production.DimAccount a
			JOIN Production.FactLoan l ON a.AccountSK = l.AccountSK
			JOIN Production.DimCustomer c ON l.CustomerSK = c.CustomerSK
			WHERE l.IsActive = 1
			AND (a.AccountClosedDate IS NULL OR NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''Production.DimAccount'') AND name = ''AccountClosedDate''))
		) AS source
		ON (
			target.CustomerSK = source.CustomerSK AND
			target.AccountSK = source.AccountSK AND
			target.AsOfDate = source.AsOfDate
		)
		WHEN MATCHED THEN
			UPDATE SET
				target.ECLStage = source.ECLStage,
				target.PreviousStage = source.PreviousStage,
				target.StageChangeReason = source.StageChangeReason,
				target.SignificantIncreaseInCreditRisk = source.SignificantIncreaseInCreditRisk,
				target.CreditImpaired = source.CreditImpaired,
				target.DaysPastDue = source.DaysPastDue,
				target.RecordedDate = GETDATE()
		WHEN NOT MATCHED THEN
			INSERT (
				CustomerSK, AccountSK, AsOfDate, ECLStage, PreviousStage, 
				StageChangeReason, SignificantIncreaseInCreditRisk, 
				CreditImpaired, DaysPastDue
			)
			VALUES (
				source.CustomerSK, source.AccountSK, source.AsOfDate, source.ECLStage, 
				source.PreviousStage, source.StageChangeReason,
				source.SignificantIncreaseInCreditRisk, source.CreditImpaired, source.DaysPastDue
			);';
			
		-- Execute the dynamic SQL with parameters
		EXEC sp_executesql @DynamicSQL, N'@AsOfDate DATE', @AsOfDate;
		
		-- Log successful completion if logging is available
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Production'))
		AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'BatchID')
		AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'Message')
		BEGIN
			SET @LogMessage = 'ECL staging classification completed successfully';
			
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_ClassifyECLStaging', 'Complete', 'Success', @LogMessage);
		END
		
		RETURN 0;
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
		DECLARE @ErrorState INT = ERROR_STATE();
		
		-- Log error if logging is available
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Production'))
		AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'BatchID')
		AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Production.ETLLog') AND name = 'Message')
		BEGIN
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_ClassifyECLStaging', 'Error', 'Failed', @ErrorMessage);
		END
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
		RETURN -1;
	END CATCH;
END;
GO