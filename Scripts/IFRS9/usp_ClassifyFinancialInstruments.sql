CREATE OR ALTER PROCEDURE IFRS9.usp_ClassifyFinancialInstruments
	@AsOfDate DATE = NULL,
	@BatchID UNIQUEIDENTIFIER = NULL,
	@ForceReclassification BIT = 0,
	@TestMode BIT = 0
AS
BEGIN
	SET NOCOUNT ON;
	
	IF @AsOfDate IS NULL
		SET @AsOfDate = CAST(GETDATE() AS DATE);
		
	IF @BatchID IS NULL
		SET @BatchID = NEWID();
		
	DECLARE @LogMessage NVARCHAR(MAX);
	SET @LogMessage = 'Starting financial instrument classification for date: ' + CAST(@AsOfDate AS VARCHAR);
	
	-- Check if ETLLog has necessary columns, otherwise use simplified logging
	IF EXISTS (SELECT 1 FROM sys.columns 
			WHERE object_id = OBJECT_ID('Production.ETLLog') 
			AND name IN ('BatchID', 'ProcedureName', 'StepName', 'Status', 'Message'))
	BEGIN
		-- Log procedure start with full columns
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_ClassifyFinancialInstruments', 'Start', 'Processing', @LogMessage);
	END
	ELSE
	BEGIN
		-- Log with available columns
		BEGIN TRY
			INSERT INTO Production.ETLLog (ExecutionDateTime, Status)
			VALUES (GETDATE(), 'Starting IFRS9 classification');
		END TRY
		BEGIN CATCH
			-- Silently continue if logging fails
		END CATCH
	END
	
	BEGIN TRY
		-- Skip actual processing if in test mode
		IF @TestMode = 0
		BEGIN
			-- Check if account table has IFRS9 columns
			DECLARE @HasIFRS9Columns BIT = 0;
			
			-- Check if required columns exist
			SELECT @HasIFRS9Columns = CASE 
				WHEN EXISTS (
					SELECT 1 FROM sys.columns 
					WHERE object_id = OBJECT_ID('Production.DimAccount')
					AND name IN ('IsActive', 'AccountClosedDate', 'HoldToMaturityFlag')
				) THEN 1 ELSE 0 END;
			
			IF @HasIFRS9Columns = 1
			BEGIN
				-- Use dynamic SQL to handle the case with IFRS9 columns
				DECLARE @DynamicSQL NVARCHAR(MAX);
				
				SET @DynamicSQL = N'
				MERGE INTO IFRS9.FinancialInstrumentClassification AS target
				USING (
					SELECT
						a.CustomerSK,
						a.AccountSK,
						a.AccountType AS InstrumentTypeCode,
						CASE
							-- Classification logic based on instrument type and business model
							WHEN a.AccountType = ''LOAN'' AND a.HoldToMaturityFlag = 1 THEN ''Amortized Cost''
							WHEN a.AccountType = ''BOND'' AND a.HoldToMaturityFlag = 1 THEN ''Amortized Cost''
							WHEN a.AccountType = ''BOND'' AND a.HoldToCollectAndSellFlag = 1 THEN ''FVOCI''
							WHEN a.AccountType = ''EQUITY'' THEN ''FVTPL''
							WHEN a.AccountType = ''DERIVATIVE'' THEN ''FVTPL''
							WHEN a.TradingFlag = 1 THEN ''FVTPL''
							ELSE ''Amortized Cost'' -- Default classification for standard loans
						END AS ClassificationCategory,
						CASE
							WHEN a.HoldToMaturityFlag = 1 THEN ''Hold to Collect''
							WHEN a.HoldToCollectAndSellFlag = 1 THEN ''Hold to Collect and Sell''
							WHEN a.TradingFlag = 1 THEN ''Other''
							ELSE ''Hold to Collect'' -- Default business model for standard loans
						END AS BusinessModelAssessment,
						CASE
							-- SPPI (Solely Payments of Principal and Interest) test
							WHEN a.AccountType IN (''LOAN'', ''BOND'') AND a.HasNonSPPIFeatures = 0 THEN ''SPPI''
							WHEN a.AccountType IN (''EQUITY'', ''DERIVATIVE'') THEN ''Non-SPPI''
							WHEN a.HasNonSPPIFeatures = 1 THEN ''Non-SPPI''
							ELSE ''SPPI'' -- Default for standard loans
						END AS CashflowCharacteristics,
						@AsOfDate AS ClassificationDate,
						''Initial classification or reclassification'' AS JustificationText
					FROM Production.DimAccount a
					WHERE a.IsActive = 1
					AND a.AccountClosedDate IS NULL
					AND (
						NOT EXISTS (
							SELECT 1 
							FROM IFRS9.FinancialInstrumentClassification c
							WHERE c.CustomerSK = a.CustomerSK
							AND c.AccountSK = a.AccountSK
						)
						OR @ForceReclass = 1
					)
				) AS source
				ON (
					target.CustomerSK = source.CustomerSK AND
					target.AccountSK = source.AccountSK
				)
				WHEN MATCHED THEN
					UPDATE SET
						target.InstrumentTypeCode = source.InstrumentTypeCode,
						target.ClassificationCategory = source.ClassificationCategory,
						target.BusinessModelAssessment = source.BusinessModelAssessment,
						target.CashflowCharacteristics = source.CashflowCharacteristics,
						target.LastReassessmentDate = @AsOfDate,
						target.RecordedDate = GETDATE()
				WHEN NOT MATCHED THEN
					INSERT (
						CustomerSK, AccountSK, InstrumentTypeCode, ClassificationCategory,
						BusinessModelAssessment, CashflowCharacteristics, ClassificationDate,
						JustificationText
					)
					VALUES (
						source.CustomerSK, source.AccountSK, source.InstrumentTypeCode,
						source.ClassificationCategory, source.BusinessModelAssessment,
						source.CashflowCharacteristics, source.ClassificationDate,
						source.JustificationText
					);';
				
				-- Execute the dynamic SQL with parameters
				EXEC sp_executesql @DynamicSQL, 
					N'@AsOfDate DATE, @ForceReclass BIT', 
					@AsOfDate, @ForceReclassification;
			END
			ELSE
			BEGIN
				-- Use alternative logic with only basic columns
				INSERT INTO IFRS9.FinancialInstrumentClassification (
					CustomerSK, AccountSK, InstrumentTypeCode, ClassificationCategory,
					BusinessModelAssessment, CashflowCharacteristics, ClassificationDate,
					JustificationText
				)
				SELECT
					a.CustomerSK,
					a.AccountSK,
					a.AccountType AS InstrumentTypeCode,
					CASE
						WHEN a.AccountType IN ('LOAN', 'MORTGAGE') THEN 'Amortized Cost'
						WHEN a.AccountType = 'BOND' THEN 'FVOCI'
						ELSE 'FVTPL'
					END AS ClassificationCategory,
					CASE
						WHEN a.AccountType IN ('LOAN', 'MORTGAGE', 'BOND') THEN 'Hold to Collect'
						ELSE 'Other'
					END AS BusinessModelAssessment,
					CASE
						WHEN a.AccountType IN ('LOAN', 'MORTGAGE', 'BOND') THEN 'SPPI'
						ELSE 'Non-SPPI'
					END AS CashflowCharacteristics,
					@AsOfDate AS ClassificationDate,
					'Initial default classification' AS JustificationText
				FROM Production.DimAccount a
				WHERE NOT EXISTS (
					SELECT 1 
					FROM IFRS9.FinancialInstrumentClassification c
					WHERE c.CustomerSK = a.CustomerSK
					AND c.AccountSK = a.AccountSK
				)
				OR @ForceReclassification = 1;
			END
		END
		ELSE
		BEGIN
			-- In test mode, just print a message
			PRINT 'Test mode enabled - no data modifications performed';
		END
		
		-- Log successful completion
		SET @LogMessage = 'Financial instrument classification completed successfully';
		
		-- Use appropriate logging method based on available columns
		IF EXISTS (SELECT 1 FROM sys.columns 
				WHERE object_id = OBJECT_ID('Production.ETLLog') 
				AND name IN ('BatchID', 'ProcedureName', 'StepName', 'Status', 'Message'))
		BEGIN
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_ClassifyFinancialInstruments', 'Complete', 'Success', @LogMessage);
		END
		ELSE
		BEGIN
			BEGIN TRY
				INSERT INTO Production.ETLLog (ExecutionDateTime, Status)
				VALUES (GETDATE(), 'Completed IFRS9 classification');
			END TRY
			BEGIN CATCH
				-- Silently continue if logging fails
			END CATCH
		END
		
		RETURN 0;
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
		DECLARE @ErrorState INT = ERROR_STATE();
		
		-- Log error with appropriate method
		IF EXISTS (SELECT 1 FROM sys.columns 
				WHERE object_id = OBJECT_ID('Production.ETLLog') 
				AND name IN ('BatchID', 'ProcedureName', 'StepName', 'Status', 'Message'))
		BEGIN
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (@BatchID, 'usp_ClassifyFinancialInstruments', 'Error', 'Failed', @ErrorMessage);
		END
		ELSE
		BEGIN
			BEGIN TRY
				INSERT INTO Production.ETLLog (ExecutionDateTime, Status)
				VALUES (GETDATE(), 'Failed IFRS9 classification: ' + LEFT(@ErrorMessage, 500));
			END TRY
			BEGIN CATCH
				-- Silently continue if logging fails
			END CATCH
		END
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
		RETURN -1;
	END CATCH;
END;
GO