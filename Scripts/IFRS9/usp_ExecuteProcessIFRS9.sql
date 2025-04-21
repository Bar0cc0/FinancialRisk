CREATE OR ALTER PROCEDURE IFRS9.usp_ExecuteProcessIFRS9
	@AsOfDate DATE = NULL,
	@PreviousDate DATE = NULL,
	@BatchID UNIQUEIDENTIFIER = NULL,
	@IncludeAllScenarios BIT = 1,
	@CalculateMovement BIT = 1
AS
BEGIN
	SET NOCOUNT ON;
	
	IF @AsOfDate IS NULL
		SET @AsOfDate = CAST(GETDATE() AS DATE);
		
	IF @BatchID IS NULL
		SET @BatchID = NEWID();
	
	-- Find previous processing date if not specified
	IF @PreviousDate IS NULL AND @CalculateMovement = 1
	BEGIN
		SELECT TOP 1 @PreviousDate = AsOfDate
		FROM IFRS9.ECLCalculation
		WHERE AsOfDate < @AsOfDate
		ORDER BY AsOfDate DESC;
		
		IF @PreviousDate IS NULL
		BEGIN
			-- If no previous date found, disable movement calculation
			SET @CalculateMovement = 0;
		END
	END
	
	DECLARE @LogMessage NVARCHAR(MAX);
	SET @LogMessage = 'Starting IFRS9 processing for date: ' + CAST(@AsOfDate AS VARCHAR);
	
	-- Log procedure start
	INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
	VALUES (@BatchID, 'usp_ProcessIFRS9', 'Start', 'Processing', @LogMessage);
	
	BEGIN TRY
		-- Step 1: Classify financial instruments
		EXEC IFRS9.usp_ClassifyFinancialInstruments
			@AsOfDate = @AsOfDate,
			@BatchID = @BatchID;
		
		-- Step 2: Determine ECL staging
		EXEC IFRS9.usp_ClassifyECLStaging
			@AsOfDate = @AsOfDate,
			@BatchID = @BatchID;
		
		-- Step 3: Calculate ECL parameters
		EXEC IFRS9.usp_CalculateECLParameters
			@AsOfDate = @AsOfDate,
			@BatchID = @BatchID;
		
		-- Step 4: Calculate ECL
		EXEC IFRS9.usp_CalculateECL
			@AsOfDate = @AsOfDate,
			@BatchID = @BatchID,
			@IncludeAllScenarios = @IncludeAllScenarios;
		
		-- Step 5: Calculate ECL movement (if applicable)
		IF @CalculateMovement = 1 AND @PreviousDate IS NOT NULL
		BEGIN
			EXEC IFRS9.usp_CalculateECLMovement
				@FromDate = @PreviousDate,
				@ToDate = @AsOfDate,
				@BatchID = @BatchID;
		END
		
		-- Step 6: Refresh IFRS9 reporting views
		EXEC IFRS9.usp_RefreshReportingViews;
		
		-- Log successful completion
		SET @LogMessage = 'IFRS9 processing completed successfully for date: ' + CAST(@AsOfDate AS VARCHAR);
		
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_ProcessIFRS9', 'Complete', 'Success', @LogMessage);
		
		RETURN 0;
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
		DECLARE @ErrorState INT = ERROR_STATE();
		
		-- Log error
		INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
		VALUES (@BatchID, 'usp_ProcessIFRS9', 'Error', 'Failed', @ErrorMessage);
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
		RETURN -1;
	END CATCH;
END;
GO
