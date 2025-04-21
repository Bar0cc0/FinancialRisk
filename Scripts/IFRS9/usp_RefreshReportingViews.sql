CREATE OR ALTER PROCEDURE IFRS9.usp_RefreshReportingViews
AS
BEGIN
	SET NOCOUNT ON;
	
	-- View 1: IFRS 9 Financial Instrument Classification Summary
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vFinancialInstrumentClassification')
		DROP VIEW IFRS9.vFinancialInstrumentClassification;
	
	EXEC('
	CREATE VIEW IFRS9.vFinancialInstrumentClassification AS
	SELECT 
		c.CustomerID,
		c.CustomerName,
		a.AccountNumber,
		a.AccountType,
		fic.ClassificationCategory,
		fic.BusinessModelAssessment,
		fic.CashflowCharacteristics,
		fic.ClassificationDate,
		fic.LastReassessmentDate
	FROM IFRS9.FinancialInstrumentClassification fic
	JOIN Production.DimCustomer c ON fic.CustomerSK = c.CustomerSK
	JOIN Production.DimAccount a ON fic.AccountSK = a.AccountSK;
	');
	
	-- View 2: IFRS9 ECL Staging Summary
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vECLStagingSummary')
		DROP VIEW IFRS9.vECLStagingSummary;
	
	EXEC('
	CREATE VIEW IFRS9.vECLStagingSummary AS
	SELECT 
		d.FullDate AS ReportDate,
		s.ECLStage,
		COUNT(*) AS AccountCount,
		SUM(l.CurrentBalance) AS GrossCarryingAmount,
		SUM(ec.AppliedECL) AS TotalECL,
		SUM(ec.AppliedECL) / NULLIF(SUM(l.CurrentBalance), 0) * 100 AS ECLRatio,
		SUM(CASE WHEN s.StageChangeDate = s.AsOfDate THEN 1 ELSE 0 END) AS StageChangesCount
	FROM IFRS9.ECLStaging s
	JOIN Production.DimDate d ON s.AsOfDate = d.FullDate
	JOIN Production.FactLoan l ON s.CustomerSK = l.CustomerSK AND s.AccountSK = l.AccountSK
	JOIN IFRS9.ECLCalculation ec ON 
		s.CustomerSK = ec.CustomerSK AND 
		s.AccountSK = ec.AccountSK AND
		s.AsOfDate = ec.AsOfDate
	GROUP BY d.FullDate, s.ECLStage;
	');
	
	
	-- View 3: ECL Calculation Detail
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vECLDetail')
		DROP VIEW IFRS9.vECLDetail;
	
	EXEC('
	CREATE VIEW IFRS9.vECLDetail AS
	SELECT 
		d.FullDate AS ReportDate,
		c.CustomerID,
		c.CustomerName,
		a.AccountNumber,
		a.AccountType,
		ec.ECLStage,
		ec.GrossCarryingAmount,
		ec.AppliedECL,
		ec.NetCarryingAmount,
		ec.PD_Applied,
		ec.LGD_Applied,
		ec.EAD_Applied,
		ec.DiscountFactor,
		l.CurrentBalance,
		l.InterestRate,
		l.MaturityDate,
		l.DaysPastDue,
		sc.ScenarioName,
		sc.ScenarioWeight
	FROM IFRS9.ECLCalculation ec
	JOIN Production.DimDate d ON ec.DateID = d.DateID
	JOIN Production.DimCustomer c ON ec.CustomerSK = c.CustomerSK
	JOIN Production.DimAccount a ON ec.AccountSK = a.AccountSK
	JOIN Production.FactLoan l ON ec.CustomerSK = l.CustomerSK AND ec.AccountSK = l.AccountSK
	JOIN IFRS9.MacroeconomicScenario sc ON ec.ScenarioSK = sc.ScenarioSK
	');
	
	-- View 4: Macroeconomic Scenario Impact Analysis
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vMacroeconomicImpact')
		DROP VIEW IFRS9.vMacroeconomicImpact;
	
	EXEC('
	CREATE VIEW IFRS9.vMacroeconomicImpact AS
	SELECT 
		d.FullDate AS ReportDate,
		sc.ScenarioName,
		sc.ScenarioWeight,
		me.GDPGrowth,
		me.UnemploymentRate,
		me.HousePriceIndex,
		me.InterestRate,
		COUNT(DISTINCT ec.AccountSK) AS AccountCount,
		SUM(ec.GrossCarryingAmount) AS TotalExposure,
		SUM(ec.AppliedECL) AS TotalECL,
		SUM(ec.AppliedECL) / NULLIF(SUM(ec.GrossCarryingAmount), 0) * 100 AS ECLRatio
	FROM IFRS9.ECLCalculation ec
	JOIN IFRS9.MacroeconomicScenario sc ON ec.ScenarioSK = sc.ScenarioSK
	JOIN IFRS9.MacroeconomicIndicators me ON sc.ScenarioSK = me.ScenarioSK AND ec.DateID = me.DateID
	JOIN Production.DimDate d ON ec.DateID = d.DateID
	GROUP BY d.FullDate, sc.ScenarioName, sc.ScenarioWeight, me.GDPGrowth, me.UnemploymentRate, me.HousePriceIndex, me.InterestRate
	');

	-- View 5: Significant Increase in Credit Risk (SICR) Monitoring
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vSICRMonitoring')
		DROP VIEW IFRS9.vSICRMonitoring;
	
	EXEC('
	CREATE VIEW IFRS9.vSICRMonitoring AS
	SELECT 
		d.FullDate AS ReportDate,
		c.CustomerID,
		c.CustomerName,
		a.AccountNumber,
		a.AccountType,
		current.ECLStage AS CurrentStage,
		previous.ECLStage AS PreviousStage,
		current.SICRIndicators,
		current.SICROverrideReason,
		current.SICROverrideBy,
		current.PD_Applied AS CurrentPD,
		previous.PD_Applied AS PreviousPD,
		(current.PD_Applied - previous.PD_Applied) / NULLIF(previous.PD_Applied, 0) * 100 AS PD_PercentChange,
		l.DaysPastDue,
		l.CurrentBalance AS ExposureAmount
	FROM IFRS9.ECLStaging current
	JOIN Production.DimDate d ON current.AsOfDate = d.FullDate
	JOIN Production.DimCustomer c ON current.CustomerSK = c.CustomerSK
	JOIN Production.DimAccount a ON current.AccountSK = a.AccountSK
	JOIN Production.FactLoan l ON current.CustomerSK = l.CustomerSK AND current.AccountSK = l.AccountSK
	LEFT JOIN IFRS9.ECLStaging previous ON 
		current.CustomerSK = previous.CustomerSK AND 
		current.AccountSK = previous.AccountSK AND
		previous.AsOfDate = (
			SELECT MAX(AsOfDate) 
			FROM IFRS9.ECLStaging 
			WHERE CustomerSK = current.CustomerSK 
			AND AccountSK = current.AccountSK 
			AND AsOfDate < current.AsOfDate
		)
	WHERE (current.ECLStage <> previous.ECLStage OR previous.ECLStage IS NULL)
	');
	
	-- View 6: Financial Instrument Classification Changes
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vClassificationChanges')
		DROP VIEW IFRS9.vClassificationChanges;
	
	EXEC('
	CREATE VIEW IFRS9.vClassificationChanges AS
	SELECT 
		c.CustomerID,
		c.CustomerName,
		a.AccountNumber,
		a.AccountType,
		current.ClassificationCategory AS CurrentClassification,
		previous.ClassificationCategory AS PreviousClassification,
		current.ClassificationDate,
		current.BusinessModelAssessment,
		current.CashflowCharacteristics,
		current.ClassificationRationale,
		current.ReassessmentTrigger
	FROM IFRS9.FinancialInstrumentClassification current
	JOIN Production.DimCustomer c ON current.CustomerSK = c.CustomerSK
	JOIN Production.DimAccount a ON current.AccountSK = a.AccountSK
	LEFT JOIN IFRS9.FinancialInstrumentClassificationHistory previous ON 
		current.CustomerSK = previous.CustomerSK AND
		current.AccountSK = previous.AccountSK AND
		previous.EffectiveEndDate = current.ClassificationDate
	WHERE previous.ClassificationCategory <> current.ClassificationCategory OR previous.ClassificationCategory IS NULL
	');

	-- View 7: Lifetime ECL Comparison (IFRS 9 vs IAS 39)
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vIFRS9vsIAS39Comparison')
		DROP VIEW IFRS9.vIFRS9vsIAS39Comparison;
	
	EXEC('
	CREATE VIEW IFRS9.vIFRS9vsIAS39Comparison AS
	SELECT
		d.FullDate AS ReportDate,
		a.AccountType,
		a.ProductCategory,
		COUNT(DISTINCT ec.AccountSK) AS AccountCount,
		SUM(ec.GrossCarryingAmount) AS TotalExposure,
		SUM(ec.AppliedECL) AS IFRS9_ECL,
		SUM(ia.HistoricalProvision) AS IAS39_Provision,
		SUM(ec.AppliedECL - ia.HistoricalProvision) AS ProvisionDifference,
		SUM(ec.AppliedECL) / NULLIF(SUM(ia.HistoricalProvision), 0) * 100 - 100 AS PercentageChange
	FROM IFRS9.ECLCalculation ec
	JOIN IFRS9.IAS39Comparison ia ON ec.CustomerSK = ia.CustomerSK AND ec.AccountSK = ia.AccountSK AND ec.DateID = ia.DateID
	JOIN Production.DimDate d ON ec.DateID = d.DateID
	JOIN Production.DimAccount a ON ec.AccountSK = a.AccountSK
	GROUP BY d.FullDate, a.AccountType, a.ProductCategory
	');

	-- View 8: Modification and Derecognition Tracking
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vModificationTracking')
		DROP VIEW IFRS9.vModificationTracking;
	
	EXEC('
	CREATE VIEW IFRS9.vModificationTracking AS
	SELECT
		d.FullDate AS ModificationDate,
		c.CustomerID,
		c.CustomerName,
		a.AccountNumber,
		a.AccountType,
		m.ModificationType,
		m.OriginalGrossCarryingAmount,
		m.ModifiedGrossCarryingAmount,
		m.ModificationGainLoss,
		m.IsSubstantial,
		m.DerecognitionReason,
		m.NewAccountSK,
		m.ModificationRationale,
		m.ApprovedBy
	FROM IFRS9.ModificationTracking m
	JOIN Production.DimDate d ON m.ModificationDateID = d.DateID
	JOIN Production.DimCustomer c ON m.CustomerSK = c.CustomerSK
	JOIN Production.DimAccount a ON m.AccountSK = a.AccountSK
	');

	-- View 9: ECL Staging Summary
		-- ECL Summary view
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'vECLSummary')
		DROP VIEW IFRS9.vECLSummary;
	
	EXEC('
	CREATE VIEW IFRS9.vECLSummary AS
	SELECT 
		ecl.AsOfDate,
		d.Year,
		d.Quarter,
		d.MonthName,
		ecl.ECLStage,
		ms.ScenarioName,
		COUNT(DISTINCT ecl.AccountSK) AS AccountCount,
		SUM(ecl.GrossCarryingAmount) AS TotalExposure,
		SUM(ecl.AppliedECL) AS TotalECL,
		SUM(ecl.AppliedECL) / NULLIF(SUM(ecl.GrossCarryingAmount), 0) * 100 AS ECLCoverageRatio,
		AVG(ecl.PD_Applied) * 100 AS AvgPD,
		AVG(ecl.LGD_Applied) * 100 AS AvgLGD
	FROM IFRS9.ECLCalculation ecl
	JOIN Production.DimDate d ON ecl.DateID = d.DateID
	JOIN IFRS9.MacroeconomicScenarios ms ON ecl.ScenarioID = ms.ScenarioID
	GROUP BY 
		ecl.AsOfDate,
		d.Year,
		d.Quarter,
		d.MonthName,
		ecl.ECLStage,
		ms.ScenarioName
	');

	-- Log the refresh with error handling for missing columns
	BEGIN TRY
		-- Check what columns exist in ETLLog
		IF EXISTS (SELECT 1 FROM sys.columns 
				WHERE object_id = OBJECT_ID('Production.ETLLog') 
				AND name IN ('BatchID', 'ProcedureName', 'StepName', 'Status', 'Message'))
		BEGIN
			-- Use standard column format if all expected columns exist
			INSERT INTO Production.ETLLog (BatchID, ProcedureName, StepName, Status, Message)
			VALUES (NEWID(), 'IFRS9.usp_RefreshReportingViews', 'Complete', 'Success', 
					'All IFRS9 reporting views refreshed at ' + CONVERT(VARCHAR, GETDATE(), 120));
		END
		ELSE IF EXISTS (SELECT 1 FROM sys.columns 
						WHERE object_id = OBJECT_ID('Production.ETLLog') 
						AND name IN ('ProcedureName', 'Status', 'Message'))
		BEGIN
			-- Alternative format if BatchID/StepName are missing
			INSERT INTO Production.ETLLog (ProcedureName, Status, Message)
			VALUES ('IFRS9.usp_RefreshReportingViews', 'Success', 
					'All IFRS9 reporting views refreshed at ' + CONVERT(VARCHAR, GETDATE(), 120));
		END
		ELSE IF EXISTS (SELECT 1 FROM sys.columns 
						WHERE object_id = OBJECT_ID('Production.ETLLog') 
						AND name = 'ExecutionDateTime')
		BEGIN
			-- Minimal logging if only basic columns exist
			INSERT INTO Production.ETLLog (ExecutionDateTime, Status)
			VALUES (GETDATE(), 'IFRS9.usp_RefreshReportingViews completed successfully');
		END
	END TRY
	BEGIN CATCH
		-- Silently continue if logging fails
	END CATCH
END;
GO
