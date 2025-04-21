/* 
Create a procedure for generating refresh data for interactive reports
*/

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE Production.usp_RefreshReportViews
AS
BEGIN
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @ViewName NVARCHAR(128);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ViewCount INT = 0;
    
    -- Create a log entry for the refresh operation
    INSERT INTO Production.ETLLog (ProcedureName, Status)
    VALUES ('Report Views', 'Refresh Started');
    
    -- 1. Customer Risk Analysis View
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vCustomerRiskAnalysis' AND schema_id = SCHEMA_ID('Production'))
    BEGIN
        DROP VIEW Production.vCustomerRiskAnalysis;
    END
    
    EXEC('CREATE VIEW Production.vCustomerRiskAnalysis WITH SCHEMABINDING AS
        SELECT 
            c.CustomerSK,
            c.CustomerID,
            c.Name,
            c.Age,
            c.EmploymentStatus,
            c.JobTitle,
            c.HomeOwnershipStatus,
            c.MaritalStatus,
            c.PreviousLoanDefault,
            SUM(fc.AnnualIncome) AS AnnualIncome,
            SUM(fc.TotalAssets) AS TotalAssets,
            SUM(fc.TotalLiabilities) AS TotalLiabilities,
            SUM(fc.TotalLiabilities) / NULLIF(SUM(fc.TotalAssets), 0) AS LeverageRatio,
            SUM(fc.TotalLiabilities) / NULLIF(SUM(fc.AnnualIncome), 0) AS DebtToIncomeRatio,
            COUNT(DISTINCT fl.LoanID) AS ActiveLoansCount,
            SUM(fl.LoanAmount) AS TotalLoanAmount,
            SUM(fl.Balance) AS TotalOutstandingBalance,
            SUM(CASE WHEN fl.PaymentStatus = ''Late'' THEN 1 ELSE 0 END) AS LatePaymentsCount,
            SUM(fl.DelayedPayments) AS TotalDelayedPayments
        FROM Production.DimCustomer c
        LEFT JOIN Production.FactCustomer fc ON c.CustomerSK = fc.CustomerSK
        LEFT JOIN Production.FactLoan fl ON c.CustomerSK = fl.CustomerSK
        WHERE c.IsCurrent = 1
        GROUP BY 
            c.CustomerSK,
            c.CustomerID,
            c.Name,
            c.Age,
            c.EmploymentStatus,
            c.JobTitle,
            c.HomeOwnershipStatus,
            c.MaritalStatus,
            c.PreviousLoanDefault');
    
    SET @ViewCount = @ViewCount + 1;
    
    -- Create indexed view if supported
    BEGIN TRY
        EXEC('CREATE UNIQUE CLUSTERED INDEX IDX_vCustomerRiskAnalysis 
              ON Production.vCustomerRiskAnalysis(CustomerSK)');
    END TRY
    BEGIN CATCH
        INSERT INTO Production.ETLLog (ProcedureName, Status, ErrorMessage)
        VALUES ('vCustomerRiskAnalysis Index', 'Failed', ERROR_MESSAGE());
    END CATCH
    
    -- 2. Loan Performance Analysis View
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vLoanPerformance' AND schema_id = SCHEMA_ID('Production'))
    BEGIN
        DROP VIEW Production.vLoanPerformance;
    END
    
    EXEC('CREATE VIEW Production.vLoanPerformance AS
        SELECT 
            fl.LoanID,
            dd.Year AS LoanYear,
            dd.Quarter AS LoanQuarter,
            dd.Month AS LoanMonth,
            dt.LoanType,
            fl.LoanAmount,
            fl.InterestRate,
            fl.LoanDurationMonths,
            fl.Balance,
            fl.DelayedPayments,
            fl.PaymentStatus,
            fl.LoanToValueRatio,
            dc.CustomerID,
            dc.Name AS CustomerName,
            dc.EmploymentStatus,
            dc.HomeOwnershipStatus,
            fc.AnnualIncome,
            fc.DebtToIncomeRatio,
            CASE 
                WHEN fl.PaymentStatus = ''Current'' THEN ''Good Standing''
                WHEN fl.PaymentStatus = ''Late'' AND fl.DelayedPayments <= 3 THEN ''Moderate Risk''
                WHEN fl.PaymentStatus = ''Late'' AND fl.DelayedPayments > 3 THEN ''High Risk''
                WHEN fl.PaymentStatus = ''Default'' THEN ''Default''
                ELSE ''Other''
            END AS RiskCategory
        FROM Production.FactLoan fl
        JOIN Production.DimDate dd ON fl.LoanDateID = dd.DateID
        JOIN Production.DimLoanType dt ON fl.LoanTypeID = dt.LoanTypeID
        JOIN Production.DimCustomer dc ON fl.CustomerSK = dc.CustomerSK
        LEFT JOIN Production.FactCustomer fc ON dc.CustomerSK = fc.CustomerSK');
    
    SET @ViewCount = @ViewCount + 1;
    
    -- 3. Fraud Detection Analysis View
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vFraudAnalysis' AND schema_id = SCHEMA_ID('Production'))
    BEGIN
        DROP VIEW Production.vFraudAnalysis;
    END
    
    EXEC('CREATE VIEW Production.vFraudAnalysis AS
        SELECT 
            ff.FraudID,
            dd.FullDate AS DetectionDate,
            dd.Year AS DetectionYear,
            dd.Quarter AS DetectionQuarter,
            dd.Month AS DetectionMonth,
            ff.FraudType,
            ff.DetectionMethod,
            ff.FraudAmount,
            ff.RecoveredAmount,
            ff.FraudAmount - ff.RecoveredAmount AS LossAmount,
            ff.InvestigationStatus,
            ff.Resolution,
            dc.CustomerID,
            dc.Name AS CustomerName,
            dc.Age,
            dc.EmploymentStatus,
            dc.HomeOwnershipStatus,
            fl.LoanAmount,
            fl.LoanDurationMonths,
            fl.InterestRate,
            dt.LoanType
        FROM Production.FactFraud ff
        JOIN Production.DimDate dd ON ff.DetectionDateID = dd.DateID
        JOIN Production.DimCustomer dc ON ff.CustomerSK = dc.CustomerSK
        LEFT JOIN Production.FactLoan fl ON ff.LoanSK = fl.LoanID
        LEFT JOIN Production.DimLoanType dt ON fl.LoanTypeID = dt.LoanTypeID');
    
    SET @ViewCount = @ViewCount + 1;
    
    -- 4. Market Analysis View
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vMarketAnalysis' AND schema_id = SCHEMA_ID('Production'))
    BEGIN
        DROP VIEW Production.vMarketAnalysis;
    END
    
    EXEC('CREATE VIEW Production.vMarketAnalysis AS
        SELECT 
            dd.FullDate AS MarketDate,
            dd.Year,
            dd.Quarter,
            dd.Month,
            fm.Region,
            fm.MarketSegment,
            fm.MarketValue,
            fm.Volume,
            fm.ChangePercent,
            fm.Volatility,
            fma.UnemploymentRate,
            fma.GDPGrowth,
            fma.InflationRate,
            fma.InterestRate,
            fma.ConsumerConfidenceIndex
        FROM Production.FactMarket fm
        JOIN Production.DimDate dd ON fm.DateID = dd.DateID
        LEFT JOIN Production.FactMacro fma ON fm.DateID = fma.DateID AND fma.Country = 
            CASE 
                WHEN fm.Region = ''North America'' THEN ''USA''
                WHEN fm.Region = ''Europe'' THEN ''EU''
                WHEN fm.Region = ''Asia'' THEN ''China''
                ELSE ''Global''
            END');
    
    SET @ViewCount = @ViewCount + 1;
    
    -- 5. Financial Risk Dashboard View
    IF EXISTS (SELECT * FROM sys.views WHERE name = 'vRiskDashboard' AND schema_id = SCHEMA_ID('Production'))
    BEGIN
        DROP VIEW Production.vRiskDashboard;
    END
    
    EXEC('CREATE VIEW Production.vRiskDashboard AS
        SELECT 
            dd.Year,
            dd.Quarter,
            dd.Month,
            dt.LoanType,
            COUNT(DISTINCT fl.LoanID) AS TotalLoans,
            SUM(fl.LoanAmount) AS TotalLoanAmount,
            SUM(fl.Balance) AS TotalOutstandingBalance,
            SUM(CASE WHEN fl.PaymentStatus = ''Late'' THEN 1 ELSE 0 END) AS LatePaymentsCount,
            SUM(CASE WHEN fl.PaymentStatus = ''Default'' THEN 1 ELSE 0 END) AS DefaultsCount,
            SUM(CASE WHEN fl.PaymentStatus = ''Default'' THEN fl.Balance ELSE 0 END) AS DefaultedBalance,
            COUNT(DISTINCT ff.FraudID) AS FraudCases,
            SUM(ISNULL(ff.FraudAmount, 0)) AS TotalFraudAmount,
            AVG(fma.UnemploymentRate) AS AvgUnemploymentRate,
            AVG(fma.InterestRate) AS AvgInterestRate,
            AVG(fma.InflationRate) AS AvgInflationRate
        FROM Production.FactLoan fl
        JOIN Production.DimDate dd ON fl.LoanDateID = dd.DateID
        JOIN Production.DimLoanType dt ON fl.LoanTypeID = dt.LoanTypeID
        LEFT JOIN Production.FactFraud ff ON fl.LoanID = ff.LoanSK
        LEFT JOIN Production.FactMacro fma ON dd.DateID = fma.DateID AND fma.Country = ''USA''
        GROUP BY 
            dd.Year,
            dd.Quarter,
            dd.Month,
            dt.LoanType');
    
    SET @ViewCount = @ViewCount + 1;
    
    -- Update statistics for the views
    DECLARE view_cursor CURSOR FOR
        SELECT name FROM sys.views 
        WHERE schema_id = SCHEMA_ID('Production') 
        AND name LIKE 'v%';
    
    OPEN view_cursor;
    FETCH NEXT FROM view_cursor INTO @ViewName;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SQL = N'UPDATE STATISTICS Production.[' + @ViewName + ']';
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            
            INSERT INTO Production.ETLLog (ProcedureName, Status)
            VALUES ('View Statistics: ' + @ViewName, 'Updated');
        END TRY
        BEGIN CATCH
            INSERT INTO Production.ETLLog (ProcedureName, Status, ErrorMessage)
            VALUES ('View Statistics: ' + @ViewName, 'Failed', ERROR_MESSAGE());
        END CATCH
        
        FETCH NEXT FROM view_cursor INTO @ViewName;
    END
    
    CLOSE view_cursor;
    DEALLOCATE view_cursor;
    
    -- Update last refresh timestamp in configuration
    DECLARE @TimestampValue VARCHAR(30);
    SET @TimestampValue = CONVERT(VARCHAR(30), GETDATE(), 121);

    EXEC Production.usp_SetConfigValue
        @ConfigKey = 'LastReportRefresh',
        @ConfigValue = @TimestampValue,
        @Description = 'Last time reporting views were refreshed';
    
    -- Log completion
    INSERT INTO Production.ETLLog 
        (ProcedureName, Status, RowsProcessed, ExecutionTimeSeconds)
    VALUES 
        ('Report Views', 'Refreshed', @ViewCount, 
        DATEDIFF(SECOND, @StartTime, GETDATE()));
    
    -- Return refresh timestamp
    SELECT GETDATE() AS RefreshTimestamp;
END;
GO

-- Create a job helper procedure to run the report refresh on a schedule
CREATE OR ALTER PROCEDURE Production.usp_ScheduleReportRefresh
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Execute the refresh procedure
    EXEC Production.usp_RefreshReportViews;
    
    -- Return success message
    SELECT 'Report views refreshed successfully' AS Status;
END;
GO