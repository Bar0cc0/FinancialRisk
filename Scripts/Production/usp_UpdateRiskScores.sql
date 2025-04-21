/* 
This script creates functions and procedures for calculating risk scores and profiles
based on customer, market, and fraud data.
Last updated: 2025-01-01
*/


-- Credit Risk Score calculation function
CREATE OR ALTER FUNCTION Production.CalculateCreditRiskScore(
    @CustomerSK INT,
    @DateID INT
)
RETURNS DECIMAL(5,2)
AS
BEGIN
	DECLARE @StartTime DATETIME = GETDATE();

    DECLARE @CreditRiskScore DECIMAL(5,2);
    
    SELECT @CreditRiskScore = 
        -- Base score (range 0-100, higher is riskier)
        (100 - (fc.CreditScore * 0.1)) * 0.30 +  -- Credit score component (30%)
        
        -- Debt factors (range 0-100)
        (CASE 
            WHEN fcu.DebtToIncomeRatio > 0.43 THEN 100
            WHEN fcu.DebtToIncomeRatio > 0.36 THEN 80
            WHEN fcu.DebtToIncomeRatio > 0.28 THEN 50
            ELSE 20
        END) * 0.25 +  -- DTI component (25%)
        
        -- Payment history (range 0-100)
        (CASE
            WHEN SUM(CASE WHEN fl.PaymentStatus = 'Defaulted' THEN 1 ELSE 0 END) > 0 THEN 100
            WHEN SUM(CASE WHEN fl.PaymentStatus = 'Late' THEN 1 ELSE 0 END) > 3 THEN 90
            WHEN SUM(CASE WHEN fl.PaymentStatus = 'Late' THEN 1 ELSE 0 END) > 0 THEN 70
            ELSE 10
        END) * 0.30 +  -- Payment history component (30%)
        
        -- Credit utilization (range 0-100)
        (CASE
            WHEN fc.CreditUtilizationRatio > 0.9 THEN 100
            WHEN fc.CreditUtilizationRatio > 0.7 THEN 80
            WHEN fc.CreditUtilizationRatio > 0.5 THEN 50
            WHEN fc.CreditUtilizationRatio > 0.3 THEN 30
            ELSE 10
        END) * 0.15  -- Credit utilization component (15%)
    FROM 
        Production.FactCredit fc
        JOIN Production.FactCustomer fcu ON fc.CustomerSK = fcu.CustomerSK
        LEFT JOIN Production.FactLoan fl ON fc.CustomerSK = fl.CustomerSK
    WHERE 
        fc.CustomerSK = @CustomerSK
        AND fcu.DateSK = @DateID
    GROUP BY
        fc.CreditScore,
        fc.CreditUtilizationRatio,
        fcu.DebtToIncomeRatio;
    
    RETURN ISNULL(@CreditRiskScore, 50.00);
END;
GO


-- Fraud Risk Score calculation function
CREATE OR ALTER FUNCTION Production.CalculateFraudRiskScore(
    @CustomerSK INT,
    @DateID INT
)
RETURNS DECIMAL(5,2)
AS
BEGIN
    DECLARE @FraudRiskScore DECIMAL(5,2);
    DECLARE @ReferenceDate DATE;
    DECLARE @HistoricDateID INT;
    
    -- Get the actual date for the DateID
    SELECT @ReferenceDate = FullDate 
    FROM Production.DimDate 
    WHERE DateID = @DateID;
    
    -- Find the DateID for 90 days ago
    SELECT @HistoricDateID = DateID
    FROM Production.DimDate
    WHERE FullDate = DATEADD(day, -90, @ReferenceDate);
    
    SELECT @FraudRiskScore = 
        -- Prior fraud incidents (range 0-100)
        (CASE 
            WHEN COUNT(DISTINCT CASE WHEN ff.IsFraudulent = 1 THEN ff.FraudID ELSE NULL END) > 3 THEN 100
            WHEN COUNT(DISTINCT CASE WHEN ff.IsFraudulent = 1 THEN ff.FraudID ELSE NULL END) > 0 THEN 80
            ELSE 20
        END) * 0.40 +  -- Prior fraud component (40%)
        
        -- Unusual transaction patterns (range 0-100)
        (CASE 
            WHEN AVG(ff.DistanceFromHome) > 1000 THEN 80
            WHEN AVG(ff.DistanceFromHome) > 500 THEN 60
            WHEN AVG(ff.DistanceFromHome) > 100 THEN 30
            ELSE 10
        END) * 0.20 +  -- Distance component (20%)
        
        -- Transaction amount deviation (range 0-100)
        (CASE
            WHEN AVG(ff.RatioToMedianTransactionAmount) > 5 THEN 80
            WHEN AVG(ff.RatioToMedianTransactionAmount) > 3 THEN 60
            WHEN AVG(ff.RatioToMedianTransactionAmount) > 2 THEN 40
            ELSE 20
        END) * 0.25 +  -- Amount deviation component (25%)
        
        -- Chip/PIN usage (higher risk when not used)
        (CASE
            WHEN MIN(CAST(ff.IsUsedChip AS INT) + CAST(ff.IsUsedPIN AS INT)) = 0 THEN 80
            WHEN AVG(CAST(ff.IsUsedChip AS INT) + CAST(ff.IsUsedPIN AS INT)) < 1 THEN 50
            ELSE 15
        END) * 0.15  -- Security feature component (15%)
    FROM 
        Production.FactFraud ff
        JOIN Production.DimDate d ON ff.TransactionDateID = d.DateID
    WHERE 
        ff.CustomerSK = @CustomerSK
        AND d.DateID <= @DateID
        AND d.DateID >= @HistoricDateID  -- Last 90 days
    GROUP BY 
        ff.CustomerSK;
    
    RETURN ISNULL(@FraudRiskScore, 25.00);
END;
GO


-- Market Risk Score calculation function
CREATE OR ALTER FUNCTION Production.CalculateMarketRiskScore(
    @CustomerSK INT,
    @DateID INT
)
RETURNS DECIMAL(5,2)
AS
BEGIN
    DECLARE @MarketRiskScore DECIMAL(5,2);
    DECLARE @ReferenceDate DATE;
    DECLARE @HistoricDateID INT;
    
    -- Get the actual date for the DateID
    SELECT @ReferenceDate = FullDate 
    FROM Production.DimDate 
    WHERE DateID = @DateID;
    
    -- Find the DateID for 30 days ago
    SELECT @HistoricDateID = DateID
    FROM Production.DimDate
    WHERE FullDate = DATEADD(day, -30, @ReferenceDate);
    
    -- Get customer loan types
    WITH CustomerLoanTypes AS (
        SELECT 
            DISTINCT lt.LoanType
        FROM 
            Production.FactLoan fl
            JOIN Production.DimLoanType lt ON fl.LoanTypeID = lt.LoanTypeID
        WHERE 
            fl.CustomerSK = @CustomerSK
    ),
    
    -- Get recent market conditions
    MarketConditions AS (
        SELECT 
            AVG(fm.VolatilityRatio) AS AvgVolatility,
            AVG(fm.InterestRate) AS AvgInterestRate,
            AVG(ABS(fm.DailyChangePercent)) AS AvgMarketChange,
			AVG(fe.UnemploymentRate) AS UnemploymentRate
        FROM 
            Production.FactMarket fm
            JOIN Production.DimDate d ON fm.DateID = d.DateID
            JOIN Production.FactEconomicIndicators fe 
				ON fe.ReportDateID = fm.DateID
				AND fe.CountryID = (
					SELECT TOP 1 c.CountryID 
					FROM Production.FactLoan fl
					JOIN Production.DimLoanType lt ON fl.LoanTypeID = lt.LoanTypeID
					JOIN Production.DimCountry c ON c.CountryID = 1 -- Default country
					WHERE fl.CustomerSK = @CustomerSK
					ORDER BY fl.LoanAmount DESC
				)
        WHERE 
            d.DateID <= @DateID
            AND d.DateID >= @HistoricDateID  -- Last 30 days
    )
    
    -- Calculate market risk score
    SELECT @MarketRiskScore = 
        -- Market volatility component (range 0-100)
        (CASE 
            WHEN mc.AvgVolatility > 0.05 THEN 100
            WHEN mc.AvgVolatility > 0.03 THEN 80
            WHEN mc.AvgVolatility > 0.02 THEN 60
            WHEN mc.AvgVolatility > 0.01 THEN 40
            ELSE 20
         END) * 0.30 +  -- Volatility component (30%)
        
        -- Interest rate component (range 0-100)
        (CASE 
            WHEN mc.AvgInterestRate > 8 THEN 90
            WHEN mc.AvgInterestRate > 6 THEN 70
            WHEN mc.AvgInterestRate > 4 THEN 50
            WHEN mc.AvgInterestRate > 2 THEN 30
            ELSE 20
         END) * 0.25 +  -- Interest rate component (25%)
        
        -- Economic indicator component (range 0-100)
        (CASE 
            WHEN mc.UnemploymentRate > 8 THEN 90
            WHEN mc.UnemploymentRate > 6 THEN 70
            WHEN mc.UnemploymentRate > 4 THEN 40
            ELSE 20
         END) * 0.25 +  -- Economic indicator component (25%)
        
        -- Market change component (range 0-100)
        (CASE
            WHEN mc.AvgMarketChange > 3 THEN 100
            WHEN mc.AvgMarketChange > 2 THEN 80
            WHEN mc.AvgMarketChange > 1 THEN 60
            WHEN mc.AvgMarketChange > 0.5 THEN 40
            ELSE 20
         END) * 0.20  -- Market change component (20%)
    FROM 
        MarketConditions mc
    CROSS JOIN
        CustomerLoanTypes;
    
    RETURN ISNULL(@MarketRiskScore, 50.00);
END;
GO


-- Aggregate Risk Score calculation function
CREATE OR ALTER FUNCTION Production.CalculateAggregateRiskScore(
    @CustomerSK INT,
    @DateID INT
)
RETURNS DECIMAL(5,2)
AS
BEGIN
    DECLARE @CreditRiskScore DECIMAL(5,2);
    DECLARE @FraudRiskScore DECIMAL(5,2);
    DECLARE @MarketRiskScore DECIMAL(5,2);
    
    -- Calculate individual risk scores
    SET @CreditRiskScore = Production.CalculateCreditRiskScore(@CustomerSK, @DateID);
    SET @FraudRiskScore = Production.CalculateFraudRiskScore(@CustomerSK, @DateID);
    SET @MarketRiskScore = Production.CalculateMarketRiskScore(@CustomerSK, @DateID);
    
    -- Calculate weighted average
    RETURN (
        @CreditRiskScore * 0.60 +   -- Credit risk has highest weight (60%)
        @FraudRiskScore * 0.15 +    -- Fraud risk (15%)
        @MarketRiskScore * 0.25     -- Market risk (25%)
    );
END;
GO

-- =========================================================
-- Create procedure to update risk scores in FactRiskProfile
-- =========================================================
CREATE OR ALTER PROCEDURE Production.usp_UpdateRiskScores
    @DateID INT = NULL 
AS
BEGIN
    SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;

    DECLARE @StartTime DATETIME = GETDATE();
	
    -- Use current date if not specified
    IF @DateID IS NULL
        SELECT @DateID = MAX(DateID) FROM Production.DimDate WHERE FullDate <= GETDATE();
    
    -- Calculate scores for all customers
    MERGE INTO Production.FactRiskProfile AS target
    USING (
        SELECT 
            fc.CustomerSK,
            @DateID AS DateID,
            Production.CalculateCreditRiskScore(fc.CustomerSK, @DateID) AS CreditRiskScore,
            Production.CalculateFraudRiskScore(fc.CustomerSK, @DateID) AS FraudRiskScore,
            Production.CalculateMarketRiskScore(fc.CustomerSK, @DateID) AS MarketRiskScore,
            Production.CalculateAggregateRiskScore(fc.CustomerSK, @DateID) AS NewAggregateRiskScore,
            ISNULL(frp.AggregateRiskScore, 0) AS PreviousAggregateRiskScore
        FROM 
            Production.FactCustomer fc
            LEFT JOIN Production.FactRiskProfile frp 
                ON fc.CustomerSK = frp.CustomerSK 
                AND frp.DateID = (
                    SELECT MAX(DateID) 
                    FROM Production.FactRiskProfile 
                    WHERE CustomerSK = fc.CustomerSK AND DateID < @DateID
                )
        WHERE 
            fc.DateSK = (
                SELECT MAX(DateSK) 
                FROM Production.FactCustomer 
                WHERE CustomerSK = fc.CustomerSK AND DateSK <= @DateID
            )
    ) AS source
    ON (target.CustomerSK = source.CustomerSK AND target.DateID = source.DateID)
    
    WHEN MATCHED THEN
        UPDATE SET
            CreditRiskScore = source.CreditRiskScore,
            FraudRiskScore = source.FraudRiskScore,
            MarketRiskScore = source.MarketRiskScore,
            AggregateRiskScore = source.NewAggregateRiskScore,
            PreviousAggregateRiskScore = source.PreviousAggregateRiskScore,
            RiskTrend = 
			CASE
                WHEN source.NewAggregateRiskScore > source.PreviousAggregateRiskScore * 1.1 THEN 'Increasing'
                WHEN source.NewAggregateRiskScore < source.PreviousAggregateRiskScore * 0.9 THEN 'Decreasing'
                ELSE 'Stable'
            END,
            RiskCategory = 
			CASE
                WHEN source.NewAggregateRiskScore >= 80 THEN 'Very High'
                WHEN source.NewAggregateRiskScore >= 60 THEN 'High'
                WHEN source.NewAggregateRiskScore >= 40 THEN 'Medium'
                ELSE 'Low'
            END
    
    WHEN NOT MATCHED THEN
        INSERT (
            CustomerSK, 
            DateID, 
            CreditRiskScore,
            FraudRiskScore,
            MarketRiskScore,
            AggregateRiskScore,
            PreviousAggregateRiskScore,
            RiskTrend,
            RiskCategory
        )
        VALUES (
            source.CustomerSK,
            source.DateID,
            source.CreditRiskScore,
            source.FraudRiskScore,
            source.MarketRiskScore,
            source.NewAggregateRiskScore,
            source.PreviousAggregateRiskScore,
            CASE
                WHEN source.NewAggregateRiskScore > source.PreviousAggregateRiskScore * 1.1 THEN 'Increasing'
                WHEN source.NewAggregateRiskScore < source.PreviousAggregateRiskScore * 0.9 THEN 'Decreasing'
                ELSE 'Stable'
            END,
            CASE
                WHEN source.NewAggregateRiskScore >= 80 THEN 'Very High'
                WHEN source.NewAggregateRiskScore >= 60 THEN 'High'
                WHEN source.NewAggregateRiskScore >= 40 THEN 'Medium'
                ELSE 'Low'
            END
        );
        
    -- Log the update
    INSERT INTO Production.ETLLog (ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
    VALUES ('usp_UpdateRiskScores', 'FactRiskProfile Table', 'Success', 
			@@ROWCOUNT, DATEDIFF(SECOND, @StartTime, GETDATE()));
	
END;
GO