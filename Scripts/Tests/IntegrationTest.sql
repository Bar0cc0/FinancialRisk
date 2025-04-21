-- ===================================================
-- Test staging to production workflow (sample subset)
-- ===================================================
SET NOCOUNT ON;

PRINT '===============================================';
PRINT 'INTEGRATION TESTING - FINANCIAL RISK ETL PIPELINE';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '===============================================';

-- 1. Create a test batch
DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
PRINT 'Test BatchID: ' + CAST(@BatchID AS VARCHAR(50));

BEGIN TRY
    -- 2. Load staging data
    PRINT 'Step 1: Loading staging data...';
    EXEC Staging.usp_LoadStagingTables 
        @BatchID = @BatchID,
        @DataPath = N'$(DataPath)',
        @LogPath = N'$(LogPath)',
        @ScriptsPath = N'$(ScriptsPath)';

    -- 3. Validate staging data
    PRINT 'Step 2: Validating staging data...';
    EXEC Staging.usp_ValidateStagingData
        @BatchID = @BatchID,
        @LogPath = N'$(LogPath)';

    -- 4. Load specific production tables
    PRINT 'Step 3: Loading production tables...';
    EXEC Production.usp_LoadProductionTables 
        @BatchID = @BatchID,
        @LogPath = N'$(LogPath)',
        @LoadDimensions = 1,
        @LoadFacts = 1;

    -- 5. Verify data integrity between staging and production
    PRINT 'Step 4: Verifying data integrity...';
    SELECT 
        'Loan Data' AS DataType,
        (SELECT COUNT(*) FROM Staging.Loan) AS StagingCount,
        (SELECT COUNT(*) FROM Production.FactLoan) AS ProductionCount;

    SELECT 
        'Customer Data' AS DataType,
        (SELECT COUNT(DISTINCT CustomerID) FROM Staging.Loan) AS StagingCustomerCount,
        (SELECT COUNT(*) FROM Production.DimCustomer WHERE IsCurrent = 1) AS ProductionCustomerCount;

    -- ==========================================
    -- Execute entire ETL process with test data
    -- ==========================================
    PRINT 'Step 5: Running full ETL process...';
    
    -- Execute staging ETL first
    EXEC Staging.usp_ExecuteStagingETL
        @Database = N'$(Database)',
        @DataPath = N'$(DataPath)',
        @LogPath = N'$(LogPath)';
    
    -- Then execute production ETL
    EXEC Production.usp_ExecuteProductionETL
        @Database = N'$(Database)',
        @DataPath = N'$(DataPath)',
        @LogPath = N'$(LogPath)',
        @ValidateAfterLoad = 1;

    -- Check ETL execution log
    PRINT 'Step 6: Checking ETL execution logs...';
    SELECT TOP 20 
        ProcedureName, 
        StepName, 
        ExecutionDateTime, 
        Status, 
        ErrorMessage,
        RowsProcessed
    FROM Production.ETLLog
    ORDER BY ExecutionDateTime DESC;

    -- Verify final data volumes match expectations
    PRINT 'Step 7: Verifying final data volumes...';
    SELECT 'DimCustomer' AS [Table], COUNT(*) AS RecordCount FROM Production.DimCustomer
    UNION ALL
    SELECT 'FactLoan', COUNT(*) FROM Production.FactLoan
    UNION ALL
    SELECT 'FactCredit', COUNT(*) FROM Production.FactCredit
    UNION ALL
    SELECT 'FactAccount', COUNT(*) FROM Production.FactAccount
    UNION ALL
    SELECT 'ValidationLog', COUNT(*) FROM Production.ValidationLog
    ORDER BY [Table];

    -- ==========================================
    -- Error handling tests
    -- ==========================================
    PRINT 'Step 8: Testing error handling with invalid paths...';
    
    BEGIN TRY
        -- Test with missing source files
        EXEC Production.usp_ExecuteProductionETL
            @Database = N'$(Database)',
            @DataPath = N'$(DataPath)\NonExistent',
            @LogPath = N'$(LogPath)',
            @ValidateAfterLoad = 1;
    END TRY
    BEGIN CATCH
        PRINT 'Expected error occurred: ' + ERROR_MESSAGE();
    END CATCH
        
    -- Check error logging
    PRINT 'Checking error logs...';
    SELECT TOP 10 
        ProcedureName, 
        StepName, 
        ExecutionDateTime, 
        Status, 
        ErrorMessage
    FROM Production.ETLLog 
    WHERE Status = 'Failed' 
    ORDER BY ExecutionDateTime DESC;

    -- Check validation results
    SELECT TOP 10 
        BatchID, 
        ExecutionDateTime, 
        ValidationCategory, 
        ErrorType, 
        Severity, 
        ErrorCount, 
        IsBlocking
    FROM Production.ValidationLog 
    ORDER BY ExecutionDateTime DESC;

    -- ==========================================
    -- Performance tests
    -- ==========================================
    PRINT 'Step 9: Running performance tests...';
    
    -- Execute ETL with performance tracking
    DECLARE @StartTime DATETIME = GETDATE();
    EXEC Production.usp_ExecuteProductionETL
        @Database = N'$(Database)',
        @DataPath = N'$(DataPath)',
        @LogPath = N'$(LogPath)',
        @ValidateAfterLoad = 1;
    
    PRINT 'Total execution time: ' + 
          CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR) + ' seconds';

    -- Check execution times by component
    SELECT 
        ProcedureName, 
        StepName,
        AVG(ExecutionTimeSeconds) AS AvgExecutionTimeSec,
        MAX(ExecutionTimeSeconds) AS MaxExecutionTimeSec,
        COUNT(*) AS ExecutionCount
    FROM Production.ETLLog
    WHERE ExecutionDateTime > DATEADD(HOUR, -1, GETDATE())
      AND ExecutionTimeSeconds IS NOT NULL
    GROUP BY ProcedureName, StepName
    ORDER BY MAX(ExecutionTimeSeconds) DESC;
    
    PRINT 'Integration tests completed successfully!';
END TRY
BEGIN CATCH
    PRINT 'Integration test failed: ' + ERROR_MESSAGE();
END CATCH