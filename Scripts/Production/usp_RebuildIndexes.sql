/* 
This script creates a stored procedure to rebuild fragmented indexes
Last updated: 2025-04-05
*/

CREATE OR ALTER PROCEDURE Production.usp_RebuildIndexes
	@MinFragmentationForReorg FLOAT = 10.0,
	@MinFragmentationForRebuild FLOAT = 30.0,
	@BatchSize INT = 10,
	@OnlineOnly BIT = 0,
	@LogPath NVARCHAR(500) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Variables for tracking execution
	DECLARE @TableName NVARCHAR(256);
	DECLARE @IndexName NVARCHAR(256);
	DECLARE @SchemaName NVARCHAR(256);
	DECLARE @ObjectID INT;
	DECLARE @IndexID INT;
	DECLARE @Fragmentation NVARCHAR(5);
	DECLARE @PageCount INT;
	DECLARE @SQL NVARCHAR(MAX);
	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @IndexesRebuilt INT = 0;
	DECLARE @IndexesReorganized INT = 0;
	DECLARE @ErrorMsg NVARCHAR(4000); 
	
	-- Create temporary table to hold index information
	CREATE TABLE #FragmentedIndexes(
		ObjectID INT,
		ObjectName NVARCHAR(256),
		SchemaName NVARCHAR(256),
		IndexID INT,
		IndexName NVARCHAR(256),
		Fragmentation NVARCHAR(5),
		PageCount INT,
		IsProcessed BIT DEFAULT 0
	);
	
	-- Get list of databases
	INSERT INTO #FragmentedIndexes(
		ObjectID, 
		IndexID,
		Fragmentation,
		PageCount
	)
	SELECT
		ps.object_id,
		ps.index_id,
		ps.avg_fragmentation_in_percent,
		ps.page_count
	FROM 
		-- Use database compatibility level-aware syntax
		sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps
	WHERE
		ps.index_id > 0 -- Exclude heaps
		AND ps.page_count > 25 -- Only consider indexes with at least 25 pages
		AND ps.avg_fragmentation_in_percent > @MinFragmentationForReorg;
	
	-- Add additional index metadata
	UPDATE #FragmentedIndexes
	SET 
		ObjectName = o.name,
		SchemaName = s.name,
		IndexName = i.name
	FROM 
		#FragmentedIndexes f
		INNER JOIN sys.objects o ON f.ObjectID = o.object_id
		INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
		INNER JOIN sys.indexes i ON f.ObjectID = i.object_id AND f.IndexID = i.index_id;
	
	-- Process indexes in batches
	WHILE EXISTS(SELECT 1 FROM #FragmentedIndexes WHERE IsProcessed = 0)
	BEGIN
		-- Get the next batch of indexes to process
		SELECT TOP(@BatchSize)
			@ObjectID = ObjectID,
			@IndexID = IndexID,
			@TableName = ObjectName,
			@SchemaName = SchemaName,
			@IndexName = IndexName,
			@Fragmentation = Fragmentation,
			@PageCount = PageCount
		FROM #FragmentedIndexes
		WHERE IsProcessed = 0
		ORDER BY 
			Fragmentation DESC, -- Process most fragmented first
			PageCount DESC;     -- Then largest indexes
			
		-- Log the index being processed
		RAISERROR('Processing index: %s.%s.%s (%s%% fragmentation, %d pages)', 
				0, 1, @SchemaName, @TableName, @IndexName, @Fragmentation, @PageCount) WITH NOWAIT;
		
		-- Choose action based on fragmentation level
		IF @Fragmentation >= @MinFragmentationForRebuild
		BEGIN
			-- Rebuild the index
			SET @SQL = 'ALTER INDEX ' + QUOTENAME(@IndexName) + 
					' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
			
			IF @OnlineOnly = 1
			BEGIN
				SET @SQL = @SQL + ' REBUILD WITH (ONLINE = ON, SORT_IN_TEMPDB = ON)';
			END
			ELSE
			BEGIN
				SET @SQL = @SQL + ' REBUILD WITH (SORT_IN_TEMPDB = ON)';
			END
			
			BEGIN TRY
				EXEC sp_executesql @SQL;
				SET @IndexesRebuilt = @IndexesRebuilt + 1;
				
				-- Log the rebuild
				RAISERROR('Rebuilt index: %s.%s.%s', 0, 1, @SchemaName, @TableName, @IndexName) WITH NOWAIT;
			END TRY
			BEGIN CATCH
				SET @ErrorMsg = ERROR_MESSAGE();
				RAISERROR('Error rebuilding index %s.%s.%s: %s', 
						16, 1, @SchemaName, @TableName, @IndexName, @ErrorMsg);
			END CATCH
		END
		ELSE IF @Fragmentation >= @MinFragmentationForReorg
		BEGIN
			-- Reorganize the index
			SET @SQL = 'ALTER INDEX ' + QUOTENAME(@IndexName) + 
					' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + 
					' REORGANIZE';
			
			BEGIN TRY
				EXEC sp_executesql @SQL;
				SET @IndexesReorganized = @IndexesReorganized + 1;
				
				-- Log the reorganize
				RAISERROR('Reorganized index: %s.%s.%s', 0, 1, @SchemaName, @TableName, @IndexName) WITH NOWAIT;
			END TRY
			BEGIN CATCH
			SET @ErrorMsg = ERROR_MESSAGE();
				RAISERROR('Error reorganizing index %s.%s.%s: %s', 
						16, 1, @SchemaName, @TableName, @IndexName, @ErrorMsg);
			END CATCH
		END
		
		-- Mark index as processed
		UPDATE #FragmentedIndexes 
		SET IsProcessed = 1 
		WHERE ObjectID = @ObjectID AND IndexID = @IndexID;
	END
	
	-- Update statistics for all tables
	EXEC sp_updatestats;
	
	-- Log the index maintenance results
	INSERT INTO Production.ETLLog 
		(ProcedureName, StepName, Status, RowsProcessed, ExecutionTimeSeconds)
	VALUES 
		('usp_RebuildIndexes', 'IndexMaintenance', 'Completed', 
		@IndexesRebuilt + @IndexesReorganized, DATEDIFF(SECOND, @StartTime, GETDATE()));
		
	-- Return results
	SELECT 
		'Index maintenance completed' AS Status, 
		@IndexesRebuilt AS IndexesRebuilt, 
		@IndexesReorganized AS IndexesReorganized,
		DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionTime;
		
	-- Cleanup
	DROP TABLE #FragmentedIndexes;
END;
GO