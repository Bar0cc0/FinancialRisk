
/*
Creates a centralized configuration management system for ETL processes in the $(Database) database.
*/

USE [$(Database)];
GO

-- Procedure to insert or update configuration
CREATE OR ALTER PROCEDURE Config.usp_SetConfigValue
	@ConfigKey NVARCHAR(100),
	@ConfigValue NVARCHAR(500),
	@CategoryName NVARCHAR(50) = 'ETL',
	@Environment NVARCHAR(20) = 'All',
	@Description NVARCHAR(500) = NULL,
	@IsEncrypted BIT = 0,
	@ChangeReason NVARCHAR(500) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	DECLARE @CategoryID INT;
	DECLARE @OldValue NVARCHAR(500);
	DECLARE @ConfigID INT;
	
	-- Get category ID
	SELECT @CategoryID = CategoryID 
	FROM Config.ConfigCategory 
	WHERE CategoryName = @CategoryName;
	
	-- Create category if it doesn't exist
	IF @CategoryID IS NULL
	BEGIN
		INSERT INTO Config.ConfigCategory (CategoryName, Description)
		VALUES (@CategoryName, 'Auto-created category');
		
		SET @CategoryID = SCOPE_IDENTITY();
	END;
	
	-- Check if config exists and is active
	SELECT @ConfigID = ConfigID, @OldValue = ConfigValue
	FROM Config.ConfigurationSettings
	WHERE ConfigKey = @ConfigKey 
	AND Environment = @Environment 
	AND IsActive = 1;
	
	-- If exists, update it
	IF @ConfigID IS NOT NULL
	BEGIN
		-- Store old value in history
		INSERT INTO Config.ConfigurationHistory 
			(ConfigID, ConfigKey, OldValue, NewValue, Environment, ChangeReason)
		VALUES 
			(@ConfigID, @ConfigKey, @OldValue, @ConfigValue, @Environment, @ChangeReason);
		
		-- Update current value
		UPDATE Config.ConfigurationSettings
		SET ConfigValue = @ConfigValue,
			Description = ISNULL(@Description, Description),
			LastModified = GETDATE(),
			ModifiedBy = SYSTEM_USER,
			Version = Version + 1
		WHERE ConfigID = @ConfigID;
	END
	-- Otherwise insert new
	ELSE
	BEGIN
		INSERT INTO Config.ConfigurationSettings 
			(ConfigKey, ConfigValue, CategoryID, Environment, Description, IsEncrypted)
		VALUES 
			(@ConfigKey, @ConfigValue, @CategoryID, @Environment, @Description, @IsEncrypted);
	END;
END;
GO

-- Procedure to get configuration values
CREATE OR ALTER PROCEDURE Config.usp_GetConfigValue
	@ConfigKey NVARCHAR(100),
	@Environment NVARCHAR(20) = NULL, -- Current environment
	@DefaultValue NVARCHAR(500) = NULL,
	@ConfigValue NVARCHAR(500) OUTPUT
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_NULLS ON;
	SET QUOTED_IDENTIFIER ON;
	
	-- Default to current environment if not specified
	IF @Environment IS NULL
	BEGIN
		-- Try to get from Config
		SELECT @Environment = ConfigValue
		FROM Config.ConfigurationSettings
		WHERE ConfigKey = 'CurrentEnvironment' AND IsActive = 1;
		
		-- Default to Staging if not found
		SET @Environment = ISNULL(@Environment, 'Staging');
	END;
	
	-- Try to get environment-specific value
	SELECT TOP 1 @ConfigValue = ConfigValue 
	FROM Config.ConfigurationSettings
	WHERE ConfigKey = @ConfigKey 
	AND IsActive = 1
	AND Environment = @Environment;
	
	-- If not found, try to get from 'All' environment
	IF @ConfigValue IS NULL
	BEGIN
		SELECT TOP 1 @ConfigValue = ConfigValue 
		FROM Config.ConfigurationSettings
		WHERE ConfigKey = @ConfigKey 
		AND IsActive = 1
		AND Environment = 'All';
	END;
	
	-- If still not found, use default value
	IF @ConfigValue IS NULL
	BEGIN
		SET @ConfigValue = @DefaultValue;
		
		-- Log missing configuration if ETLLog exists
		IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Staging'))
		BEGIN
			INSERT INTO Staging.ETLLog (
				ProcedureName, StepName, Status, ErrorMessage
			)
			VALUES (
				'Config.usp_GetConfigValue', 
				'Get Config', 
				'Warning', 
				'Configuration key "' + @ConfigKey + '" not found for environment "' + 
				@Environment + '". Using default: ' + ISNULL(@DefaultValue, 'NULL')
			);
		END;
	END;
END;
GO



-- Main script to create the configuration management system
BEGIN TRY
	BEGIN TRANSACTION;

	-- Ensure schema exists
	IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Config')
	BEGIN
		EXEC('CREATE SCHEMA Config');
	END

	-- Create configuration category table
	IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ConfigCategory' AND schema_id = SCHEMA_ID('Config'))
	BEGIN
		CREATE TABLE Config.ConfigCategory (
			CategoryID INT IDENTITY(1,1) PRIMARY KEY,
			CategoryName NVARCHAR(50) NOT NULL UNIQUE,
			Description NVARCHAR(500) NULL,
			CreatedDate DATETIME NOT NULL DEFAULT GETDATE()
		);
		
		-- Insert default categories
		INSERT INTO Config.ConfigCategory (CategoryName, Description)
		VALUES 
			('Paths', 'File system paths for data and logs'),
			('ETL', 'ETL process settings'),
			('Validation', 'Data validation rules and thresholds'),
			('Security', 'Security-related settings'),
			('Performance', 'Performance optimization settings'),
			('Notification', 'Alert and notification settings');
	END

	-- Create main configuration table
	IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ConfigurationSettings' AND schema_id = SCHEMA_ID('Config'))
	BEGIN
		CREATE TABLE Config.ConfigurationSettings (
			ConfigID INT IDENTITY(1,1) PRIMARY KEY,
			ConfigKey NVARCHAR(100) NOT NULL,
			ConfigValue NVARCHAR(500) NOT NULL,
			IsEncrypted BIT NOT NULL DEFAULT 0,
			CategoryID INT NOT NULL REFERENCES Config.ConfigCategory(CategoryID),
			Environment NVARCHAR(20) NOT NULL, -- 'Staging', 'Production', 'All'
			Description NVARCHAR(500) NULL,
			IsActive BIT NOT NULL DEFAULT 1,
			Version INT NOT NULL DEFAULT 1,
			CreatedDate DATETIME NOT NULL DEFAULT GETDATE(),
			LastModified DATETIME NOT NULL DEFAULT GETDATE(),
			ModifiedBy NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
			CONSTRAINT UQ_Config_Key_Env UNIQUE (ConfigKey, Environment, IsActive)
		);
		
		-- Create index for faster lookups
		CREATE NONCLUSTERED INDEX IX_ConfigSettings_KeyEnv
		ON Config.ConfigurationSettings(ConfigKey, Environment, IsActive);
	END

	-- Create configuration history table
	IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ConfigurationHistory' AND schema_id = SCHEMA_ID('Config'))
	BEGIN
		CREATE TABLE Config.ConfigurationHistory (
			HistoryID INT IDENTITY(1,1) PRIMARY KEY,
			ConfigID INT NOT NULL,
			ConfigKey NVARCHAR(100) NOT NULL,
			OldValue NVARCHAR(500) NOT NULL,
			NewValue NVARCHAR(500) NOT NULL,
			Environment NVARCHAR(20) NOT NULL,
			ChangeDate DATETIME NOT NULL DEFAULT GETDATE(),
			ChangedBy NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
			ChangeReason NVARCHAR(500) NULL
		);
		
		-- Create index for faster lookups
		CREATE NONCLUSTERED INDEX IX_ConfigHistory_ConfigID
		ON Config.ConfigurationHistory(ConfigID);
	END;
	
	
	-- Populate with default values
	-- Determine paths using variables passed from PowerShell
	DECLARE @DataPath NVARCHAR(500) = '$(DataPath)';
	DECLARE @LogPath NVARCHAR(500) = '$(LogPath)';
	DECLARE @ScriptsPath NVARCHAR(500) = '$(ScriptsPath)';
	
	-- Set default configurations
	EXEC Config.usp_SetConfigValue 'CurrentEnvironment', 'Staging', 'ETL', 'All', 'Current execution environment';
	EXEC Config.usp_SetConfigValue 'DataPath', @DataPath, 'Paths', 'All', 'Path to source data files';
	EXEC Config.usp_SetConfigValue 'LogPath', @LogPath, 'Paths', 'All', 'Path to log files';
	EXEC Config.usp_SetConfigValue 'ScriptsPath', @ScriptsPath, 'Paths', 'All', 'Path to scripts files';
	EXEC Config.usp_SetConfigValue 'DefaultSchema', 'Staging', 'ETL', 'Staging', 'Default schema for staging objects';
	EXEC Config.usp_SetConfigValue 'DefaultSchema', 'Production', 'ETL', 'Production', 'Default schema for production objects';
	EXEC Config.usp_SetConfigValue 'ErrorThreshold', '100', 'Validation', 'All', 'Maximum number of errors before ETL fails';
	EXEC Config.usp_SetConfigValue 'BatchSize', '10000', 'ETL', 'All', 'Number of rows to process in each batch';

	COMMIT TRANSACTION;
	
	PRINT 'Configuration management system created successfully.';
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
		ROLLBACK TRANSACTION;
		
	DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
	DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
	DECLARE @ErrorState INT = ERROR_STATE();
	
	RAISERROR(@ErrorMsg, @ErrorSeverity, @ErrorState);
END CATCH;
GO

-- Set additional configuration settings
EXEC Config.usp_SetConfigValue 'ValidationLogLevel', 'Warning', 'Validation', 'All', 'Minimum log level for validation issues';
EXEC Config.usp_SetConfigValue 'DatabaseBackupPath', '$(DataPath)\Backups', 'Paths', 'All', 'Path for database backups';
GO

