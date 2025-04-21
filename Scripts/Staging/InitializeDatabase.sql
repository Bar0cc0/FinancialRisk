/* 
This script initializes the $(Database) database 
by creating the necessary schemas for staging and production environments.
*/

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- Enable xp_cmdshell if it is not already enabled
IF NOT EXISTS (SELECT * FROM sys.configurations WHERE name = 'xp_cmdshell' AND value_in_use = 1)
BEGIN
    EXEC sp_configure 'show advanced options', 1;  
    RECONFIGURE;
    EXEC sp_configure 'xp_cmdshell', 1;
    RECONFIGURE;
END;
GO

-- Create the $(Database) database if it does not exist
DECLARE @DatabaseName NVARCHAR(128) = '$(Database)';

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = @DatabaseName)
BEGIN
	BEGIN TRY
		CREATE DATABASE [$(Database)];
		PRINT 'Database $(Database) created successfully.';
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
		RAISERROR('Error creating database: %s', 15, 1, @ErrorMsg);
		RETURN;
	END CATCH;
END
ELSE
BEGIN
	PRINT 'Database $(Database) already exists.';
END
GO

-- Use the created database
USE [$(Database)];
GO

-- Create a dedicated schema for the $(Database) database Staging environment
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Staging')
BEGIN
	BEGIN TRY
		EXEC('CREATE SCHEMA Staging');
		PRINT 'Schema Staging created successfully.';
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMsg1 NVARCHAR(4000) = ERROR_MESSAGE();
		RAISERROR('Error creating Staging schema: %s', 15, 1, @ErrorMsg1);
	END CATCH;
END
ELSE
BEGIN
	PRINT 'Schema Staging already exists.';
END
GO

-- Create a dedicated schema for the $(Database) database Production environment
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Production')
BEGIN
	BEGIN TRY
		EXEC('CREATE SCHEMA Production');
		PRINT 'Schema Production created successfully.';
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMsg2 NVARCHAR(4000) = ERROR_MESSAGE();
		RAISERROR('Error creating Production schema: %s', 15, 1, @ErrorMsg2);
	END CATCH;
END
ELSE
BEGIN
	PRINT 'Schema Production already exists.';
END
GO

-- Create a dedicated schema for the $(Database) database Config environment
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Config')
BEGIN
	BEGIN TRY
		EXEC('CREATE SCHEMA Config');
		PRINT 'Schema Config created successfully.';
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMsg3 NVARCHAR(4000) = ERROR_MESSAGE();
		RAISERROR('Error creating Config schema: %s', 15, 1, @ErrorMsg3);
	END CATCH;
END
ELSE
BEGIN
	PRINT 'Schema Config already exists.';
END
GO

PRINT 'Database initialization completed successfully.';
GO