#-------------------------------------
# IFRS9SupportPatch.ps1
# NOTE: Not yet released to production
#-------------------------------------

<#
.SYNOPSIS
	Deploys and configures IFRS9 compliance components for the Financial Risk database.

.DESCRIPTION
	This script automates the deployment and configuration of IFRS9 (International Financial Reporting Standard 9)
	components for financial risk reporting. It performs the following main operations:
	- Creates the IFRS9 schema if it doesn't exist
	- Deploys required tables and stored procedures for IFRS9 compliance
	- Integrates IFRS9 with existing ETL processes
	- Creates SQL Agent jobs for automated IFRS9 processing
	- Runs validation tests to ensure proper deployment
	- Optionally executes an initial IFRS9 ETL process

.PARAMETER ServerInstance
	SQL Server instance name where the IFRS9 components will be deployed.
	Default is "(local)".

.PARAMETER Database
	Name of the database where IFRS9 components will be deployed.
	Default is "FinancialRiskDB".

.PARAMETER ScriptsPath
	Path to the directory containing SQL scripts and IFRS9 components.
	Default is the directory containing this script.

.PARAMETER LogPath
	Path where log files will be written.
	Default is a "Logs" folder in the parent directory of ScriptsPath.

.PARAMETER NotificationEmail
	Email address for SQL Agent job notifications.
	Default is "dba@financialrisk.com".

.PARAMETER IsTest
	Switch parameter to run the script in test mode without making permanent changes.
	Default is false.

.PARAMETER DeployIFRS9
	Controls whether IFRS9 components should be deployed.
	Default is true.

.PARAMETER RunIFRS9ETL
	Controls whether the IFRS9 ETL process should be executed after deployment.
	Default is true.

.EXAMPLE
	.\IFRS9SupportPatch.ps1
	# Runs with default parameters, deploying to local instance and FinancialRiskDB database

.NOTES
	Author: Michael Garancher
	Last Updated: April 21, 2025
	Requirements: 
	- SQL Server with appropriate permissions
	- PowerShell 5.1 or higher
	- All required IFRS9 SQL scripts must be available in the ScriptsPath\IFRS9 directory
#>

param (
	[Parameter(Mandatory=$false)]
	[ValidateNotNullOrEmpty()]
	[string]$ServerInstance = "(local)",
	
	[Parameter(Mandatory=$false)]
	[ValidateNotNullOrEmpty()]
	[string]$Database = "FinancialRiskDB",
	
	[Parameter(Mandatory=$false)]
	[ValidateNotNullOrEmpty()]
	[string]$ScriptsPath = $PSScriptRoot,
	
	[Parameter(Mandatory=$false)]
	[ValidateNotNullOrEmpty()]
	[string]$LogPath = (Join-Path -Path (Split-Path -Parent $ScriptsPath) -ChildPath "Logs"),
	
	[Parameter(Mandatory=$false)]
	[ValidateNotNullOrEmpty()]
	[string]$NotificationEmail = "dba@financialrisk.com",
	
	[Parameter(Mandatory=$false)]
	[switch]$IsTest = $false,
	
	[Parameter(Mandatory=$false)]
	[bool]$DeployIFRS9 = $true,
	
	[Parameter(Mandatory=$false)]
	[bool]$RunIFRS9ETL = $true
)


# Helper function to log messages
function Write-Log {
	param (
		[string]$Message,
		[string]$ForegroundColor,
		[string]$LogFile = (Join-Path -Path $LogPath -ChildPath ($timestamp + "_ETLDeployment.log"))
	)
	
	if (-not (Test-Path -Path $LogPath)) {
		New-Item -Path $LogPath -ItemType Directory -Force | Out-Null
	}

	Add-Content -Path $LogFile -Value $Message
	Write-Host $Message -ForegroundColor $ForegroundColor
}

#region Test-SqlConnection()
# Function to test SQL Server connection
function Test-SqlConnection {
	param (
		[string]$ServerInstance,
		[string]$Database = "master",
		[int]$Timeout = 3
	)
	
	try {
		Write-Log "`nTesting SQL connection to $ServerInstance..." -ForegroundColor Yellow
		$connectionString = "Server=$ServerInstance;Database=$Database;Integrated Security=True;Connection Timeout=$Timeout"
		$connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
		$connection.Open()
		$connection.Close()
		Write-Log "Successfully connected to $ServerInstance" -ForegroundColor Green
		return $true
	}
	catch {
		Write-Log "Failed to connect to SQL Server: $_" -ForegroundColor Red
		
		Write-Log "`nTroubleshooting information:" -ForegroundColor Yellow
		Write-Log "- Ensure SQL Server is running on $ServerInstance" -ForegroundColor Yellow
		Write-Log "- Check if SQL Browser service is running (for named instances)" -ForegroundColor Yellow
		Write-Log "- Make sure TCP/IP protocol is enabled in SQL Configuration Manager" -ForegroundColor Yellow
		Write-Log "- Verify port 1433 is accessible (default SQL port)" -ForegroundColor Yellow
		Write-Log "- Consider using the full server instance name" -ForegroundColor Yellow
		
		return $false
	}
}
#endregion

#region Execute-Script()
# Function to execute a SQL script file with variable replacements
# Execute a script file using the given connection
function Execute-Script {
	param (
		[System.Data.SqlClient.SqlConnection]$Connection,
		[string]$ScriptPath,
		[hashtable]$Variables = @{}
	)
	
	# Reset database context before executing each script
	$resetContextCmd = $Connection.CreateCommand()
	$resetContextCmd.CommandText = "USE $($Connection.Database)"
	$resetContextCmd.ExecuteNonQuery() | Out-Null

	# Reset metadata cache if we're running IFRS9 scripts
	if ($ScriptPath -like "*IFRS9*") {
		$resetCacheCmd = $Connection.CreateCommand()
		$resetCacheCmd.CommandText = "DBCC FREEPROCCACHE; DBCC FREESYSTEMCACHE('ALL');"
		$resetCacheCmd.ExecuteNonQuery() | Out-Null
	}
		

	# Log the script execution
	Write-Log "  Executing script: $ScriptPath" -ForegroundColor Cyan
	
	# Read the script content
	if (-not (Test-Path $ScriptPath)) {
		Write-Log "  ERROR: Script file not found: $ScriptPath" -ForegroundColor Red
		return $false
	}
	
	$scriptContent = Get-Content -Path $ScriptPath -Raw
	
	# Replace variables
	foreach ($key in $Variables.Keys) {
		$scriptContent = $scriptContent -replace "\$\($key\)", $Variables[$key]
	}
	
	# Split by GO statements
	$batches = $scriptContent -split '(?:\r\n|\r|\n)[\t ]*GO[\t ]*(?:\r\n|\r|\n|$)'
	
	# Execute each batch
	foreach ($batch in $batches) {
		$batch = $batch.Trim()
		if ([string]::IsNullOrWhiteSpace($batch)) {
			continue
		}
		
		$command = $Connection.CreateCommand()
		$command.CommandText = $batch
		$command.CommandTimeout = 180 # 3 minutes
		
		try {
			$command.ExecuteNonQuery() | Out-Null
		}
		catch {
			Write-Log "  Error executing batch in $ScriptPath" -ForegroundColor Red
			Write-Log "  Error: $_" -ForegroundColor Red
			throw
		}
	}
	
	return $true
}
#endregion

#region Deploy-IFRS9Components()
# Function to deploy IFRS9 components
function Deploy-IFRS9Components {
	param (
		[System.Data.SqlClient.SqlConnection]$Connection,
		[string]$ScriptsPath,
		[string]$LogPath,
		[string]$DataPath,
		[string]$DatabaseName 
	)
	
	Write-Log "`nDeploying IFRS9 compliance components..." -ForegroundColor Yellow
	
	$ifrs9Path = Join-Path -Path $ScriptsPath -ChildPath "IFRS9"
	
	# Check if IFRS9 directory exists
	if (-not (Test-Path $ifrs9Path)) {
		Write-Log "ERROR: IFRS9 scripts directory not found at $ifrs9Path" -ForegroundColor Red
		return $false
	}
	
	try {
		# 0. Create IFRS9 schema explicitly first
		$command = $Connection.CreateCommand()
		$command.CommandText = @"
		IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'IFRS9')
		BEGIN
			EXEC('CREATE SCHEMA IFRS9');
			PRINT 'Created IFRS9 schema';
		END
"@
		$command.ExecuteNonQuery() | Out-Null
		Write-Log "  IFRS9 schema created/verified" -ForegroundColor Green

		# 1. Run schema update script to add necessary columns if they don't exist
		$schemaUpdateScript = Join-Path -Path $ifrs9Path -ChildPath "UpdateSchemaForIFRS9.sql"
		if (Test-Path $schemaUpdateScript) {
			Write-Log "  Executing schema update: $schemaUpdateScript" -ForegroundColor Cyan
			$success = Execute-Script -Connection $Connection -ScriptPath $schemaUpdateScript -Variables @{
				Database = $DatabaseName
				LogPath = $LogPath
				DataPath = $DataPath
				DatabaseName = $DatabaseName
			}
			if (-not $success) {
				Write-Log "  CRITICAL ERROR: Failed to execute schema update script" -ForegroundColor Red
				return $false
			}
		} else {
			Write-Log "  CRITICAL ERROR: Schema update script not found at $schemaUpdateScript" -ForegroundColor Red
			return $false
		}
		
		# 2. Deploy IFRS9 procedures in correct order
		$ifrs9Scripts = @(
			# Core procedures
			"usp_ClassifyFinancialInstruments.sql",
			"usp_ClassifyECLStaging.sql",
			"usp_CalculateECLParameters.sql",
			"usp_CalculateECL.sql",
			"usp_CalculateECLMovement.sql",
			"usp_RefreshReportingViews.sql",
			# Master procedure
			"usp_ExecuteProcessIFRS9.sql"
		)
		
		$allScriptsExecuted = $true
		foreach ($scriptFile in $ifrs9Scripts) {
			$scriptPath = Join-Path -Path $ifrs9Path -ChildPath $scriptFile
			if (Test-Path $scriptPath) {
				Write-Log "  Executing IFRS9 script: $scriptFile" -ForegroundColor Cyan
				try {
					$success = Execute-Script -Connection $Connection -ScriptPath $scriptPath -Variables @{
						LogPath = $LogPath
						DataPath = $DataPath
						Database = $DatabaseName
					}
					if (-not $success) {
						Write-Log "  ERROR: Failed to execute $scriptFile" -ForegroundColor Red
						$allScriptsExecuted = $false
					}
				} catch {
					Write-Log "  EXCEPTION executing $scriptFile : $_" -ForegroundColor Red
					$allScriptsExecuted = $false
				}
			} else {
				Write-Log "  WARNING: IFRS9 script $scriptFile not found" -ForegroundColor Yellow
				$allScriptsExecuted = $false
			}
		}
		
		if (-not $allScriptsExecuted) {
			Write-Log "  Some IFRS9 scripts failed to execute properly" -ForegroundColor Red
			# Don't return false yet, continue with other components
		}

		# Verify critical components
		$command = $Connection.CreateCommand()
		$command.CommandText = @"
		SELECT 
			CASE WHEN EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'IFRS9') THEN 1 ELSE 0 END AS SchemaExists,
			(SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID('IFRS9')) AS ProcedureCount,
			CASE WHEN EXISTS(SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteProcessIFRS9' AND schema_id = SCHEMA_ID('IFRS9')) THEN 1 ELSE 0 END AS MainProcExists
"@
		$reader = $command.ExecuteReader()
		$criticalComponentsMissing = $true
		
		if ($reader.Read()) {
			$schemaExists = [bool]$reader["SchemaExists"]
			$procedureCount = [int]$reader["ProcedureCount"]
			$mainProcExists = [bool]$reader["MainProcExists"]
			$criticalComponentsMissing = (-not $schemaExists) -or (-not $mainProcExists)
			
			Write-Log "  IFRS9 Schema exists: $schemaExists" -ForegroundColor $(if ($schemaExists) { 'Green' } else { 'Red' })
			Write-Log "  IFRS9 Procedure count: $procedureCount" -ForegroundColor $(if ($procedureCount -gt 0) { 'Green' } else { 'Yellow' })
			Write-Log "  IFRS9 Main procedure exists: $mainProcExists" -ForegroundColor $(if ($mainProcExists) { 'Green' } else { 'Red' })
		}
		$reader.Close()
		
		if ($criticalComponentsMissing) {
			Write-Log "  CRITICAL IFRS9 components are missing - cannot continue with IFRS9 deployment" -ForegroundColor Red
			return $false
		}
		
		# 3. Deploy integration with Production ETL
		$integrationScript = Join-Path -Path $ifrs9Path -ChildPath "usp_ExecuteProductionETLWithIFRS9.sql"
		if (Test-Path $integrationScript) {
			Execute-Script -Connection $Connection -ScriptPath $integrationScript -Variables @{
				LogPath = $LogPath
				DataPath = $DataPath
				Database = $DatabaseName
			}
		} else {
			Write-Log "  WARNING: IFRS9 integration script not found" -ForegroundColor Yellow
		}
		
		# 4. Create IFRS9 SQL Agent Job
		$jobScript = Join-Path -Path $ifrs9Path -ChildPath "SQLAgent_IFRS9ProcessJob.sql"
		if (Test-Path $jobScript) {
			Execute-Script -Connection $Connection -ScriptPath $jobScript -Variables @{
				LogPath = $LogPath
				DataPath = $DataPath
				NotificationEmail = $NotificationEmail
				Database = $DatabaseName
			}
		} else {
			Write-Log "  WARNING: IFRS9 SQL Agent job script not found" -ForegroundColor Yellow
		}
		
		# 5. Configure IFRS9 settings
		$command = $Connection.CreateCommand()
		$command.CommandText = @"
		-- Add IFRS9 configuration settings if Config schema and procedure exist
		IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Config') AND
		EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_SetConfigValue' AND schema_id = SCHEMA_ID('Config'))
		BEGIN
			-- IFRS9 core settings
			EXEC Config.usp_SetConfigValue 
				@ConfigKey = 'IFRS9_Enabled', 
				@ConfigValue = 'True',
				@ConfigArea = 'IFRS9',
				@ConfigGroup = 'Core',
				@Description = 'Enable IFRS9 processing';
				
			-- IFRS9 model parameters
			EXEC Config.usp_SetConfigValue 
				@ConfigKey = 'IFRS9_ModelVersion', 
				@ConfigValue = '1.0',
				@ConfigArea = 'IFRS9',
				@ConfigGroup = 'ModelParameters',
				@Description = 'IFRS9 model version';
				
			-- IFRS9 integration settings
			EXEC Config.usp_SetConfigValue 
				@ConfigKey = 'IFRS9_IntegrateWithETL', 
				@ConfigValue = 'True',
				@ConfigArea = 'IFRS9',
				@ConfigGroup = 'Integration',
				@Description = 'Whether to include IFRS9 in the main ETL pipeline';
		END
"@
		$command.ExecuteNonQuery() | Out-Null
		return $true
	}
	catch {
		Write-Log "Error deploying IFRS9 components: $_" -ForegroundColor Red
		return $false
	}
}
#endregion

#region Test-IFRS9Components()
# Function to test IFRS9 components
function Test-IFRS9Components {
	param (
		[System.Data.SqlClient.SqlConnection]$Connection
	)
	
	Write-Log "`nTesting IFRS9 components..." -ForegroundColor Yellow
	
	try {
		# Get correct database name
		$databaseName = $Connection.Database
		Write-Log "  Using database: $databaseName" -ForegroundColor Cyan
		
		# Reset context explicitly 
		$resetCmd = $Connection.CreateCommand()
		$resetCmd.CommandText = "USE [$databaseName]"
		$resetCmd.ExecuteNonQuery() | Out-Null
		
		# Check connection is really on the right database
		$checkCmd = $Connection.CreateCommand()
		$checkCmd.CommandText = "SELECT DB_NAME() AS CurrentDB" 
		$currentDB = $checkCmd.ExecuteScalar()
		Write-Log "  Confirmed current database context: $currentDB" -ForegroundColor Cyan
		
		# Get schema components
		$command = $Connection.CreateCommand()
		$command.CommandText = @"
		USE [$databaseName];  -- Explicit database context
		SELECT 
			CASE WHEN EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'IFRS9') THEN 1 ELSE 0 END AS SchemaExists,
			(SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID('IFRS9')) AS ProcedureCount,
			(SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('IFRS9')) AS TableCount,
			CASE WHEN EXISTS(SELECT 1 FROM sys.procedures WHERE name = 'usp_ExecuteProcessIFRS9' AND schema_id = SCHEMA_ID('IFRS9')) THEN 1 ELSE 0 END AS MainProcExists
"@
		
		$reader = $command.ExecuteReader()
		if ($reader.Read()) {
			$schemaExists = [bool]$reader["SchemaExists"]
			$procedureCount = [int]$reader["ProcedureCount"]
			$tableCount = [int]$reader["TableCount"]
			$mainProcExists = [bool]$reader["MainProcExists"]
			$jobExists = [bool]$reader["JobExists"]
			
			Write-Log "IFRS9 Components Status:" -ForegroundColor Cyan
			Write-Log "  Schema exists: $(if ($schemaExists) { 'Yes' } else { 'No' })" -ForegroundColor $(if ($schemaExists) { 'Green' } else { 'Red' })
			Write-Log "  Procedure count: $procedureCount" -ForegroundColor $(if ($procedureCount -gt 0) { 'Green' } else { 'Yellow' })
			Write-Log "  Table count: $tableCount" -ForegroundColor $(if ($tableCount -gt 0) { 'Green' } else { 'Yellow' })
			Write-Log "  Main procedure exists: $(if ($mainProcExists) { 'Yes' } else { 'No' })" -ForegroundColor $(if ($mainProcExists) { 'Green' } else { 'Red' })
			Write-Log "  SQL Agent job exists: $(if ($jobExists) { 'Yes' } else { 'No' })" -ForegroundColor $(if ($jobExists) { 'Green' } else { 'Yellow' })
			
			$reader.Close()
			
			# Run a simple test of IFRS9 components if everything exists
			if ($schemaExists -and $mainProcExists) {
				Write-Log "`nRunning basic IFRS9 test..." -ForegroundColor Yellow
				
				$command.CommandText = @"
				BEGIN TRY
					-- Test IFRS9 execution without affecting data
					DECLARE @TestBatchID UNIQUEIDENTIFIER = NEWID();
					DECLARE @TestDate DATE = GETDATE();
					
					-- Test classification procedure for financial instruments
					IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_ClassifyFinancialInstruments' AND schema_id = SCHEMA_ID('IFRS9'))
					BEGIN
						EXEC IFRS9.usp_ClassifyFinancialInstruments @AsOfDate = @TestDate, @BatchID = @TestBatchID, @TestMode = 1;
						SELECT 'Financial instrument classification test successful' AS TestResult;
					END
					ELSE
					BEGIN
						SELECT 'Financial instrument classification procedure not found' AS TestResult;
					END
				END TRY
				BEGIN CATCH
					SELECT 'IFRS9 test failed: ' + ERROR_MESSAGE() AS TestResult;
				END CATCH
"@
				$testReader = $command.ExecuteReader()
				if ($testReader.Read()) {
					Write-Log "  $($testReader["TestResult"])" -ForegroundColor $(if ($testReader["TestResult"].ToString().Contains("successful")) { 'Green' } else { 'Yellow' })
				}
				$testReader.Close()
			}
			
			return $schemaExists -and $mainProcExists
		}
		else {
			Write-Log "  Unable to retrieve IFRS9 component information" -ForegroundColor Red
			$reader.Close()
			return $false
		}
		
		# Execute IFRS9 tests if enabled
		if ($TestIFRS9 -and $ifrs9Exists) {
			$ifrs9TestScript = Join-Path -Path $ScriptsPath -ChildPath "..\Tests\IFRS9IntegrationTest.sql"
			if (Test-Path $ifrs9TestScript) {
				Write-Log "`nRunning IFRS9 integration tests..." -ForegroundColor Yellow
				Execute-Script -Connection $connection -ScriptPath $ifrs9TestScript -Variables @{ 
					LogPath = $LogPath 
					DataPath = $TestDataPath
				}
			}
			else {
				Write-Log "`nIFRS9 integration test script not found: $ifrs9TestScript" -ForegroundColor Yellow
			}
		}
	}
	catch {
		Write-Log "Error testing IFRS9 components: $_" -ForegroundColor Red
		return $false
	}
}
#endregion

#region Run-IFRS9Process()
# Function to run the IFRS9 process
function Run-IFRS9Process {
	param (
		[System.Data.SqlClient.SqlConnection]$Connection
	)
	
	Write-Log "`nRunning initial IFRS9 process..." -ForegroundColor Yellow
	
	try {
		# Get correct database name
		$databaseName = 'FinancialRiskDB'
		
		# Reset context explicitly
		$resetCmd = $Connection.CreateCommand()
		$resetCmd.CommandText = "USE [$databaseName]"
		$resetCmd.ExecuteNonQuery() | Out-Null
		
		# Verify database context
		$checkCmd = $Connection.CreateCommand()
		$checkCmd.CommandText = "SELECT DB_NAME() AS CurrentDB"
		$currentDB = $checkCmd.ExecuteScalar()
		Write-Log "  Confirmed current database context: $currentDB" -ForegroundColor Cyan
		

		
		# Run debug queries to track issues
		$debugCmd = $Connection.CreateCommand()
		$debugCmd.CommandText = @"
		
		-- Print statements for major steps (will appear in SQL logs)
		PRINT 'DEBUG: Starting IFRS9 debug process';
		
		-- Test if each stored procedure exists
		SELECT 
			'IFRS9 procedures status:' AS Info,
			OBJECT_ID('IFRS9.usp_ClassifyFinancialInstruments') AS ClassifyExists,
			OBJECT_ID('IFRS9.usp_ClassifyECLStaging') AS StagingExists,
			OBJECT_ID('IFRS9.usp_CalculateECLParameters') AS ParamsExists,
			OBJECT_ID('IFRS9.usp_CalculateECL') AS CalcExists,
			OBJECT_ID('IFRS9.usp_ExecuteProcessIFRS9') AS MainExists;
			
		-- Test AccountSK column existence in each table
		SELECT 
			t.name AS TableName, 
			c.name AS ColumnName, 
			'Column Exists' AS Status
		FROM 
			sys.tables t
			INNER JOIN sys.columns c ON t.object_id = c.object_id
		WHERE 
			t.schema_id = SCHEMA_ID('IFRS9')
			AND c.name = 'AccountSK'
		ORDER BY t.name;
		
		-- Try executing part of the main procedure with simplified parameters
		BEGIN TRY
			DECLARE @TestBatchID UNIQUEIDENTIFIER = NEWID();
			DECLARE @TestDate DATE = GETDATE();
			
			-- Test only classification procedure with trace flags
			PRINT 'DEBUG: Running financial instrument classification';
			EXEC IFRS9.usp_ClassifyFinancialInstruments 
				@AsOfDate = @TestDate, 
				@BatchID = @TestBatchID,
				@TestMode = 1;
				
			PRINT 'DEBUG: Classification successful';
		END TRY
		BEGIN CATCH
			PRINT 'DEBUG ERROR in classification: ' + ERROR_MESSAGE();
		END CATCH
"@
		
		$debugReader = $debugCmd.ExecuteReader()
		Write-Log "  Running diagnostic queries..." -ForegroundColor Yellow
		
		while ($debugReader.Read()) {
			# Handle multiple result sets
			for ($i = 0; $i -lt $debugReader.FieldCount; $i++) {
				$columnName = $debugReader.GetName($i)
				$value = $debugReader.GetValue($i)
				Write-Log "    $columnName : $value" -ForegroundColor Cyan
			}
		}
		$debugReader.Close()



		# Use explicit database context in the SQL
		$command = $Connection.CreateCommand()
		$command.CommandTimeout = 300  # 5 minutes timeout
		$command.CommandText = @"

		BEGIN TRY
			DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
			DECLARE @AsOfDate DATE = GETDATE();
			
			-- Run IFRS9 process with sample data using fully qualified name
			EXEC [$databaseName].IFRS9.usp_ExecuteProcessIFRS9 
				@AsOfDate = @AsOfDate,
				@PreviousDate = NULL,
				@BatchID = @BatchID,
				@IncludeAllScenarios = 1,
				@CalculateMovement = 1;
				
			SELECT 'IFRS9 process executed successfully. BatchID: ' + CONVERT(VARCHAR(36), @BatchID) AS Result;
		END TRY
		BEGIN CATCH
			SELECT 'IFRS9 process failed: ' + ERROR_MESSAGE() AS Result;
		END CATCH
"@
		$reader = $command.ExecuteReader()
		if ($reader.Read()) {
			$result = $reader["Result"].ToString()
			if ($result.Contains("executed successfully")) {
				Write-Log "  $result" -ForegroundColor Green
				$reader.Close()
				return $true
			}
			else {
				Write-Log "  $result" -ForegroundColor Red
				$reader.Close()
				return $false
			}
		}
		else {
			Write-Log "  No result returned from IFRS9 process execution" -ForegroundColor Red
			$reader.Close()
			return $false
		}
	}
	catch {
		Write-Log "Error running IFRS9 process: $_" -ForegroundColor Red
		return $false
	}
}
#endregion

#region Main()
# IFRS9 deployment
if (-not $IsTest -and $DeployIFRS9 -and $baseETLSuccess) {
	Write-Log "`nStep B: Deploying IFRS9 components..." -ForegroundColor Yellow
	
	# Create a new connection for IFRS9 deployment
	$ifrs9Connection = New-Object System.Data.SqlClient.SqlConnection("Server=$ServerInstance;Database=$Database;Integrated Security=True;")
	$ifrs9Connection.Open()
	
	try {
		# Enable info messages for this connection
		$ifrs9Connection.FireInfoMessageEventOnUserErrors = $true
		try {
			$ifrs9Connection.add_InfoMessage({
				param ($sender, $event)
				Write-Log "SQL Server: $($event.Message)" -ForegroundColor Cyan
			})
		} catch { }
		
		# Create IFRS9 schema if it doesn't exist
		$command = $ifrs9Connection.CreateCommand()
		$command.CommandText = @"
		IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'IFRS9')
		BEGIN
			EXEC('CREATE SCHEMA IFRS9');
			PRINT 'Created IFRS9 schema';
		END
"@
		$command.ExecuteNonQuery() | Out-Null
		
		# Deploy IFRS9 components
		if (Deploy-IFRS9Components -Connection $ifrs9Connection -ScriptsPath $ScriptsPath -LogPath $LogPath -DataPath $DataPath -DatabaseName $Database) {
			Write-Log "IFRS9 components deployed successfully." -ForegroundColor Green
			
			# Verify and test IFRS9 components
			Test-IFRS9Components -Connection $ifrs9Connection
			
			# Run IFRS9 process if requested
			if ($RunIFRS9ETL) {
				if (Run-IFRS9Process -Connection $ifrs9Connection) {
					Write-Log "IFRS9 process executed successfully." -ForegroundColor Green
				}
				else {
					Write-Log "IFRS9 process execution had issues." -ForegroundColor Yellow
				}
			}
		}
		else {
			Write-Log "IFRS9 component deployment had issues." -ForegroundColor Yellow
		}
	}
	catch {
		Write-Log "Error during IFRS9 deployment: $_" -ForegroundColor Red
	}
	finally {
		# Close the IFRS9 connection
		$ifrs9Connection.Close()
	}
}
elseif (-not $IsTest -and $DeployIFRS9) {
	Write-Log "`nSkipping IFRS9 deployment because base ETL pipeline deployment failed." -ForegroundColor Yellow
}

#endregion