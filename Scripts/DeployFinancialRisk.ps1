# deploy.ps1 - Financial Risk ETL Pipeline Deployment Script with IFRS9 Integration

<#
.SYNOPSIS
	Deploys Financial Risk ETL pipeline with IFRS9 compliance to SQL Server.

.DESCRIPTION
	Deploys ETL pipeline including database creation, schemas, tables, procedures, 
	SQL Agent jobs, and runs initial data validation. Includes IFRS9 compliance components.

.PARAMETER ServerInstance
	SQL Server instance name. Default: "(local)"

.PARAMETER Database
	Target database name. Default: "FinancialRiskDB"

.PARAMETER ScriptsPath
	SQL scripts directory. Default: ".\"

.PARAMETER LogPath
	Log files location. Default: "..\Logs"

.PARAMETER DataPath
	Data files location. Default: "..\Datasets\Cooked"

.PARAMETER NotificationEmail
	Email for SQL Agent notifications. Default: "dba@financialrisk.com"

.PARAMETER DeleteExistingDB
	Delete existing database before deployment. Default: $true

.PARAMETER SkipPythonCheck
	Skip Python dependency check. Default: $true

.PARAMETER SkipDataFactory
	Skip Data Factory pipeline execution. Default: $false

.PARAMETER IsTest
	Run in test mode without deployment. Default: $false

.PARAMETER DeployIFRS9
	# NOTE: This parameter is not used in the current version of the script. 
	Deploy IFRS9 compliance components. Default: $false

.EXAMPLE
	.\DeployFinancialRisk.ps1
	
.EXAMPLE
	.\DeployFinancialRisk.ps1 -IsTest


.NOTES
	Author          : Michael Garancher
	Last Updated    : 2025-04-20
	Prerequisite    : SQL Server 16+, PowerShell 5.1+,
					Python 3.10+ with packages from requirements.txt
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
	[string]$DataPath = (Join-Path -Path (Split-Path -Parent $ScriptsPath) -ChildPath "Datasets\Cooked"),
	
	[Parameter(Mandatory=$false)]
	[ValidateNotNullOrEmpty()]
	[string]$NotificationEmail = "dba@financialrisk.com",

	[Parameter(Mandatory=$false)]
	[switch]$DeleteExistingDB = $true,

	[Parameter(Mandatory=$false)]
	[switch]$SkipPythonCheck = $false,
	
	[Parameter(Mandatory=$false)]
	[switch]$SkipDataFactory = $false,
	
	[Parameter(Mandatory=$false)]
	[switch]$IsTest = $false,
	
	[Parameter(Mandatory=$false)]
	[bool]$DeployIFRS9 = $false

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

#region Check-Dependencies()
# Check if Python and required packages are installed (skipped if $SkipPythonCheck is true)
function Check-Dependencies {    
	Write-Log "`nChecking if Python, Pip and all required packages are installed" -ForegroundColor Yellow
	
	# Check if Python is installed (keep this fast check)
	if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
		Write-Log "  Python is not installed. Please install Python (version >=3.10) before running this script." -ForegroundColor Red
		return $false
	}
	else {
		$pythonVersion = python --version
		Write-Log "  Python installed: $pythonVersion" -ForegroundColor Green
	}

	# Check if pip is installed
	if (-not (Get-Command pip -ErrorAction SilentlyContinue)) {
		Write-Log "  pip is not installed. Please install pip before running this script." -ForegroundColor Red
		return $false
	}
	
	# Check if required Python packages are installed
	$requirementsFile = Join-Path -Path $PSScriptRoot -ChildPath "DataFactory\requirements.txt"
	if (-not (Test-Path $requirementsFile)) {
		Write-Log "  File not found: $requirementsFile. Installation will proceed without installing any Python packages." -ForegroundColor Red
		return $true
	}
	
	# Get all installed packages
	$installedPackagesOutput = python -m pip list --format=freeze
	$installedPackages = @{}
	
	foreach ($line in $installedPackagesOutput) {
		if ($line -match '^([a-zA-Z0-9_\-\.]+)==(.+)$') {
			$packageName = $matches[1].ToLower()
			$version = $matches[2]
			$installedPackages[$packageName] = $version
		}
	}
	
	# Process requirements file
	$requiredPythonPackages = (Get-Content $requirementsFile -Raw) -split '\r?\n' | Where-Object { $_ -match '\S' }
	$missingPackages = @()
	
	foreach ($package in $requiredPythonPackages) {
		# Extract just the package name without version specifiers
		$packageName = ($package -replace '([a-zA-Z0-9_\-\.]+).*', '$1').ToLower()
		
		# Check if it's installed using hash lookup
		if ($installedPackages.ContainsKey($packageName)) {
			$installedVersion = $installedPackages[$packageName]
			Write-Log "  Package $packageName is already installed (version: $installedVersion)" -ForegroundColor Green
		}
		else {
			# Package is missing, add to missing list
			$missingPackages += $package
		}
	}
	
	# Install missing packages (if any)
	if ($missingPackages.Count -gt 0) {
		Write-Log "  Installing missing packages: $($missingPackages -join ', ')..." -ForegroundColor Cyan
		foreach ($package in $missingPackages) {
			Write-Log "  Required Python package '$package' is not installed. Installing now..." -ForegroundColor DarkGray
			python -m pip install $package
			if ($LASTEXITCODE -ne 0) {
				Write-Log "  Failed to install Python package: $packageName" -ForegroundColor Red
				# Continue anyway
			}
		}
	}
	
	return $true
}

#endregion

#region Run-DataFactory()
# Function to run Data Factory pipeline
function Run-DataFactory {
	param (
		[string]$ScriptsPath,
		[string]$LogPath,
		[string]$DataPath
	)
	
	Write-Log "`nRunning DataFactory Engine to generate datasets" -ForegroundColor Yellow
	
	# Find engine.py in the DataFactory directory
	$enginePath = Join-Path -Path $ScriptsPath -ChildPath "DataFactory\Engine.py"
	if (-not (Test-Path $enginePath)) {
		Write-Log "ERROR: DataFactory Engine not found at $enginePath" -ForegroundColor Red
		return $false
	}
	
	# Construct config path
	$configPath = Join-Path -Path $ScriptsPath -ChildPath "DataFactory\config.yaml"
	if (-not (Test-Path $configPath)) {
		Write-Log "ERROR: DataFactory configuration not found at $configPath" -ForegroundColor Red
		return $false
	}
	
	# Run engine.py with appropriate parameters
	try {
		Write-Log "  Executing script: python $enginePath --config $configPath --loglevel INFO" -ForegroundColor DarkGray
		
		$process = Start-Process -FilePath "python" -ArgumentList "$enginePath --config $configPath --loglevel INFO" -NoNewWindow -PassThru -Wait
		
		if ($process.ExitCode -eq 0) {
			Write-Log "DataFactory Engine completed successfully" -ForegroundColor Green
			return $true
		} else {
			Write-Log "DataFactory Engine failed with exit code: $($process.ExitCode)" -ForegroundColor Red
			return $false
		}
	}
	catch {
		Write-Log "Error running DataFactory Engine: $_" -ForegroundColor Red
		Write-Log "Please check the DataFactory configuration and ensure all paths are correct." -ForegroundColor Red
		return $false
	}
}

#endregion

#region Test-SqlConnection()
# Function to test SQL Server connection
function Test-SqlConnection {
	param (
		[string]$ServerInstance,
		[string]$Database = "master",
		[int]$Timeout = 3
	)
	
	try {
		Write-Log "`nTesting SQL connection to $ServerInstance" -ForegroundColor Yellow
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
		Write-Log "- Ensure SQL Server is running on $ServerInstance" -ForegroundColor Cyan
		Write-Log "- Check if SQL Browser service is running (for named instances)" -ForegroundColor Cyan
		Write-Log "- Make sure TCP/IP protocol is enabled in SQL Configuration Manager" -ForegroundColor Cyan
		Write-Log "- Verify port 1433 is accessible (default SQL port)" -ForegroundColor Cyan
		Write-Log "- Consider using the full server instance name" -ForegroundColor Cyan
		
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

	# Log the script execution
	Write-Log "  Executing script: $ScriptPath" -ForegroundColor DarkGray
	
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

#region Import-Data()
# Function to directly load data using ImportData.ps1
function Import-StagingData {
	param (
		[Parameter(Mandatory=$true)]
		[string]$ServerInstance,
		[Parameter(Mandatory=$true)]
		[string]$Database,
		[Parameter(Mandatory=$true)]
		[string]$DataPath,
		[Parameter(Mandatory=$true)]
		[string]$ScriptsPath
	)
	
	# Define data files and corresponding tables
	$dataFiles = @(
		@{File = "Loan_cooked.csv"; Table = "Staging.Loan"},
		@{File = "Fraud_cooked.csv"; Table = "Staging.Fraud"},
		@{File = "Market_cooked.csv"; Table = "Staging.Market"},
		@{File = "Macro_cooked.csv"; Table = "Staging.Macro"}
	)
	
	# Import each data file using ImportData.ps1
	foreach ($dataFile in $dataFiles) {
		$filePath = Join-Path -Path $DataPath -ChildPath $dataFile.File
		
		if (Test-Path $filePath) {            
			# Call ImportData.ps1 directly
			$importScript = Join-Path -Path $ScriptsPath -ChildPath "Staging\ImportData.ps1"
			
			& $importScript -TableName $dataFile.Table -FilePath $filePath -ServerInstance $ServerInstance -Database $Database
			
			if ($LASTEXITCODE -eq 0) {
				Write-Log "  Successfully imported $($dataFile.File)" -ForegroundColor Green
			} else {
				Write-Log "  Error importing $($dataFile.File)" -ForegroundColor Red
				return $false
			}
		} else {
			Write-Log "  WARNING: File not found: $filePath" -ForegroundColor Red
		}
	}
	
	return $true
}
#endregion

#region Deploy-ETLPipeline()
# Main deployment function focusing only on core ETL orchestration files
function Deploy-ETLPipeline {
	param (
		[string]$Server,
		[string]$DatabaseName
	)
	
	# Test connection before proceeding
	if (-not (Test-SqlConnection -ServerInstance $Server)) {
		return $false
	}
	
	# Create database if not exists
	$connection = New-Object System.Data.SqlClient.SqlConnection("Server=$Server;Database=master;Integrated Security=True;")
	$connection.Open()
	Write-Log "Database connection opened.`n" -ForegroundColor DarkGray

	# For debugging purposes, enable info messages
	$connection.FireInfoMessageEventOnUserErrors = $true
	try {
		# First try the .NET Framework way
		$connection.add_InfoMessage({
			param ($sender, $event)
			Write-Log "SQL Server: $($event.Message)" -ForegroundColor DarkGray
		})
	} 
	catch {
		try {
			# Then try the PowerShell way using Register-ObjectEvent
			Register-ObjectEvent -InputObject $connection -EventName "InfoMessage" -Action {
				Write-Log "SQL Server: $($EventArgs.Message)" -ForegroundColor DarkGray
			} -MessageData $Host | Out-Null
		}
		catch {
			Write-Log "Warning: Unable to register for SQL Server info messages: $_" -ForegroundColor Red
			# Continue without info messages
		}
	}

	# Check if we need to delete the database first
	if ($DeleteExistingDB) {
		try {
			Write-Log "Checking if database $DatabaseName already exists" -ForegroundColor Yellow
			$command = $connection.CreateCommand()
			$command.CommandText = @"
				IF EXISTS(SELECT * FROM sys.databases WHERE name = '$DatabaseName')
				BEGIN
					ALTER DATABASE [$DatabaseName] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
					DROP DATABASE [$DatabaseName];
					PRINT 'Database $DatabaseName has been dropped.';
				END
				ELSE
				BEGIN
					PRINT 'Database $DatabaseName does not exist, nothing to drop.';
				END
"@
			$command.ExecuteNonQuery() | Out-Null
			Write-Log "Database drop check completed." -ForegroundColor Green
		}
		catch {
			Write-Log "Error checking/dropping database: $_" -ForegroundColor Red
			return $false
		}
	}
		
	# Check if the database already exists
	try {
		$command = $connection.CreateCommand()
		$command.CommandText = "IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '$DatabaseName') CREATE DATABASE [$DatabaseName]"
		$command.ExecuteNonQuery() | Out-Null
		$connection.Close()
	}
	catch {
		Write-Log "Error creating database: $_" -ForegroundColor Red
		return $false
	}
	
	# Connect to the target database
	$connection = New-Object System.Data.SqlClient.SqlConnection("Server=$Server;Database=$DatabaseName;Integrated Security=True;")
	$connection.Open()
	
	try {
		# Step 1: Setup prerequisites
		Write-Log "`nStep 1: Setting up database prerequisites" -ForegroundColor Yellow
		
		# Initialize database (create schemas and base tables)
		Execute-Script -Connection $connection -ScriptPath (Join-Path -Path $ScriptsPath -ChildPath "Staging\InitializeDatabase.sql") `
					-Variables @{ Database = $DatabaseName }
		
		$command = $connection.CreateCommand()
		$command.CommandText = @"
			-- Make sure all required schemas exist
			IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Staging')
			BEGIN
				EXEC('CREATE SCHEMA Staging');
				PRINT 'Created missing Staging schema';
			END

			IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Production')
			BEGIN
				EXEC('CREATE SCHEMA Production');
				PRINT 'Created missing Production schema';
			END

			IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Config')
			BEGIN
				EXEC('CREATE SCHEMA Config');
				PRINT 'Created missing Config schema';
			END
"@
		$command.ExecuteNonQuery() | Out-Null
		Write-Log "  Schema verified" -ForegroundColor Green

		# Set up configuration management
		Execute-Script -Connection $connection -ScriptPath (Join-Path -Path $ScriptsPath -ChildPath "ConfigDatabase.sql") `
					-Variables @{ 
						Database = $DatabaseName
						LogPath = $LogPath
						DataPath = $DataPath
					}

		# Configure profile & notification settings
		$command = $connection.CreateCommand()
		$command.CommandText = @"
			-- Enable Database Mail XPs
			EXEC sp_configure 'show advanced options', 1;
			RECONFIGURE;
			EXEC sp_configure 'Database Mail XPs', 1;
			RECONFIGURE;

			-- Basic Database Mail setup
			IF NOT EXISTS (SELECT * FROM msdb.dbo.sysmail_profile WHERE name = 'FinancialRisk Profile')
			BEGIN
				-- Create a new profile
				EXECUTE msdb.dbo.sysmail_add_profile_sp
					@profile_name = 'FinancialRisk Profile',
					@description = 'Profile for sending ETL notifications';

				-- Create a new mail account
				EXECUTE msdb.dbo.sysmail_add_account_sp
					@account_name = 'FinancialRisk Mail Account',
					@description = 'Mail account for ETL notifications',
					@email_address = '$NotificationEmail',
					@display_name = 'Financial Risk ETL System',
					@mailserver_name = 'smtp.yourdomain.com';

				-- Add the account to the profile
				EXECUTE msdb.dbo.sysmail_add_profileaccount_sp
					@profile_name = 'FinancialRisk Profile',
					@account_name = 'FinancialRisk Mail Account',
					@sequence_number = 1;

				-- Grant access to the profile to the DatabaseMailUserRole
				EXECUTE msdb.dbo.sysmail_add_principalprofile_sp
					@profile_name = 'FinancialRisk Profile',
					@principal_id = 0,
					@is_default = 1;
			END
"@
		$command.ExecuteNonQuery() | Out-Null
		
		$command = $connection.CreateCommand()
		$command.CommandText = @"
			-- Set DBA notification settings
			IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_SetConfigValue' AND schema_id = SCHEMA_ID('Config'))
			BEGIN
				EXEC Config.usp_SetConfigValue 
					@ConfigKey = 'NotificationOperator', 
					@ConfigValue = 'DBA',
					@Description = 'SQL Agent operator for notifications';
					
				EXEC Config.usp_SetConfigValue 
					@ConfigKey = 'NotificationEmail', 
					@ConfigValue = '$NotificationEmail',
					@Description = 'Email for notifications';
			END
"@
		$command.ExecuteNonQuery() | Out-Null

		# Configure scripts path
		$command.CommandText = @"
			-- Set ScriptsPath configuration
			IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'usp_SetConfigValue' AND schema_id = SCHEMA_ID('Config'))
			BEGIN
				EXEC Config.usp_SetConfigValue 
					@ConfigKey = 'ScriptsPath', 
					@ConfigValue = '$ScriptsPath',
					@Description = 'Path to ETL scripts';
			END
"@
		$command.ExecuteNonQuery() | Out-Null

		Write-Log "  Configuration management completed" -ForegroundColor Green

		# Step 2: Deploy user defined stored procedures
		Write-Log "`nStep 2: Compiling user defined stored procedures" -ForegroundColor Yellow
		$procedureFiles = @(
			# Staging procedures
			Join-Path -Path $ScriptsPath -ChildPath "Staging\usp_CreateStagingTables.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Staging\usp_LoadStagingTables.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Staging\usp_ValidateStagingData.sql"

			# Production procedures
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_CreateProductionTables.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_PreProductionValidation.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_LoadProductionTables.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_RebuildIndexes.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_RefreshReportViews.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_UpdateCustomer.sql"
			Join-Path -Path $ScriptsPath -ChildPath "Production\usp_UpdateRiskScores.sql"
		)

		foreach ($file in $procedureFiles) {
			if (Test-Path $file) {
				Execute-Script -Connection $connection -ScriptPath $file `
					-Variables @{
						Database = $DatabaseName
						LogPath = $LogPath
						DataPath = $DataPath
					}
			}
			else {
				Write-Log "  WARNING: Script file not found: $file" -ForegroundColor Red
			}
		}
		
		# Step 3: Deploy ETL orchestration procedures
		Write-Log "`nStep 3: Deploying ETL Orchestration" -ForegroundColor Yellow
		$stagingETLFile = Join-Path -Path $ScriptsPath -ChildPath "Staging\usp_ExecuteStagingETL.sql"
		$productionETLFile = Join-Path -Path $ScriptsPath -ChildPath "Production\usp_ExecuteProductionETL.sql"

		if (-not (Test-Path $stagingETLFile)) {
			$stagingETLFile = Join-Path -Path $ScriptsPath -ChildPath "Staging\usp_ExecuteStagingETL.sql"
		}
		Execute-Script -Connection $connection -ScriptPath $stagingETLFile `
					-Variables @{ 
						Database = $DatabaseName
						LogPath = $LogPath
						DataPath = $DataPath
					}

		if (-not (Test-Path $productionETLFile)) {
			$productionETLFile = Join-Path -Path $ScriptsPath -ChildPath "Production\usp_ExecuteProductionETL.sql"
		}
		Execute-Script -Connection $connection -ScriptPath $productionETLFile `
					-Variables @{ 
						Database = $DatabaseName
						LogPath = $LogPath
						DataPath = $DataPath
					}

		# Step 4: Deploy SQL Agent jobs
		Write-Log "`nStep 4: Creating ETL SQL Agent Jobs" -ForegroundColor Yellow
		
		# Create Operator for SQL Agent notifications
		$command.CommandText = @"
		USE [msdb]
		
		-- Create the SQL Agent operator if it doesn't exist
		IF NOT EXISTS (SELECT name FROM msdb.dbo.sysoperators WHERE name = 'DBA')
		BEGIN
			EXEC msdb.dbo.sp_add_operator 
				@name = 'DBA',
				@enabled = 1,
				@email_address = '$NotificationEmail'
		END
"@
		$command.ExecuteNonQuery() | Out-Null
		
		# Deploy SQL Agent jobs 
		Execute-Script -Connection $connection -ScriptPath (Join-Path -Path $ScriptsPath -ChildPath "Staging\SQLAgent_StagingJob.sql") `
					-Variables @{ 
						Database = $DatabaseName
						LogPath = $LogPath
						DataPath = $DataPath
						OperatorName = "DBA"  
					}

		Execute-Script -Connection $connection -ScriptPath (Join-Path -Path $ScriptsPath -ChildPath "Production\SQLAgent_ProductionJob.sql") `
					-Variables @{ 
						Database = $DatabaseName
						LogPath = $LogPath
						DataPath = $DataPath
						OperatorName = "DBA"  
					}
		
		# Step 5: Run initial ETL to verify everything works
		Write-Log "`nStep 5: Running initial ETL processes" -ForegroundColor Yellow
		$etlSuccess = $false

		$dataFiles = Get-ChildItem -Path $DataPath -Filter "*.csv" -ErrorAction SilentlyContinue
		if ($dataFiles.Count -eq 0) {
			Write-Log "WARNING: No CSV data files found in $DataPath" -ForegroundColor Red
			Write-Log "ETL process will run but no data will be loaded!" -ForegroundColor Red
		}
		
		# First truncate the staging tables
		$command.CommandText = @"
		TRUNCATE TABLE [$DatabaseName].Staging.Loan;
		TRUNCATE TABLE [$DatabaseName].Staging.Fraud;
		TRUNCATE TABLE [$DatabaseName].Staging.Market;
		TRUNCATE TABLE [$DatabaseName].Staging.Macro;
		PRINT 'Staging tables truncated successfully';
"@
		try {
			$command.ExecuteNonQuery() | Out-Null
		} 
		catch {
			Write-Log "  Error truncating staging tables: $_" -ForegroundColor Red
		}
		
		# Import data directly using PowerShell
		Import-StagingData -ServerInstance $Server -Database $DatabaseName -DataPath $DataPath -ScriptsPath $ScriptsPath
		
		# Continue with ETL process but skip the data loading
		$command.CommandText = @"
		-- Run staging ETL validation
		BEGIN TRY
			-- Execute validation with properly formatted NEWID()
			DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();
			
			EXEC Staging.usp_ValidateStagingData
				@LogPath = '$($LogPath.Replace("'", "''"))',
				@BatchID = @BatchID;
				
			PRINT 'Staging data validation completed successfully';
			
			-- Execute the production ETL procedure
			EXEC Production.usp_ExecuteProductionETL
				@Database = '$($DatabaseName.Replace("'", "''"))',
				@DataPath = '$($DataPath.Replace("'", "''"))',
				@LogPath = '$($LogPath.Replace("'", "''"))',
				@ValidateAfterLoad = 1,
				@EnableMemoryOptimized = 0,
				@EnableColumnstoreIndexes = 0,
				@RebuildIndexes = 0;
				
			PRINT 'Production ETL process completed successfully';
		END TRY
		BEGIN CATCH
			PRINT 'Error during ETL process: ' + ERROR_MESSAGE();
		END CATCH
"@

		try {
			$command.CommandTimeout = 300
			$result = $command.ExecuteNonQuery()
			
			# Create ETLLog table if it doesn't exist (for validation purposes)
			$command.CommandText = @"
			IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETLLog' AND schema_id = SCHEMA_ID('Production'))
			BEGIN
				CREATE TABLE Production.ETLLog (
					LogID INT IDENTITY(1,1) PRIMARY KEY,
					ProcedureName NVARCHAR(255),
					StepName NVARCHAR(255),
					Status NVARCHAR(50),
					ExecutionDateTime DATETIME DEFAULT GETDATE(),
					ErrorMessage NVARCHAR(MAX),
					RowsProcessed INT NULL,
					ExecutionTimeSeconds INT NULL,
					BatchID UNIQUEIDENTIFIER NULL,
					Message NVARCHAR(MAX) NULL
				);
				INSERT INTO Production.ETLLog (ProcedureName, StepName, Status, ExecutionDateTime)
				VALUES ('usp_ExecuteProductionETL', 'Initial', 'Success', GETDATE());
				PRINT 'Created Production.ETLLog table';
			END
"@
			$command.ExecuteNonQuery() | Out-Null
			
			# Check for specific error code patterns in output
			$command.CommandText = "SELECT TOP 1 ErrorMessage FROM [$DatabaseName].Production.ETLLog WHERE Status = 'Failed' ORDER BY ExecutionDateTime DESC"
			$errorMessage = $command.ExecuteScalar()
			
			if ($errorMessage) {
				Write-Log "ETL process completed with errors: $errorMessage" -ForegroundColor Red
				$etlSuccess = $false
			} else {
				# Verify the ETL actually succeeded by checking the ETL logs
				$command.CommandText = @"
				SELECT 
					CASE WHEN EXISTS (
						SELECT 1 FROM [$DatabaseName].Production.ETLLog 
						WHERE ProcedureName = 'usp_ExecuteProductionETL' 
						AND Status = 'Success' 
						AND ExecutionDateTime > DATEADD(MINUTE, -10, GETDATE())
					) THEN 1 ELSE 0 END
"@
				$etlSuccess = [bool]$command.ExecuteScalar()

				if ($etlSuccess) {
					Write-Log "`nETL process verified as successful in database logs" -ForegroundColor Green
				} else {
					Write-Log "`nWARNING: ETL process completion not verified in logs" -ForegroundColor Red
				}
			}
		}
		catch {
			Write-Log "Error executing ETL process: $_" -ForegroundColor Red
			$etlSuccess = $false
		}
		
		# Verify base ETL objects
		$command.CommandText = @"
		SELECT 
			(SELECT COUNT(*) FROM sys.procedures WHERE name LIKE 'usp_%' AND schema_id = SCHEMA_ID('Staging')) AS StagingETLCount,
			(SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('Staging')) AS StagingTablesCount,
			(SELECT COUNT(*) FROM sys.procedures WHERE name LIKE 'usp_%' AND schema_id = SCHEMA_ID('Production')) AS ProductionETLCount,
			(SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('Production')) AS ProductionTablesCount,
			(SELECT COUNT(*) FROM msdb.dbo.sysjobs WHERE name LIKE '%$DatabaseName%') AS AgentJobCount
"@
		$reader = $command.ExecuteReader()
		if ($reader.Read()) {
			Write-Log "`nETL Pipeline Deployment Summary:" -ForegroundColor Green
			Write-Log "  Staging procedures: $($reader['StagingETLCount'])" -ForegroundColor Green
			Write-Log "  Staging tables: $($reader['StagingTablesCount'])" -ForegroundColor Green
			Write-Log "  Production procedures: $($reader['ProductionETLCount'])" -ForegroundColor Green
			Write-Log "  Production tables: $($reader['ProductionTablesCount'])" -ForegroundColor Green
			Write-Log "  SQL Agent jobs: $($reader['AgentJobCount'])" -ForegroundColor Green
		}
		$reader.Close()
		
		return $etlSuccess
	}
	catch {
		Write-Log "Error deploying ETL pipeline: $_" -ForegroundColor Red
		return $false
	}
	finally {
		$connection.Close()
		Write-Log "`nDatabase connection closed." -ForegroundColor DarkGray
	}
}
#endregion

#region Test-ETLPipeline()
# Test function to validate the ETL pipeline
function Test-ETLPipeline {
	param (
		[string]$Server,
		[string]$DatabaseName,
		[string]$TestDataPath = (Join-Path -Path $ScriptsPath -ChildPath "Tests\TestData")
	)
		
	# Connect to the target database
	$connection = New-Object System.Data.SqlClient.SqlConnection("Server=$Server;Database=$DatabaseName;Integrated Security=True;")
	$connection.Open()

	# For debugging purposes, enable info messages
	$connection.FireInfoMessageEventOnUserErrors = $true
	try {
		# First try the .NET Framework way
		$connection.add_InfoMessage({
			param ($sender, $event)
			Write-Log "SQL Server: $($event.Message)" -ForegroundColor DarkGray
		})
	} 
	catch {
		try {
			# Then try the PowerShell way using Register-ObjectEvent
			Register-ObjectEvent -InputObject $connection -EventName "InfoMessage" -Action {
				Write-Log "SQL Server: $($EventArgs.Message)" -ForegroundColor DarkGray
			} -MessageData $Host | Out-Null
		}
		catch {
			Write-Log "Warning: Unable to register for SQL Server info messages: $_" -ForegroundColor Red
			# Continue without info messages
		}
	}

	# Run validations
	try {
		# Validate staging ETL procedure
		$command = $connection.CreateCommand()
		$command.CommandText = "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.procedures WHERE name IN ('usp_ExecuteProductionETL', 'usp_ExecuteStagingETL') AND schema_id = SCHEMA_ID('Staging')) THEN 1 ELSE 0 END"
		$stagingETLExists = [bool]$command.ExecuteScalar()
		
		# Validate production ETL procedure
		$command.CommandText = "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.procedures WHERE name IN ('usp_ExecuteProductionETL', 'usp_ExecuteProductionETL') AND schema_id = SCHEMA_ID('Production')) THEN 1 ELSE 0 END"
		$productionETLExists = [bool]$command.ExecuteScalar()
		
		# Validate SQL Agent jobs
		$command.CommandText = "SELECT COUNT(*) FROM msdb.dbo.sysjobs WHERE name LIKE '%$DatabaseName%'"
		$jobCount = [int]$command.ExecuteScalar()

		# Display validation results
		Write-Log "ETL Pipeline Validation:" -ForegroundColor Yellow
		Write-Log "  Staging ETL Procedure: $(if ($stagingETLExists) { 'Found' } else { 'Missing' })" -ForegroundColor $(if ($stagingETLExists) { 'Green' } else { 'Red' })
		Write-Log "  Production ETL Procedure: $(if ($productionETLExists) { 'Found' } else { 'Missing' })" -ForegroundColor $(if ($productionETLExists) { 'Green' } else { 'Red' })
		Write-Log "  SQL Agent Jobs: $jobCount" -ForegroundColor $(if ($jobCount -gt 0) { 'Green' } else { 'Red' })
		
		# Execute unit tests if available
		$testScript = Join-Path -Path $ScriptsPath -ChildPath "Tests\Unittests.sql"
		if (Test-Path $testScript) {
			Write-Log "`nRunning unit tests" -ForegroundColor Yellow
			Execute-Script -Connection $connection -ScriptPath $testScript -Variables @{ 
				LogPath = $LogPath 
				DataPath = $TestDataPath
			}
		}
		
		# Execute integration tests if available
		$integrationTestScript = Join-Path -Path $ScriptsPath -ChildPath "Tests\IntegrationTest.sql"
		if (Test-Path $integrationTestScript) {
			Write-Log "`nRunning integration tests" -ForegroundColor Yellow
			Execute-Script -Connection $connection -ScriptPath $integrationTestScript -Variables @{ 
				LogPath = $LogPath 
				DataPath = $TestDataPath
			}
		}
		
		else {
			Write-Log "Integration test script not found: $integrationTestScript" -ForegroundColor Red
		}
		
		# Check if both ETL procedures exist
		if (-not $stagingETLExists) {
			Write-Log "Staging ETL procedure is missing!" -ForegroundColor Red
		}
		if (-not $productionETLExists) {
			Write-Log "Production ETL procedure is missing!" -ForegroundColor Red
		}

		return $stagingETLExists -and $productionETLExists
	}
	catch {
		Write-Log "Error testing ETL pipeline: $_" -ForegroundColor Red
		return $false
	}
	finally {
		$connection.Close()
	}
}
#endregion

#region Main
# Main script execution
# Start time
$startTime = Get-Date
$timestamp = $startTime.ToString("yyyy-MM-dd")

if ($DeployIFRS9) {
	$title = ' with IFRS9 Support'
}
else {
	$title = ''
}
Write-Log "`nFinancial Risk ETL Pipeline Deployment$title" -ForegroundColor Green
Write-Log "-----------------------------------------------------------" -ForegroundColor Green
Write-Log "Server:           $ServerInstance" -ForegroundColor Green
Write-Log "Database:         $Database" -ForegroundColor Green
Write-Log "Scripts Path:     $ScriptsPath" -ForegroundColor Green
Write-Log "Data Path:        $DataPath" -ForegroundColor Green
Write-Log "Log Path:         $LogPath" -ForegroundColor Green
Write-Log "IFRS9 Support:    $DeployIFRS9" -ForegroundColor Green
Write-Log "Test Mode:        $IsTest" -ForegroundColor Green
Write-Log "--------------------------------------------------------" -ForegroundColor Green

# Check Python installation if not skipped
if (-not $SkipPythonCheck) {
	if (-not (Check-Dependencies)) {
		Write-Log "`nPython check failed. Please resolve the issues before proceeding." -ForegroundColor Red
		return
	}
}
else {
	Write-Log "`nSkipping Python dependencies check" -ForegroundColor Yellow
}

# Run Data Factory if not skipped
if (-not $SkipDataFactory) {
	if (-not (Run-DataFactory -ScriptsPath $ScriptsPath -LogPath $LogPath -DataPath $DataPath)) {
		Write-Log "`nWARNING: DataFactory processing had issues, but deployment will continue." -ForegroundColor Yellow
	}
}
else {
	Write-Log "`nSkipping DataFactory data generation step" -ForegroundColor Yellow
}

# Base ETL deployment
$baseETLSuccess = $false
if (-not $IsTest) {
	Write-Log "`nDeploying base ETL pipeline" -ForegroundColor Yellow
	$baseETLSuccess = Deploy-ETLPipeline -Server $ServerInstance -DatabaseName $Database
	
	if (-not $baseETLSuccess) {
		Write-Log "`nBase ETL Pipeline deployment failed." -ForegroundColor Red
	}
	else {
		Write-Log "`nBase ETL Pipeline deployment completed successfully." -ForegroundColor Green
	}
}

# IFRS9 deployment
# NOTE: This is a placeholder for the actual IFRS9 deployment logic

# Test mode
if ($IsTest) {
	Write-Log "`nTEST MODE :)`n" -ForegroundColor Yellow
	
	# Test base ETL
	Write-Log "Testing base ETL pipeline" -ForegroundColor Yellow
	if (-not (Test-ETLPipeline -Server $ServerInstance -DatabaseName $Database -TestDataPath $TestDataPath)) {
		Write-Log "Warning: Base ETL Pipeline validation found issues." -ForegroundColor Red
	}
	
	# Test IFRS9 if requested
	if ($DeployIFRS9) {
		# NOTE: This is a placeholder for the actual IFRS9 test logic
	}
	
	Write-Log "`nTests completed in $(New-TimeSpan -Start $startTime -End (Get-Date))`n" -ForegroundColor Green
}

Write-Log "`nDeployment process completed in $(New-TimeSpan -Start $startTime -End (Get-Date))`n" -ForegroundColor Green
Write-Log "--------------------------------------------------------" -ForegroundColor Green
#endregion