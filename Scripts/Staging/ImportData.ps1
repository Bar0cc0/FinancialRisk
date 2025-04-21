# ImportData.ps1 - Script to import CSV data into SQL Server staging tables

#Requires -Version 5.0

<#
.SYNOPSIS
    Imports CSV data into SQL Server tables with robust error handling and type conversion.
.DESCRIPTION
    Powerful CSV to SQL Server import utility that handles data type conversion,
    large files, and provides detailed logging and error handling.
.PARAMETER TableName
    The target table name (can be SchemaName.TableName format)
.PARAMETER FilePath
    The path to the CSV file to import
.PARAMETER ServerInstance
    The SQL Server instance name (default: local)
.PARAMETER Database
    The target database name
.PARAMETER BatchSize
    The number of rows to process in each batch (default: 5000)
.PARAMETER Timeout
    Timeout in seconds for bulk operations (default: 600)
.PARAMETER TruncateTable
    Whether to truncate the destination table before import (default: true)
.PARAMETER LogLevel
    Controls verbosity of output: Minimal, Normal, Verbose (default: Normal)
.EXAMPLE
    .\ImportData.ps1 -TableName "Customer" -FilePath "C:\data\customers.csv"
.EXAMPLE
    .\ImportData.ps1 -TableName "dbo.Orders" -FilePath "C:\data\orders.csv" -BatchSize 10000 -LogLevel Verbose
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    [string]$TableName,
    
    [Parameter(Mandatory=$true)]
    [string]$FilePath,
    
    [Parameter(Mandatory=$false)]
    [string]$ServerInstance = "(local)",
    
    [Parameter(Mandatory=$false)]
    [string]$Database = "FinancialRiskDB",
    
    [Parameter(Mandatory=$false)]
    [int]$BatchSize = 5000,
    
    [Parameter(Mandatory=$false)]
    [int]$Timeout = 600,
    
    [Parameter(Mandatory=$false)]
    [bool]$TruncateTable = $true,
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("Minimal", "Normal", "Verbose")]
    [string]$LogLevel = "Normal"
)

#region Helper Functions
function Write-ImportLog {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Message,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("INFO", "WARNING", "ERROR", "SUCCESS", "DEBUG")]
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $color = switch ($Level) {
        "INFO" { "DarkGray" }
        "WARNING" { "Yellow" }
        "ERROR" { "Red" }
        "SUCCESS" { "Green" }
        "DEBUG" { "Cyan" }
        default { "White" }
    }
    
    # Only write DEBUG messages if LogLevel is Verbose
    if ($Level -eq "DEBUG" -and $LogLevel -ne "Verbose") {
        return
    }
    
    # Only write INFO messages if LogLevel is not Minimal
    if ($Level -eq "INFO" -and $LogLevel -eq "Minimal") {
        return
    }
    
    Write-Host "  [$timestamp] $Level - $Message" -ForegroundColor $color
}

function Get-SchemaAndTableName {
    param (
        [Parameter(Mandatory=$true)]
        [string]$FullTableName
    )
    
    # Split the table name into schema and table parts
    if ($FullTableName -like "*.*") {
        $parts = $FullTableName -split "\."
        $schema = $parts[0]
        $table = $parts[1]
    }
    else {
        $schema = "Staging"
        $table = $FullTableName
    }
    
    return @{
        Schema = $schema
        Table = $table
        FullName = "FinancialRiskDB.$schema.$table"
    }
}

function Test-SqlConnection {
    param (
        [Parameter(Mandatory=$true)]
        [string]$ConnectionString
    )
    
    try {
        $connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
        $connection.Open()
        $connection.Close()
        return $true
    }
    catch {
        Write-ImportLog "Failed to connect to SQL Server: $_" -Level "ERROR"
        return $false
    }
}

function Get-TableSchema {
    param (
        [Parameter(Mandatory=$true)]
        [System.Data.SqlClient.SqlConnection]$Connection,
        
        [Parameter(Mandatory=$true)]
        [string]$SchemaName,
        
        [Parameter(Mandatory=$true)]
        [string]$TableName
    )
    
    $schema = @{}
    
    try {
        $command = $Connection.CreateCommand()
        $command.CommandText = @"
			SELECT 
				c.name AS ColumnName, 
				t.name AS DataType,
				c.max_length AS MaxLength,
				c.precision AS Precision,
				c.scale AS Scale,
				c.is_nullable AS IsNullable
			FROM 
				sys.columns c
			INNER JOIN 
				sys.types t ON c.user_type_id = t.user_type_id
			INNER JOIN 
				sys.tables tbl ON c.object_id = tbl.object_id
			INNER JOIN 
				sys.schemas s ON tbl.schema_id = s.schema_id
			WHERE 
				s.name = @SchemaName AND tbl.name = @TableName
			ORDER BY 
				c.column_id
"@
        $command.Parameters.AddWithValue("@SchemaName", $SchemaName) | Out-Null
        $command.Parameters.AddWithValue("@TableName", $TableName) | Out-Null
        
        $reader = $command.ExecuteReader()
        
        while ($reader.Read()) {
            $columnName = $reader["ColumnName"].ToString()
            $schema[$columnName] = @{
                DataType = $reader["DataType"].ToString()
                MaxLength = $reader["MaxLength"]
                Precision = $reader["Precision"]
                Scale = $reader["Scale"]
                IsNullable = $reader["IsNullable"]
            }
        }
        
        $reader.Close()
        
        if ($schema.Count -eq 0) {
            Write-ImportLog "No columns found for table $SchemaName.$TableName" -Level "ERROR"
            return $null
        }
        
        return $schema
    }
    catch {
        Write-ImportLog "Error retrieving table schema: $_" -Level "ERROR"
        return $null
    }
}

function Convert-CsvValue {
    param (
        [Parameter(Mandatory=$false)]
        [AllowNull()]
        [object]$Value,
        
        [Parameter(Mandatory=$true)]
        [hashtable]$ColumnSchema
    )
    
    # Handle null values
    if ([string]::IsNullOrEmpty($Value)) {
        return [DBNull]::Value
    }
    
    # Handle different data types
    try {
        switch ($ColumnSchema.DataType) {
            # Handle different numeric types
            { $_ -in 'tinyint', 'smallint', 'int', 'bigint' } {
                return [int]$Value
            }
            
            # Handle decimal types with appropriate precision/scale
            { $_ -in 'decimal', 'numeric', 'money', 'smallmoney' } {
                return [decimal]$Value
            }
            
            # Handle floating-point types
            { $_ -in 'float', 'real' } {
                return [double]$Value
            }
            
			# Handle date types
			{ $_ -in 'date', 'datetime', 'datetime2', 'smalldatetime' } {
				$dateResult = [datetime]::MinValue
				if ([DateTime]::TryParse($Value, [ref]$dateResult)) {
					return $dateResult
				}
				Write-ImportLog "Invalid date value '$Value' for column $($ColumnSchema.ColumnName)" -Level "ERROR"
				return [DBNull]::Value
			}
            
            # Handle bit fields with enhanced detection
			'bit' {
				$normalizedValue = $Value.ToString().Trim().ToLower()
				if ($normalizedValue -in @('1', 'true', 'yes', 'y', 't', 'on')) {
					return 1
				}
				elseif ($normalizedValue -in @('0', 'false', 'no', 'n', 'f', 'off')) {
					return 0
				}
				elseif ($normalizedValue -eq '') {
					return [DBNull]::Value
				}
				Write-ImportLog "Invalid bit value '$Value' for column $($ColumnSchema.ColumnName)" -Level "ERROR"
				return [DBNull]::Value
			}
            
            # Handle Unicode string types
            { $_ -in 'nchar', 'nvarchar', 'ntext' } {
                return $Value
            }
            
            # Handle GUID/uniqueidentifier
			'uniqueidentifier' {
				$guidResult = [Guid]::Empty
				if ([Guid]::TryParse($Value, [ref]$guidResult)) {
					return $guidResult
				}
				Write-ImportLog "Invalid GUID value '$Value' for column $($ColumnSchema.ColumnName)" -Level "ERROR"
				return [DBNull]::Value
			}
            
            # Default case for all other types
            default {
                return $Value
            }
        }
    }
    catch {
    Write-ImportLog "Error writing batch: $_" -Level "ERROR"
    
    # Diagnostic - find problematic rows
    Write-ImportLog "Attempting to identify problematic bit fields..." -Level "WARNING"
    
    # Find bit columns in the schema
    $bitColumns = $TableSchema.Keys | Where-Object { $TableSchema[$_].DataType -eq 'bit' }
    Write-ImportLog "Bit columns: $($bitColumns -join ', ')" -Level "WARNING"
    
    # Check each row for problematic bit values
    foreach ($row in $dataTable.Rows) {
        foreach ($column in $bitColumns) {
            $value = $row[$column]
            $valueType = if ($value -eq [DBNull]::Value) { "DBNull" } else { $value.GetType().Name }
            Write-ImportLog "Row data: Column=$column, Value='$value', Type=$valueType" -Level "WARNING"
            
            # Force bit conversion explicitly
            if ($value -ne [DBNull]::Value -and $valueType -ne "Int32") {
                Write-ImportLog "Found problematic bit value in column $column" -Level "ERROR"
            }
        }
    }
    
    $transaction.Rollback()
    Write-ImportLog "Transaction rolled back due to error" -Level "WARNING"
    throw
    }
}

function Import-CsvToSql {
    param (
        [Parameter(Mandatory=$true)]
        [string]$ConnectionString,
        
        [Parameter(Mandatory=$true)]
        [string]$Schema,
        
        [Parameter(Mandatory=$true)]
        [string]$TableName,
        
        [Parameter(Mandatory=$true)]
        [array]$CsvData,
        
        [Parameter(Mandatory=$true)]
        [hashtable]$TableSchema,
        
        [Parameter(Mandatory=$false)]
        [bool]$TruncateFirst = $true,
        
        [Parameter(Mandatory=$false)]
        [int]$BatchSize = 5000,
        
        [Parameter(Mandatory=$false)]
        [int]$Timeout = 600
    )
    
    $connection = $null
    $transaction = $null
    $bulkCopy = $null
    $totalRows = $CsvData.Count
    $processedRows = 0
    $batchCount = 0
    
    try {
        $connection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
        $connection.Open()
        
        # Start transaction
        $transaction = $connection.BeginTransaction()
        
        # Truncate table if requested
        if ($TruncateFirst) {
            $truncateCmd = New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE [$Schema].[$TableName]", $connection, $transaction)
            try {
                $truncateCmd.ExecuteNonQuery() | Out-Null
                Write-ImportLog "Table [$Schema].[$TableName] truncated successfully" -Level "INFO"
            }
            catch {
                Write-ImportLog "Error truncating table: $_" -Level "ERROR"
                throw
            }
        }
        
        # Set up bulk copy
        $bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($connection, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock, $transaction)
        $bulkCopy.DestinationTableName = "[$Schema].[$TableName]"
        $bulkCopy.BulkCopyTimeout = $Timeout
        $bulkCopy.BatchSize = $BatchSize
        
        # Get column names from the first row of CSV
        $columnNames = $CsvData[0].PSObject.Properties.Name
        
        # Set up column mappings
        foreach ($column in $columnNames) {
            if ($TableSchema.ContainsKey($column)) {
                $bulkCopy.ColumnMappings.Add($column, $column) | Out-Null
            }
            else {
                Write-ImportLog "Column '$column' does not exist in table [$Schema].[$TableName] - it will be ignored" -Level "WARNING"
            }
        }
        
        # Create data table
        $dataTable = New-Object System.Data.DataTable
        foreach ($column in $columnNames) {
            if ($TableSchema.ContainsKey($column)) {
                $dataTable.Columns.Add($column) | Out-Null
            }
        }
        
        # Process in batches
        $currentBatch = 0
        $startTime = Get-Date
        
        foreach ($row in $CsvData) {
            $dataRow = $dataTable.NewRow()
            
            foreach ($column in $columnNames) {
                if ($TableSchema.ContainsKey($column)) {
                    $dataRow[$column] = Convert-CsvValue -Value $row.$column -ColumnSchema $TableSchema[$column]
                }
            }
            
            $dataTable.Rows.Add($dataRow)
            $currentBatch++
            $processedRows++
            
            # Show progress every 10% of completion or when LogLevel is Verbose
            if ($LogLevel -eq "Verbose" -or $processedRows % [Math]::Max(1, [Math]::Floor($totalRows / 10)) -eq 0) {
                $progressPercent = [Math]::Round(($processedRows / $totalRows) * 100, 1)
                $elapsedTime = (Get-Date) - $startTime
                $estimatedTotal = $elapsedTime.TotalSeconds / ($processedRows / $totalRows)
                $remainingTime = $estimatedTotal - $elapsedTime.TotalSeconds
                
                Write-ImportLog "Progress: $processedRows/$totalRows rows ($progressPercent%) - Est. remaining: $([Math]::Round($remainingTime, 0))s" -Level $(if ($LogLevel -eq "Verbose") { "DEBUG" } else { "INFO" })
            }
            
            # Write batch when size reached or at end
            if ($currentBatch -ge $BatchSize -or $processedRows -eq $totalRows) {
                Write-ImportLog "Writing batch $($batchCount + 1) ($currentBatch rows)" -Level $(if ($LogLevel -eq "Verbose") { "INFO" } else { "DEBUG" })
                
                try {
                    $bulkCopy.WriteToServer($dataTable)
                    $batchCount++
                    $dataTable.Clear()
                    $currentBatch = 0
                }
                catch {
                    Write-ImportLog "Error writing batch: $_" -Level "ERROR"
                    throw
                }
            }
        }
        
        # Commit transaction
        $transaction.Commit()
        
        $totalTime = ((Get-Date) - $startTime).TotalSeconds
        $rate = if ($totalTime -gt 0) { [Math]::Round($processedRows / $totalTime, 1) } else { 0 }
        Write-ImportLog "Successfully imported $processedRows rows in $([Math]::Round($totalTime, 1)) seconds ($rate rows/sec)" -Level "SUCCESS"
        
        return $processedRows
    }
    catch {
        if ($transaction -ne $null) {
            try {
                $transaction.Rollback()
                Write-ImportLog "Transaction rolled back due to error" -Level "WARNING"
            }
            catch {
                Write-ImportLog "Error rolling back transaction: $_" -Level "ERROR"
            }
        }
        
        Write-ImportLog "Import failed: $_" -Level "ERROR"
        throw $_
    }
    finally {
        if ($bulkCopy -ne $null) {
            $bulkCopy.Close()
        }
        
        if ($connection -ne $null -and $connection.State -eq [System.Data.ConnectionState]::Open) {
            $connection.Close()
        }
    }
}


#endregion

#region Main Script

# Script entry point
try {
    $startTime = Get-Date
    Write-ImportLog "Starting import process for table $TableName from $FilePath" -Level "INFO"
    
    # Parse schema and table name
    $tableInfo = Get-SchemaAndTableName -FullTableName $TableName
    Write-ImportLog "Target: $($tableInfo.FullName)" -Level "INFO"
    
    # Validate file exists
    if (-not (Test-Path $FilePath)) {
        Write-ImportLog "File not found: $FilePath" -Level "ERROR"
        exit 1
    }
    
    $fileInfo = Get-Item $FilePath
    Write-ImportLog "Source file: $($fileInfo.Name) ($([Math]::Round($fileInfo.Length / 1MB, 2)) MB)" -Level "INFO"
    
    # Build connection string
    $connectionString = "Server=$ServerInstance;Database=$Database;Integrated Security=True;"
    
    # Test connection
    if (-not (Test-SqlConnection -ConnectionString $connectionString)) {
        exit 1
    }
    
    # Read CSV data with progress
    Write-ImportLog "Reading CSV data..." -Level "INFO"
    $csvData = Import-Csv -Path $FilePath
    
    if ($csvData.Count -eq 0) {
        Write-ImportLog "CSV file contains no data" -Level "WARNING"
        exit 0
    }
    
    Write-ImportLog "Found $($csvData.Count) rows in CSV" -Level "INFO"
    
    # Get column names
    $columnNames = $csvData[0].PSObject.Properties.Name
    Write-ImportLog "CSV contains $($columnNames.Count) columns" -Level $(if ($LogLevel -eq "Verbose") { "INFO" } else { "DEBUG" })
    
    if ($LogLevel -eq "Verbose") {
        Write-ImportLog "Columns: $($columnNames -join ', ')" -Level "DEBUG"
    }
    
    # Get table schema
    Write-ImportLog "Retrieving table schema..." -Level "INFO"
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    
    $tableSchema = Get-TableSchema -Connection $connection -SchemaName $tableInfo.Schema -TableName $tableInfo.Table
    $connection.Close()
    
    if ($tableSchema -eq $null) {
        Write-ImportLog "Could not retrieve schema for table $($tableInfo.FullName)" -Level "ERROR"
        exit 1
    }
    
    Write-ImportLog "Retrieved schema with $($tableSchema.Count) columns" -Level "INFO"
    
    # Validate columns
    $missingColumns = @()
    foreach ($column in $tableSchema.Keys) {
        if ($column -notin $columnNames) {
            if (-not $tableSchema[$column].IsNullable) {
                $missingColumns += $column
            }
        }
    }
    
    if ($missingColumns.Count -gt 0) {
        Write-ImportLog "CSV is missing required columns: $($missingColumns -join ', '). Generating..." -Level "WARNING"
    }
    
    # Import data
    Write-ImportLog "Starting bulk import with batch size $BatchSize..." -Level "INFO"
    
    $rowsImported = Import-CsvToSql -ConnectionString $connectionString `
                                    -Schema $tableInfo.Schema `
                                    -TableName $tableInfo.Table `
                                    -CsvData $csvData `
                                    -TableSchema $tableSchema `
                                    -TruncateFirst $TruncateTable `
                                    -BatchSize $BatchSize `
                                    -Timeout $Timeout
    

    
    # Return the count of imported rows
    $rowsImported
	exit 0
}
catch {
    Write-ImportLog "Script failed: $_" -Level "ERROR"
    exit 1
}

#endregion