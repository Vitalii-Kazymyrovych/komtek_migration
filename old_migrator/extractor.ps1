[CmdletBinding()]
param(
    [string]$BasePath = "C:\Scripts",
    [string]$DumpPath,
    [string]$TablesPath,
    [string]$DdlSourcePath,
    [string]$DataOutPath,
    [string]$OldDdlOutPath
)

$ErrorActionPreference = "Stop"

if (-not $DumpPath) { $DumpPath = Join-Path $BasePath "videoanalytics.sql" }
if (-not $TablesPath) { $TablesPath = Join-Path $BasePath "tables.txt" }
if (-not $DdlSourcePath) { $DdlSourcePath = Join-Path $BasePath "OLD_DDL.txt" }
if (-not $DataOutPath) { $DataOutPath = Join-Path $BasePath "inserts_selected.sql" }
if (-not $OldDdlOutPath) { $OldDdlOutPath = Join-Path $BasePath "oldDDL_selected.txt" }

if (!(Test-Path $DumpPath)) { throw "Dump not found: $DumpPath" }
if (!(Test-Path $DdlSourcePath)) { throw "DDL source not found: $DdlSourcePath" }

$defaultTables = @(
    "analytics",
    "clients",
    "event_manager",
    "face_detections",
    "face_encodings",
    "face_list_items",
    "face_list_items_images",
    "face_lists",
    "face_notifications",
    "face_unique_person_mapping",
    "roles",
    "servers",
    "settings",
    "stats_traffic_minutely",
    "stream_groups",
    "streams",
    "traffic_stat",
    "users"
)

if (Test-Path $TablesPath) {
    $wanted = Get-Content $TablesPath | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
} else {
    $wanted = $defaultTables
}

$wantedSet = New-Object "System.Collections.Generic.HashSet[string]" ([System.StringComparer]::OrdinalIgnoreCase)
$wanted | ForEach-Object { [void]$wantedSet.Add($_) }

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

Write-Host "Dump in:      $DumpPath"
Write-Host "DDL source:   $DdlSourcePath"
Write-Host "Data out:     $DataOutPath"
Write-Host "Old DDL out:  $OldDdlOutPath"
Write-Host ("Tables:       {0}" -f (($wanted | Sort-Object) -join ", "))

function Get-InsertTable([string]$line) {
    if ($line -match '^INSERT\s+INTO\s+`?([^`\s\(]+)`?') {
        return $Matches[1]
    }
    return $null
}

function Get-LockTable([string]$line) {
    if ($line -match '^LOCK\s+TABLES\s+`?([^`\s]+)`?\s+WRITE;') {
        return $Matches[1]
    }
    return $null
}

# 1) Extract table data exactly as present in source dump.
$reader = New-Object System.IO.StreamReader($DumpPath)
$dataWriter = New-Object System.IO.StreamWriter($DataOutPath, $false, $utf8NoBom)
$includeCurrentLock = $false

try {
    while (($line = $reader.ReadLine()) -ne $null) {
        $lockTable = Get-LockTable $line
        if ($null -ne $lockTable) {
            $includeCurrentLock = $wantedSet.Contains($lockTable)
            if ($includeCurrentLock) {
                $dataWriter.WriteLine($line)
            }
            continue
        }

        if ($line -match '^UNLOCK\s+TABLES;') {
            if ($includeCurrentLock) {
                $dataWriter.WriteLine($line)
            }
            $includeCurrentLock = $false
            continue
        }

        if ($includeCurrentLock) {
            $dataWriter.WriteLine($line)
            continue
        }

        $insertTable = Get-InsertTable $line
        if ($null -ne $insertTable -and $wantedSet.Contains($insertTable)) {
            $dataWriter.WriteLine($line)
            while ($line -notmatch ';\s*$') {
                $line = $reader.ReadLine()
                if ($null -eq $line) { break }
                $dataWriter.WriteLine($line)
            }
        }
    }
}
finally {
    $dataWriter.Flush(); $dataWriter.Close()
    $reader.Close()
}

# 2) Extract DDL blocks from an existing DDL file in the exact original formatting.
$ddlLines = Get-Content -Path $DdlSourcePath
$ddlWriter = New-Object System.IO.StreamWriter($OldDdlOutPath, $false, $utf8NoBom)

try {
    $inside = $false
    foreach ($line in $ddlLines) {
        if ($line -match '^--\s+.+\.(?:"|`)?([^"`\s]+)(?:"|`)?\s+definition\s*$') {
            $tableName = $Matches[1]
            $inside = $wantedSet.Contains($tableName)
        }

        if ($inside) {
            $ddlWriter.WriteLine($line)
        }
    }
}
finally {
    $ddlWriter.Flush(); $ddlWriter.Close()
}

Write-Host "Done."
