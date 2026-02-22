# extractor.ps1
# Output:
#   C:\Scripts\ddl_all.sql
#   C:\Scripts\inserts_selected.sql

$ErrorActionPreference = "Stop"

$base = "C:\Scripts"
$dump = "$base\videoanalytics.sql"
$list = "$base\tables.txt"

$ddlOut  = "$base\ddl_all.sql"
$dataOut = "$base\inserts_selected.sql"

if (!(Test-Path $dump)) { throw "Dump not found: $dump" }
if (!(Test-Path $list)) { throw "Table list not found: $list" }

$wanted = Get-Content $list | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
$wantedSet = @{}
$wanted | ForEach-Object { $wantedSet[$_] = $true }

Write-Host "Dump:     $dump"
Write-Host "DDL out:  $ddlOut"
Write-Host "DATA out: $dataOut"
Write-Host ("Wanted tables: {0}" -f $wanted.Count)

$reDrop   = '^DROP TABLE IF EXISTS `([^`]+)`;'
$reCreate = '^CREATE TABLE `'
$reCreateEnd = '^\)\s*ENGINE=|^\)\s*;'
$reLock   = '^LOCK TABLES '
$reInsert = '^INSERT INTO '
$reUnlock = '^UNLOCK TABLES;'

$reader = New-Object System.IO.StreamReader($dump)
$ddlW   = New-Object System.IO.StreamWriter($ddlOut, $false, [System.Text.Encoding]::UTF8)
$dataW  = New-Object System.IO.StreamWriter($dataOut, $false, [System.Text.Encoding]::UTF8)

$currentTable = $null
$inCreate = $false
$writingDDL = $false
$includeData = $false

try {
  while (($line = $reader.ReadLine()) -ne $null) {

    # Before first table: copy header to both files
    if ($null -eq $currentTable -and ($line -notmatch $reDrop)) {
      $ddlW.WriteLine($line)
      $dataW.WriteLine($line)
      continue
    }

    # New table
    if ($line -match $reDrop) {
      $currentTable = $Matches[1]
      $inCreate = $false
      $writingDDL = $true
      $includeData = $wantedSet.ContainsKey($currentTable)

      $ddlW.WriteLine($line)
      continue
    }

    # CREATE TABLE
    if ($line -match $reCreate) {
      $inCreate = $true
      $writingDDL = $true
      $ddlW.WriteLine($line)
      continue
    }

    # DDL block
    if ($writingDDL) {
      $ddlW.WriteLine($line)
      if ($inCreate -and ($line -match $reCreateEnd)) {
        $inCreate = $false
        $writingDDL = $false
      }
      continue
    }

    # DATA block
    if ($line -match $reLock) {
      if ($includeData) {
        $dataW.WriteLine($line)
        while (($l2 = $reader.ReadLine()) -ne $null) {
          $dataW.WriteLine($l2)
          if ($l2 -match $reUnlock) { break }
        }
      }
      else {
        while (($l2 = $reader.ReadLine()) -ne $null) {
          if ($l2 -match $reUnlock) { break }
        }
      }
      $currentTable = $null
      continue
    }

    # Handle dumps without LOCK/UNLOCK (pure INSERT style)
    if ($includeData -and $line -match $reInsert) {
      $dataW.WriteLine($line)
      continue
    }

    if ($line -match $reUnlock) {
      $currentTable = $null
      continue
    }
  }
}
finally {
  $ddlW.Flush();  $ddlW.Close()
  $dataW.Flush(); $dataW.Close()
  $reader.Close()
}

Write-Host "Done."
Write-Host "Created:"
Write-Host "  $ddlOut"
Write-Host "  $dataOut"