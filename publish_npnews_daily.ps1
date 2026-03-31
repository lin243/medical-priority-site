param(
    [string]$Date
)

$ErrorActionPreference = "Stop"

function Get-PreviousDayInShanghai {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("China Standard Time")
    $shanghaiNow = [System.TimeZoneInfo]::ConvertTimeFromUtc([DateTime]::UtcNow, $tz)
    return $shanghaiNow.Date.AddDays(-1).ToString("yyyy-MM-dd")
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pipelineScript = Join-Path $scriptDir "run_npnews_pipeline.ps1"

if (-not (Test-Path $pipelineScript)) {
    throw "Missing pipeline entrypoint: $pipelineScript"
}

$targetDate = if ($Date) { $Date } else { Get-PreviousDayInShanghai }
$historyDir = Join-Path $scriptDir "history"
$dailyCsv = Join-Path $historyDir "npnews_daily_table_${targetDate}_scored.csv"
$webCsv = Join-Path $scriptDir "npnews_daily_table_latest_scored.csv"

Write-Host "Running NPNews pipeline for date: $targetDate"
& $pipelineScript --date $targetDate

if (-not (Test-Path $dailyCsv)) {
    throw "Missing daily scored csv after pipeline run: $dailyCsv"
}

if (-not (Test-Path $webCsv)) {
    throw "Missing webpage csv after pipeline run: $webCsv"
}

Write-Host "Pipeline complete."
Write-Host "Daily CSV: $dailyCsv"
Write-Host "Web CSV: $webCsv"
