param(
    [string]$Date,
    [string]$CommitMessage,
    [switch]$SkipPush
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
$dailyCsv = Join-Path $scriptDir "npnews_daily_table_${targetDate}_scored.csv"
$webCsv = Join-Path $scriptDir "npnews_daily_table_latest_scored.csv"

Write-Host "Running NPNews pipeline for date: $targetDate"
& $pipelineScript --date $targetDate

if (-not (Test-Path $dailyCsv)) {
    throw "Missing daily scored csv after pipeline run: $dailyCsv"
}

if (-not (Test-Path $webCsv)) {
    throw "Missing webpage csv after pipeline run: $webCsv"
}

$repoRoot = $scriptDir
$branch = (git -C $repoRoot rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch) {
    throw "Unable to determine current git branch."
}

Write-Host "Staging data files for publish"
git -C $repoRoot add -- $webCsv $dailyCsv

$stagedNames = git -C $repoRoot diff --cached --name-only -- `
    "npnews_daily_table_latest_scored.csv" `
    "npnews_daily_table_${targetDate}_scored.csv"

if (-not $stagedNames) {
    Write-Host "No staged data changes to commit."
    exit 0
}

$message = if ($CommitMessage) {
    $CommitMessage
} else {
    "update npnews data $targetDate"
}

Write-Host "Creating commit: $message"
git -C $repoRoot commit -m $message -- `
    "npnews_daily_table_latest_scored.csv" `
    "npnews_daily_table_${targetDate}_scored.csv"

if ($SkipPush) {
    Write-Host "SkipPush enabled. Commit created locally on branch $branch."
    exit 0
}

Write-Host "Pushing branch: $branch"
git -C $repoRoot push origin $branch

Write-Host "Publish complete."
