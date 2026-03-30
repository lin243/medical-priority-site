$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonScript = Join-Path $scriptDir "score_npnews_daily_table.py"

if (-not (Test-Path $pythonScript)) {
    throw "Missing Python entrypoint: $pythonScript"
}

$env:UV_LINK_MODE = "copy"
uv run --project "E:\llm-master" python $pythonScript @args
