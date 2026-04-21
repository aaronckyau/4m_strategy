$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $root ".venv\Scripts\python.exe"

if (Test-Path $venvPython) {
    $python = $venvPython
} else {
    $python = "python"
}

function Invoke-Step {
    param(
        [string]$Label,
        [scriptblock]$Command
    )

    Write-Host "[QC] $Label"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "QC step failed: $Label"
    }
}

Invoke-Step "compileall" {
    & $python -m compileall "$root\Aurum_Infinity_AI" "$root\Aurum_Data_Fetcher" "$root\News_fetcher"
}

Invoke-Step "ruff" {
    & $python -m ruff check `
        "$root\Aurum_Infinity_AI" `
        "$root\Aurum_Data_Fetcher" `
        "$root\News_fetcher" `
        "$root\check_db.py" `
        "$root\check_db2.py"
}

$env:PYTHONPATH = Join-Path $root "Aurum_Infinity_AI"
Invoke-Step "pytest" {
    & $python -m pytest "$root\Aurum_Infinity_AI\tests" -q
}
