param(
    [string]$AsOf = "2026-01-10"
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$month = ([datetime]::Parse($AsOf)).ToString("yyyy-MM")

Push-Location $projectRoot
try {
    Write-Host "Running PBOR-Lite month-end for $AsOf"
    python -m src.run_month_end --asof $AsOf --project-root $projectRoot

    Write-Host ""
    Write-Host "Showing results for $month"
    python -m src.show_results --month $month --project-root $projectRoot

    Write-Host ""
    Write-Host "Report pack:"
    $summaryPath = Join-Path $projectRoot "outputs\$month\summary.json"
    $workbookName = "report.xlsx"
    if (Test-Path $summaryPath) {
        $summaryJson = Get-Content $summaryPath -Raw | ConvertFrom-Json
        $xlsx = @($summaryJson.files | Where-Object { $_ -like "*.xlsx" })
        if ($xlsx.Count -gt 0) { $workbookName = $xlsx[0] }
    }
    Write-Host "$projectRoot\outputs\$month\onepager.md"
    Write-Host "$projectRoot\outputs\$month\onepager.pdf"
    Write-Host "$projectRoot\outputs\$month\$workbookName"
    Write-Host "$projectRoot\outputs\$month\tearsheet.png"
} finally {
    Pop-Location
}
