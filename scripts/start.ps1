param([ValidateRange(1,64)][int]$Workers = 2)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    docker compose up --build --wait --scale "worker=$Workers"
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose startup failed' }
    docker compose ps
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose status failed' }
} finally { Pop-Location }
