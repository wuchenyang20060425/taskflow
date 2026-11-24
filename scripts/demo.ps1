param(
    [string]$Pdf = (Join-Path (Split-Path -Parent $PSScriptRoot) 'examples/sample.pdf'),
    [string]$BaseUrl = 'http://127.0.0.1:8080',
    [string]$Output = 'result.txt',
    [ValidateRange(1,600)][int]$TimeoutSeconds = 90
)
$ErrorActionPreference = 'Stop'
$Pdf = (Resolve-Path -LiteralPath $Pdf).Path
$status = curl.exe --silent --show-error --fail --max-time 5 "$BaseUrl/ready"
if ($LASTEXITCODE -ne 0) { throw 'API is not ready' }
$reply = curl.exe --silent --show-error --fail --max-time 40 -F "file=@$Pdf" "$BaseUrl/tasks"
if ($LASTEXITCODE -ne 0) { throw 'PDF upload failed' }
$id = ($reply | ConvertFrom-Json).id
if ($id -notmatch '^[0-9a-f]{32}$') { throw 'Missing task ID' }
$until = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
do {
    $reply = curl.exe --silent --show-error --fail --max-time 5 "$BaseUrl/tasks/$id"
    if ($LASTEXITCODE -ne 0) { throw "Task lookup failed: $id" }
    $task = $reply | ConvertFrom-Json
    if ($task.status -eq 'succeeded') {
        curl.exe --silent --show-error --fail --max-time 40 -o "$Output" "$BaseUrl/tasks/$id/result"
        if ($LASTEXITCODE -ne 0) { throw "Download failed: $id" }
        Write-Output "$id succeeded, attempts=$($task.attempts), output=$Output"
        return
    }
    if ($task.status -eq 'failed') { throw "Task failed: $($task.error)" }
    Start-Sleep -Milliseconds 300
} while ([DateTime]::UtcNow -lt $until)
throw "Timed out waiting for task $id"
