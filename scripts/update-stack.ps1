[CmdletBinding()]
param(
    [switch]$Force,
    [switch]$IncludeObservability,
    [switch]$IncludeGraph,
    [int]$WaitTimeoutSeconds = 300,
    [string]$AuditPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot ".env"

function Get-DotEnvValue([string]$Name) {
    if (-not (Test-Path -LiteralPath $envFile)) { return $null }
    $line = Get-Content -LiteralPath $envFile | Where-Object { $_ -match "^$([regex]::Escape($Name))=" } | Select-Object -Last 1
    if ($null -eq $line) { return $null }
    return ($line -split "=", 2)[1].Trim()
}

function Write-Audit([string]$Result, [string]$Message) {
    $record = [ordered]@{
        timestamp = (Get-Date).ToUniversalTime().ToString("o")
        result = $Result
        message = $Message
        host_name = [Environment]::MachineName
    }
    $record | ConvertTo-Json -Compress | Add-Content -LiteralPath $AuditPath -Encoding utf8
}

if (-not $Force -and (Get-DotEnvValue "AUTO_UPDATE_ENABLED") -ne "true") {
    Write-Host "Automatic stack updates are disabled. Set AUTO_UPDATE_ENABLED=true or pass -Force."
    exit 0
}

if (-not $PSBoundParameters.ContainsKey("IncludeObservability")) {
    $IncludeObservability = (Get-DotEnvValue "AUTO_UPDATE_INCLUDE_OBSERVABILITY") -eq "true"
}
if (-not $PSBoundParameters.ContainsKey("WaitTimeoutSeconds")) {
    $configuredTimeout = Get-DotEnvValue "AUTO_UPDATE_WAIT_TIMEOUT_SECONDS"
    if ($configuredTimeout) { $WaitTimeoutSeconds = [int]$configuredTimeout }
}
if (-not $PSBoundParameters.ContainsKey("IncludeGraph")) {
    $IncludeGraph = (Get-DotEnvValue "AUTO_UPDATE_INCLUDE_GRAPH") -eq "true"
}
if ([string]::IsNullOrWhiteSpace($AuditPath)) {
    $AuditPath = Join-Path $repoRoot "backups\stack-update-audit.jsonl"
}
$auditDirectory = Split-Path -Parent $AuditPath
New-Item -ItemType Directory -Path $auditDirectory -Force | Out-Null

$mutex = [Threading.Mutex]::new($false, "GbuzzStackAutoUpdate")
$hasLock = $false
try {
    $hasLock = $mutex.WaitOne(0)
    if (-not $hasLock) { throw "Another stack update is already running." }

    Push-Location $repoRoot
    try {
        & docker info --format '{{.ServerVersion}}' | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Docker Engine is unavailable." }

        Import-Module (Join-Path $PSScriptRoot 'ComposePlan.psm1') -Force
        $plan = Get-GbuzzComposePlan $repoRoot (Join-Path $repoRoot 'config/windows-startup.compose-files.json')
        if ($IncludeGraph) { throw 'The graph extension was retired: Graphiti and FalkorDB are no longer part of Gbuzz (see docs/optional-graph.md).' }
        $composeArgs = @('compose') + $plan.Arguments
        if (@($plan.Files | Where-Object { $_ -like '*knowledge-trust*' }).Count) {
            throw 'The knowledge trust release is pinned. Build and verify a new release before replacing its images; automatic image refresh is disabled.'
        }

        Write-Host "Pulling configured remote image tags..."
        & docker @composeArgs pull --ignore-buildable
        if ($LASTEXITCODE -ne 0) { throw "Image pull failed." }

        Write-Host "Rebuilding workspace-owned images with refreshed base images..."
        & docker @composeArgs build --pull
        if ($LASTEXITCODE -ne 0) { throw "Image build failed." }

        Write-Host "Applying the update and waiting for health checks..."
        & docker @composeArgs up -d --wait --wait-timeout $WaitTimeoutSeconds
        if ($LASTEXITCODE -ne 0) { throw "Updated stack did not become healthy within $WaitTimeoutSeconds seconds." }

        $message = "Stack update completed; observability=$IncludeObservability; graph=$IncludeGraph"
        Write-Audit "success" $message
        Write-Host $message
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Audit "failure" $_.Exception.Message
    Write-Error $_
    exit 1
}
finally {
    if ($hasLock) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
