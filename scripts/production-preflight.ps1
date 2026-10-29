[CmdletBinding()]
param(
    [string]$EnvFile = ".env",
    [string]$LockFile = "docker-compose.production.lock.yml",
    [ValidateRange(1, 99)][int]$MinimumFreePercent = 15,
    [switch]$SkipProjectionGate,
    [switch]$IncludeGraph,
    [switch]$AllowDirtyTree
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

function Read-DotEnv([string]$Path) {
    $values = @{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '^\s*#' -or $line -notmatch '=') { continue }
        $name, $value = $line -split '=', 2
        $values[$name.Trim()] = $value.Trim()
    }
    return $values
}

function Assert-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) { throw "Required command is unavailable: $Name" }
}

Push-Location $repoRoot
try {
    Assert-Command git
    Assert-Command docker
    if (-not (Test-Path -LiteralPath $EnvFile)) { throw "Missing environment file: $EnvFile" }
    if (-not (Test-Path -LiteralPath $LockFile)) { throw "Missing production lock: $LockFile" }
    $envValues = Read-DotEnv $EnvFile

    $requiredSecrets = @('POSTGRES_PASSWORD','GCOR_DB_PASSWORD','GCOR_S3_SECRET_KEY','REDIS_PASSWORD','BUZZ_S3_SECRET_KEY','INGEST_WEBHOOK_SECRET','STACK_API_SECRET','GRAFANA_ADMIN_PASSWORD')
    if ($envValues['EMBEDDING_BACKEND'] -ne 'ollama') { $requiredSecrets += 'OPENAI_API_KEY' }
    foreach ($name in $requiredSecrets) {
        $value = $envValues[$name]
        if ([string]::IsNullOrWhiteSpace($value) -or $value -match '(?i)change-me|replace|example') {
            throw "Production secret is missing or a placeholder: $name"
        }
    }

    foreach ($name in @('RELEASE_OWNER','ROLLBACK_OWNER','MIGRATION_OWNER','ON_CALL_CONTACT')) {
        $value = $envValues[$name]
        if ([string]::IsNullOrWhiteSpace($value) -or $value -match '(?i)change-me|unassigned|unknown|tbd') {
            throw "Production ownership is not assigned: $name"
        }
    }

    $alertConfig = $envValues['ALERTMANAGER_CONFIG_FILE']
    if ([string]::IsNullOrWhiteSpace($alertConfig) -or -not (Test-Path -LiteralPath $alertConfig)) {
        throw "ALERTMANAGER_CONFIG_FILE must reference a deployment-specific configuration."
    }
    $alertText = Get-Content -Raw -LiteralPath $alertConfig
    if ($alertText -match 'REPLACE_|local-audit-webhook|http://alert-receiver') {
        throw "Alertmanager still uses a placeholder or local-only receiver."
    }

    $drive = Get-PSDrive -Name ([IO.Path]::GetPathRoot($repoRoot).TrimEnd(':','\'))
    $freePercent = 100 * $drive.Free / ($drive.Used + $drive.Free)
    if ($freePercent -lt $MinimumFreePercent) {
        throw ("Workspace filesystem has {0:N1}% free; at least {1}% is required." -f $freePercent, $MinimumFreePercent)
    }

    if (-not $AllowDirtyTree -and -not [string]::IsNullOrWhiteSpace((git status --porcelain))) {
        throw "Release candidate has uncommitted or untracked changes."
    }

    $graphArgs = @()
    if ($IncludeGraph) { throw 'The graph extension was retired: Graphiti and FalkorDB are no longer part of Gbuzz (see docs/optional-graph.md).' }
    & docker compose --env-file $EnvFile -f docker-compose.yml -f docker-compose.observability.yml -f docker-compose.production.yml -f docker-compose.observability-production.yml -f docker-compose.enterprise.yml -f docker-compose.buzz.yml -f docker-compose.knowledge-trust.yml @graphArgs -f $LockFile --profile observability config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Production Compose rendering failed." }
    $pinArgs = @()
    & powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-production-pins.ps1 -ComposeFile $LockFile -RequireRegistry @pinArgs
    if ($LASTEXITCODE -ne 0) { throw "Production image pin verification failed." }

    Write-Output "Production P0 preflight passed for commit $(git rev-parse HEAD)."
}
finally { Pop-Location }
