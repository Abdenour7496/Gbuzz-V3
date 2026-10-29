[CmdletBinding()]
param(
    [string]$ComposeFile = "docker-compose.production.lock.yml",
    [bool]$IncludeObservability = $true,
    [switch]$IncludeGraph,
    [switch]$RequireRegistry
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ComposeFile)) {
    throw "Missing $ComposeFile. Copy docker-compose.production.lock.example.yml and replace every sha256 placeholder."
}

$composeFiles = @("-f", "docker-compose.yml", "-f", "docker-compose.enterprise.yml", "-f", "docker-compose.buzz.yml", "-f", "docker-compose.knowledge-trust.yml")
if ($IncludeGraph) { throw 'The graph extension was retired: Graphiti and FalkorDB are no longer part of Gbuzz (see docs/optional-graph.md).' }
if ($IncludeObservability) {
    $composeFiles += @("-f", "docker-compose.observability.yml")
    $composeFiles += @("--profile", "observability")
}
$composeFiles += @("-f", $ComposeFile)
$images = @(docker compose @composeFiles config --images)
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose could not render the production configuration."
}

$invalid = @($images | Where-Object {
    $_ -notmatch '@sha256:[0-9a-f]{64}$' -and $_ -notmatch '^gbuzz-[a-z0-9-]+:[0-9a-f]{7,40}$'
})
if ($invalid.Count -gt 0) {
    throw "Production images must use a sha256 digest or a commit-addressed Gbuzz tag:`n$($invalid -join "`n")"
}

if ($RequireRegistry) {
    $localImages = @($images | Where-Object { $_ -match '^gbuzz-[a-z0-9-]+@sha256:' -or $_ -match '^gbuzz-[a-z0-9-]+:' })
    if ($localImages.Count -gt 0) {
        throw "Production application images must be published to a registry, not referenced by local names:`n$($localImages -join "`n")"
    }
}

Write-Host "Verified $($images.Count) immutable production image references."
