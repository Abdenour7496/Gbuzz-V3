[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$workspace = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$components = @("proxy", "projector", "mcp-postgres-gcor")

foreach ($component in $components) {
    docker run --rm `
        --volume "${workspace}:/workspace" `
        --workdir "/workspace/$component" `
        python:3.12-slim `
        sh -ec "python -m pip install --disable-pip-version-check --quiet pip-tools==7.5.0 && pip-compile --quiet --strip-extras --generate-hashes --resolver=backtracking --output-file=requirements.lock requirements.txt"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to lock dependencies for $component"
    }
}

Write-Host "Updated Python lock files for $($components.Count) components."
