$ErrorActionPreference = 'Stop'
# Graphiti and FalkorDB were retired (docs/optional-graph.md). GCOR's graph and bi-temporal facts
# live in PostgreSQL. This check keeps every Compose plan free of the retired services.
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    function Read-Config([string[]]$Extra) {
        $raw = & docker compose --env-file .env.example -f docker-compose.yml @Extra config --format json
        if ($LASTEXITCODE -ne 0) { throw 'Compose rendering failed' }
        return ($raw | ConvertFrom-Json)
    }
    $plans = @{
        core = Read-Config @()
        production = Read-Config @('-f','docker-compose.observability.yml','-f','docker-compose.production.yml')
        enterprise = Read-Config @('-f','docker-compose.enterprise.yml','-f','docker-compose.buzz.yml')
    }
    foreach ($plan in $plans.Keys) {
        $services = $plans[$plan].services.PSObject.Properties.Name
        foreach ($name in @('falkordb','graphiti-mcp','graphiti-projector','graphiti-model-init')) {
            if ($services -contains $name) { throw "$plan plan still includes retired $name" }
        }
        foreach ($name in @('gcor-proxy','gcor-event-projector','postgres','relay')) {
            if ($services -notcontains $name) { throw "$plan plan omits $name" }
        }
    }
    if (($plans.core.services.'ollama-init'.command -join ' ') -match 'GRAPHITI') { throw 'Core downloads graph models' }
    foreach ($file in @('docker-compose.graph.yml','docker-compose.graph-production.yml')) {
        if (Test-Path $file) { throw "Retired overlay $file is present" }
    }
    Write-Output 'Retired graph extension checks passed: core, production and enterprise plans.'
} finally { Pop-Location }
