<#
.SYNOPSIS
  Release 2026-10-26: production-blocker fixes, retrieval quality and the knowledge-loop sprint
  (Gbuzz-V3 main). Knowledge services only; Buzz relay, Postgres, Redis, MinIO and Ollama are not
  recreated.

.DESCRIPTION
  1. Preflight: Docker, disk, clean git tree, running compose set, clock skew.
  2. Rollback record, then a verified encrypted backup (scripts/backup-stack.ps1) unless -SkipBackup.
  3. Check out the release branch (default release-20261026) in this repository.
  4. Build the knowledge image and run the release unit gate inside it (no network).
  5. Apply migrations (0016 retrieval performance, 0017 unanswered questions; both additive).
     Report the pgvector version; update the extension when the image ships a newer one.
  6. Point GCOR_TRUST_IMAGE at the new image and recreate gcor-proxy, gcor-enterprise and
     buzz-knowledge. Automatic rollback if they do not become healthy.
  7. Verify: readiness and model status, migration ledger, new tables, signed-request probe.

  Rollback: .\scripts\release-20261026.ps1 -Rollback
  (previous image and branch; the additive migrations need no database rollback)
#>
[CmdletBinding()]
param(
    [string]$Branch = 'release-20261026',
    [string]$Tag = 'gbuzz-knowledge-durable:release-20261026',
    [string]$PreviousBranch = '',
    [switch]$SkipBackup,
    [switch]$SkipVectorUpdate,
    [switch]$Rollback,
    [int]$WaitTimeoutSeconds = 300,
    [int]$MinFreeGB = 8
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot '.env'
$services = @('gcor-proxy', 'gcor-enterprise', 'buzz-knowledge')
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$evidenceRoot = Join-Path $repoRoot 'backups'
$agentCli = Join-Path $repoRoot 'agent-tools\gcor-agent\dist\gcor-agent.mjs'

function Step([string]$Text) { Write-Host ''; Write-Host "==> $Text" -ForegroundColor Cyan }
function Ok([string]$Text) { Write-Host "    OK  $Text" -ForegroundColor Green }
function Warn([string]$Text) { Write-Host "    !!  $Text" -ForegroundColor Yellow }

# Windows PowerShell 5.1 turns native stderr into terminating errors under 'Stop'.
function Invoke-Native([string]$File, [string[]]$Arguments, [switch]$Capture, [switch]$AllowFailure) {
    $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
    try {
        if ($Capture) { $out = & $File @Arguments 2>$null } else { & $File @Arguments 2>&1 | ForEach-Object { $line = "$_"; if ($line -and $line -ne 'System.Management.Automation.RemoteException') { Write-Host "      $line" } } }
        $code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    if ($code -ne 0 -and -not $AllowFailure) { throw "$File $($Arguments[0]) $($Arguments[1]) failed (exit $code)" }
    if ($Capture) { return $out }
}

function Get-DotEnvValue([string]$Name) {
    $line = Get-Content -LiteralPath $envFile | Where-Object { $_ -match "^$([regex]::Escape($Name))=" } | Select-Object -Last 1
    if ($null -eq $line) { return $null }
    return ($line -split '=', 2)[1].Trim()
}

function Set-DotEnvValue([string]$Name, [string]$Value) {
    $lines = [Collections.Generic.List[string]](Get-Content -LiteralPath $envFile)
    $found = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match "^$([regex]::Escape($Name))=") { $lines[$i] = "$Name=$Value"; $found = $true }
    }
    if (-not $found) { $lines.Add("$Name=$Value") }
    $tmp = "$envFile.tmp-$stamp"
    [IO.File]::WriteAllLines($tmp, $lines, (New-Object Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $tmp -Destination $envFile -Force
}

# The running stack is authoritative (the startup plan has drifted before).
function Get-RunningComposeArgs {
    $inspect = (Invoke-Native docker @('inspect', 'gbuzz-gcor-proxy-1') -Capture) -join "`n" | ConvertFrom-Json
    $label = @($inspect)[0].Config.Labels.'com.docker.compose.project.config_files'
    $files = @(("$label".Trim()) -split ',' | Where-Object { $_ })
    if (-not $files.Count) { throw 'Cannot read the compose file set from gbuzz-gcor-proxy-1. Is the stack running?' }
    if (-not ($files | Where-Object { $_ -like '*knowledge-trust*' })) { throw 'Running stack is not on the knowledge-trust overlay; refusing to release into an unknown baseline.' }
    $composeArgs = @('compose', '--project-name', 'gbuzz')
    foreach ($f in $files) { $composeArgs += @('-f', $f) }
    $plan = Get-Content (Join-Path $repoRoot 'config\windows-startup.compose-files.json') -Raw | ConvertFrom-Json
    foreach ($p in @($plan.profiles)) { $composeArgs += @('--profile', $p) }
    return [pscustomobject]@{ Args = $composeArgs; Files = $files }
}

function Wait-Healthy([object]$Compose) {
    Invoke-Native docker ($Compose.Args + @('up', '-d', '--no-deps', '--wait', '--wait-timeout', "$WaitTimeoutSeconds") + $services)
}

function Invoke-Psql([string]$Sql) {
    $user = Get-DotEnvValue 'POSTGRES_USER'; $db = Get-DotEnvValue 'POSTGRES_DB'
    return Invoke-Native docker @('exec', '-i', 'gbuzz-postgres-1', 'psql', '-U', $user, '-d', $db, '-v', 'ON_ERROR_STOP=1', '-qtAF', '|', '-c', $Sql) -Capture
}

function Test-ClockSkew([int]$LimitSeconds = 20) {
    $vm = Invoke-Native docker @('exec', 'gbuzz-gcor-enterprise-1', 'python', '-c', 'import time;print(int(time.time()))') -Capture -AllowFailure | Select-Object -First 1
    if ("$vm" -notmatch '^\d+$') { Warn 'Could not read the container clock; skipping skew check.'; return }
    $skew = [int64]$vm - [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    if ([math]::Abs($skew) -gt $LimitSeconds) {
        throw "Docker VM clock differs from Windows by $skew s; signed requests would be rejected. Fix: 'wsl --shutdown', restart Docker Desktop, re-run."
    }
    Ok "Clock skew Windows vs Docker VM: $skew s"
}

Push-Location $repoRoot
try {
    if ($Rollback) {
        Step 'Rolling back release 2026-10-26'
        $record = Get-ChildItem $evidenceRoot -Directory -Filter 'release-20261026-*' | Sort-Object Name -Descending |
            ForEach-Object { Join-Path $_.FullName 'rollback.json' } | Where-Object { Test-Path $_ } | Select-Object -First 1
        if (-not $record) { throw 'No release-20261026 rollback record found.' }
        $data = Get-Content $record -Raw | ConvertFrom-Json
        Set-DotEnvValue 'GCOR_TRUST_IMAGE' $data.previous_trust_image
        Invoke-Native git @('checkout', $data.previous_branch)
        Wait-Healthy (Get-RunningComposeArgs)
        Ok "Restored GCOR_TRUST_IMAGE=$($data.previous_trust_image) and branch $($data.previous_branch). Migrations 0016/0017 are additive and stay; older images ignore them."
        return
    }

    Step 'Preflight'
    Invoke-Native docker @('info', '--format', '{{.ServerVersion}}') -Capture | Out-Null; Ok 'Docker Engine reachable'
    $freeGB = [math]::Round((Get-PSDrive -Name C).Free / 1GB, 1)
    if ($freeGB -lt $MinFreeGB) { throw "Only $freeGB GB free on C:. Free at least $MinFreeGB GB before building (old images: docker image prune)." }
    Ok "$freeGB GB free on C:"
    $dirty = @(Invoke-Native git @('status', '--porcelain', '--untracked-files=no') -Capture | Where-Object { $_ })
    if ($dirty.Count) { $dirty | Select-Object -First 10 | ForEach-Object { Warn "  $_" }; throw 'Tracked files have local changes; commit or stash them before releasing.' }
    Invoke-Native git @('rev-parse', '--verify', "$Branch^{commit}") -Capture | Out-Null
    $currentBranch = (Invoke-Native git @('rev-parse', '--abbrev-ref', 'HEAD') -Capture | Select-Object -First 1)
    $previousBranch = if ($PreviousBranch) { $PreviousBranch } else { $currentBranch }
    if ($previousBranch -eq $Branch) { throw "Previous branch equals the release branch; pass -PreviousBranch <branch to roll back to>." }
    Ok "Git tree clean on $currentBranch; release branch $Branch present; rollback branch $previousBranch"
    $compose = Get-RunningComposeArgs
    Ok "Running compose set: $($compose.Files.Count) files"
    Test-ClockSkew
    $previousImage = Get-DotEnvValue 'GCOR_TRUST_IMAGE'
    if (-not $previousImage) { throw 'GCOR_TRUST_IMAGE is not set in .env' }
    foreach ($name in @('EMBEDDING_BACKEND', 'EMBEDDING_MODEL', 'EMBEDDING_DIMS')) {
        if (-not (Get-DotEnvValue $name)) { throw "$name is not set in .env; set it explicitly (the release changed Compose defaults)." }
    }
    Ok "Embeddings pinned in .env: $(Get-DotEnvValue 'EMBEDDING_MODEL') ($(Get-DotEnvValue 'EMBEDDING_DIMS')-d)"
    $evidence = Join-Path $evidenceRoot "release-20261026-$stamp"
    New-Item -ItemType Directory -Path $evidence -Force | Out-Null
    [ordered]@{ created_at = $stamp; previous_trust_image = $previousImage; new_trust_image = $Tag; previous_branch = $previousBranch;
                release_branch = $Branch; compose_files = $compose.Files } |
        ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $evidence 'rollback.json') -Encoding utf8
    Ok "Rollback record: $evidence\rollback.json"

    if (-not $SkipBackup) {
        Step 'Verified encrypted backup'
        & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'backup-stack.ps1')
        if ($LASTEXITCODE -ne 0) { throw 'Backup failed; nothing was changed.' }
        Ok 'Backup completed'
    } else { Warn 'Backup skipped by request' }

    Step "Check out $Branch"
    if ($currentBranch -ne $Branch) { Invoke-Native git @('checkout', $Branch) }
    $head = Invoke-Native git @('log', '--oneline', '-1') -Capture | Select-Object -First 1
    Ok "HEAD: $head"
    $compose = Get-RunningComposeArgs
    Invoke-Native docker ($compose.Args + @('config', '--quiet')); Ok 'Compose configuration valid on the release branch'

    try {
        Step "Build $Tag"
        Invoke-Native docker @('build', '-f', 'proxy/Dockerfile', '-t', $Tag, '.')
        Ok 'Image built'

        Step 'Release unit gate inside the new image (no network)'
        $gateEnv = @('-e', 'POSTGRES_USER=gate', '-e', 'POSTGRES_PASSWORD=gate', '-e', 'POSTGRES_DB=gate', '-e', 'MINIO_ENDPOINT=minio:9000',
            '-e', 'MINIO_ROOT_USER=gate', '-e', 'MINIO_ROOT_PASSWORD=gate', '-e', 'INGEST_WEBHOOK_SECRET=gate', '-e', 'GCOR_PUBLIC_ORIGIN=http://127.0.0.1:5011', '-e', 'PYTHONPATH=/app')
        Invoke-Native docker (@('run', '--rm', '--network', 'none') + $gateEnv + @($Tag, 'python', '-m', 'unittest',
            'tests.test_enterprise_access', 'tests.test_buzz_chat', 'tests.test_buzz_synthesis',
            'tests.test_ingest_governance', 'tests.test_retrieval_quality', 'tests.test_unanswered'))
        # tests.test_production_safeguards reads Compose files that are not in the image; CI runs it on the repository.
        Ok 'Access, chat, synthesis, governance, retrieval and unanswered-queue tests passed in the release image'
    } catch {
        Warn "Build or gate failed: $($_.Exception.Message). Returning to $previousBranch; nothing was deployed."
        Invoke-Native git @('checkout', $previousBranch)
        throw
    }

    Step 'Database migrations (additive)'
    Invoke-Native docker ($compose.Args + @('run', '--rm', '--no-deps', 'gcor-migrate'))
    $ledger = @(Invoke-Psql "SELECT filename FROM gcor.schema_migrations WHERE filename IN ('0016_retrieval_performance.sql','0017_unanswered_questions.sql') ORDER BY 1")
    if (($ledger | Where-Object { $_ -like '00*' }).Count -ne 2) { throw "Migrations 0016/0017 not recorded (ledger: $($ledger -join ', '))" }
    Ok 'Migrations 0016 and 0017 recorded'
    $vector = @(Invoke-Psql "SELECT extversion || '|' || (SELECT default_version FROM pg_available_extensions WHERE name='vector') FROM pg_extension WHERE extname='vector'") | Select-Object -First 1
    $installed, $available = "$vector" -split '\|', 2
    if ($installed -and $available -and $installed -ne $available -and -not $SkipVectorUpdate) {
        Invoke-Psql 'ALTER EXTENSION vector UPDATE' | Out-Null
        Ok "pgvector updated $installed -> $available"
    } else { Ok "pgvector $installed (image default $available)" }
    $nowVector = "$((Invoke-Psql "SELECT extversion FROM pg_extension WHERE extname='vector'") | Select-Object -First 1)".Trim()
    if ($nowVector -match '^(\d+)\.(\d+)' -and ([int]$Matches[1] -eq 0 -and [int]$Matches[2] -lt 8)) {
        Warn "pgvector $nowVector < 0.8: iterative index scans stay off (HNSW_ITERATIVE_SCAN=auto). Everything else works."
    }

    Step 'Deploy (gcor-proxy, gcor-enterprise, buzz-knowledge only)'
    Set-DotEnvValue 'GCOR_TRUST_IMAGE' $Tag
    try {
        Wait-Healthy $compose
        Ok 'Services healthy on the new image'
    } catch {
        Warn "Deployment unhealthy: $($_.Exception.Message). Rolling back to $previousImage"
        Set-DotEnvValue 'GCOR_TRUST_IMAGE' $previousImage
        Invoke-Native git @('checkout', $previousBranch)
        Wait-Healthy (Get-RunningComposeArgs)
        throw 'Release rolled back; the previous image and branch are active again.'
    }

    Step 'Verify'
    foreach ($port in @(5001, 5011)) {
        $ready = (Invoke-Native curl.exe @('-s', "http://127.0.0.1:$port/health/ready") -Capture -AllowFailure) -join ''
        if ($ready -notmatch '"status":"ready"') { throw "Port $port not ready: $ready" }
        $models = if ($ready -match '"models":(\{[^}]*\})') { $Matches[1] } else { 'n/a' }
        Ok "127.0.0.1:$port ready; models $models"
        if ($ready -match '"embedding":"(missing|unavailable)"') { Warn 'Embedding model not available: retrieval falls back to keywords until Ollama has it.' }
    }
    $tables = @(Invoke-Psql "SELECT to_regclass('gcor.unanswered_questions') IS NOT NULL, (SELECT count(*) FROM gcor.documents WHERE channel_id IS NOT NULL)")
    Ok "Unanswered-question table present; documents with channel column populated: $(($tables | Select-Object -First 1) -split '\|' | Select-Object -Last 1)"
    Test-ClockSkew
    if ((Get-Command node -ErrorAction SilentlyContinue) -and (Test-Path $agentCli)) {
        $bytes = New-Object byte[] 32; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $env:BUZZ_PRIVATE_KEY = -join ($bytes | ForEach-Object { $_.ToString('x2') })
        $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
        try { $probe = (& node $agentCli unanswered --channel 00000000-0000-4000-8000-000000000000 2>&1 | Out-String).Trim() }
        finally { $ErrorActionPreference = $previous; Remove-Item Env:\BUZZ_PRIVATE_KEY -ErrorAction SilentlyContinue }
        if ($probe -match 'Not permitted|403|membership') { Ok 'Signed agent request reached the new unanswered-question endpoint and was refused for a non-member (as designed)' }
        else { Warn "Unexpected agent probe result: $probe" }
    }

    Step 'Done'
    Write-Host @"
    Released $Tag from $Branch (previous: $previousImage on $previousBranch).
    Evidence and rollback record: $evidence

    Try it in Buzz:
      * Ask something the brain cannot answer, then open the workspace (http://127.0.0.1:5011/workspace)
        > Unanswered questions. Answer it from there or dismiss it.
      * In a discussion thread, reply with:  !knowledge synthesize thread
      * Agents: re-run 'claude mcp list' - gcor-knowledge now also offers unanswered_questions.
      * Syntheses started before this release report 'stale'; resubmit them.

    Next: build the real evaluation set (docs\evaluation-runbook.md).
    Rollback: .\scripts\release-20261026.ps1 -Rollback
"@
}
finally { Pop-Location }
