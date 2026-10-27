<#
.SYNOPSIS
  Release 2026-10-27: everything in release 2026-10-26 (production-blocker fixes, retrieval
  quality, knowledge-loop sprint) plus bi-temporal facts (#18) and the PostgreSQL relationship
  projector that produces them. Buzz relay, Postgres, Redis, MinIO and Ollama are not recreated.

.DESCRIPTION
  1. Preflight: Docker, disk, clean git tree, running compose set, clock skew, pinned embeddings,
     generation model.
  2. Rollback record, then a verified encrypted backup (scripts/backup-stack.ps1) unless -SkipBackup.
  3. Provision the relationship projector's database login in .env (generated secret) if missing.
  4. Check out the release branch (default release-20261027).
  5. Build the knowledge image and run the release unit gate inside it (no network); build the
     relationship projector image and run its unit tests inside it (no network).
  6. Migrations 0016/0017/0018 (additive) with ledger check; pgvector report/update.
  7. Recreate gcor-proxy, gcor-enterprise and buzz-knowledge on the new image.
  8. Relationship migrations (0015/0016/0018, idempotent) through the running Postgres container,
     then the projector login role (member of gcor_relationship_projector only).
  9. Start gcor-relationship-projector and confirm it runs with temporal facts enabled.
 10. Verify: readiness and models, ledger, facts table, projector state, signed agent probe.

  Any failure from step 7 on rolls the whole release back (projector removed, previous image and
  branch restored). Migrations are additive and stay; older images ignore them.

  Rollback later: .\scripts\release-20261027.ps1 -Rollback
#>
[CmdletBinding()]
param(
    [string]$Branch = 'release-20261027',
    [string]$Tag = 'gbuzz-knowledge-durable:release-20261027',
    [string]$PreviousBranch = '',
    [switch]$SkipBackup,
    [switch]$SkipVectorUpdate,
    [switch]$Rollback,
    [int]$WaitTimeoutSeconds = 300,
    [int]$ProjectorSettleSeconds = 60,
    [int]$MinFreeGB = 8
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot '.env'
$services = @('gcor-proxy', 'gcor-enterprise', 'buzz-knowledge')
$projectorService = 'gcor-relationship-projector'
$projectorContainer = 'gbuzz-gcor-relationship-projector-1'
$relationshipsFile = 'docker-compose.relationships.yml'
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

function Get-PlanProfiles {
    $plan = Get-Content (Join-Path $repoRoot 'config\windows-startup.compose-files.json') -Raw | ConvertFrom-Json
    return @($plan.profiles)
}

# Compose arguments for an explicit file list (relative or absolute) plus the startup plan's profiles.
function New-ComposeArgs([string[]]$Files, [switch]$WithRelationships) {
    $composeArgs = @('compose', '--project-name', 'gbuzz')
    $all = @($Files)
    if ($WithRelationships -and -not ($all | Where-Object { $_ -like "*$relationshipsFile" })) { $all += (Join-Path $repoRoot $relationshipsFile) }
    foreach ($f in $all) { $composeArgs += @('-f', $f) }
    $profiles = @(Get-PlanProfiles)
    if ($WithRelationships -and $profiles -notcontains 'relationship-projection') { $profiles += 'relationship-projection' }
    foreach ($p in $profiles) { $composeArgs += @('--profile', $p) }
    return [pscustomobject]@{ Args = $composeArgs; Files = $all }
}

# The running stack is authoritative (the startup plan has drifted before).
function Get-RunningComposeFiles {
    $inspect = (Invoke-Native docker @('inspect', 'gbuzz-gcor-proxy-1') -Capture) -join "`n" | ConvertFrom-Json
    $label = @($inspect)[0].Config.Labels.'com.docker.compose.project.config_files'
    $files = @(("$label".Trim()) -split ',' | Where-Object { $_ })
    if (-not $files.Count) { throw 'Cannot read the compose file set from gbuzz-gcor-proxy-1. Is the stack running?' }
    if (-not ($files | Where-Object { $_ -like '*knowledge-trust*' })) { throw 'Running stack is not on the knowledge-trust overlay; refusing to release into an unknown baseline.' }
    return $files
}

function Wait-Healthy([object]$Compose) {
    Invoke-Native docker ($Compose.Args + @('up', '-d', '--no-deps', '--wait', '--wait-timeout', "$WaitTimeoutSeconds") + $services)
}

function Invoke-Psql([string]$Sql) {
    $user = Get-DotEnvValue 'POSTGRES_USER'; $db = Get-DotEnvValue 'POSTGRES_DB'
    return Invoke-Native docker @('exec', '-i', 'gbuzz-postgres-1', 'psql', '-U', $user, '-d', $db, '-v', 'ON_ERROR_STOP=1', '-qtAF', '|', '-c', $Sql) -Capture
}

# Run a SQL file inside the running Postgres container (no extra image pull).
function Invoke-PsqlFile([string]$Path, [string[]]$Variables = @()) {
    $user = Get-DotEnvValue 'POSTGRES_USER'; $db = Get-DotEnvValue 'POSTGRES_DB'
    $arguments = @('exec', '-i', 'gbuzz-postgres-1', 'psql', '-U', $user, '-d', $db, '-v', 'ON_ERROR_STOP=1', '-q')
    foreach ($v in $Variables) { $arguments += @('-v', $v) }
    $arguments += @('-f', '-')
    $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
    try {
        Get-Content -LiteralPath $Path -Raw | & docker @arguments 2>&1 | ForEach-Object {
            $line = "$_"; if ($line -and $line -notmatch 'NOTICE|RemoteException') { Write-Host "      $line" } }
        $code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    if ($code -ne 0) { throw "psql $([IO.Path]::GetFileName($Path)) failed (exit $code)" }
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

function Invoke-ReleaseRollback([object]$Record) {
    Invoke-Native docker @('rm', '-f', $projectorContainer) -AllowFailure | Out-Null
    Ok 'Relationship projector stopped and removed'
    Set-DotEnvValue 'GCOR_TRUST_IMAGE' $Record.previous_trust_image
    Invoke-Native git @('checkout', $Record.previous_branch)
    Wait-Healthy (New-ComposeArgs @($Record.compose_files))
    Ok "Restored GCOR_TRUST_IMAGE=$($Record.previous_trust_image) and branch $($Record.previous_branch). Migrations 0016-0018 are additive and stay; older images ignore them."
}

# Full console transcript next to the backups, so the run can be reviewed (and followed) afterwards.
New-Item -ItemType Directory -Path $evidenceRoot -Force | Out-Null
$transcript = Join-Path $evidenceRoot "release-20261027-$stamp.transcript.log"
Start-Transcript -LiteralPath $transcript -Force | Out-Null
Push-Location $repoRoot
try {
    if ($Rollback) {
        Step 'Rolling back release 2026-10-27'
        $recordPath = Get-ChildItem $evidenceRoot -Directory -Filter 'release-20261027-*' | Sort-Object Name -Descending |
            ForEach-Object { Join-Path $_.FullName 'rollback.json' } | Where-Object { Test-Path $_ } | Select-Object -First 1
        if (-not $recordPath) { throw 'No release-20261027 rollback record found.' }
        Invoke-ReleaseRollback (Get-Content $recordPath -Raw | ConvertFrom-Json)
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
    $runningFiles = Get-RunningComposeFiles
    Ok "Running compose set: $($runningFiles.Count) files"
    Test-ClockSkew
    $previousImage = Get-DotEnvValue 'GCOR_TRUST_IMAGE'
    if (-not $previousImage) { throw 'GCOR_TRUST_IMAGE is not set in .env' }
    foreach ($name in @('EMBEDDING_BACKEND', 'EMBEDDING_MODEL', 'EMBEDDING_DIMS')) {
        if (-not (Get-DotEnvValue $name)) { throw "$name is not set in .env; set it explicitly (the release changed Compose defaults)." }
    }
    Ok "Embeddings pinned in .env: $(Get-DotEnvValue 'EMBEDDING_MODEL') ($(Get-DotEnvValue 'EMBEDDING_DIMS')-d)"
    $relationshipModel = if (Get-DotEnvValue 'RELATIONSHIP_MODEL') { Get-DotEnvValue 'RELATIONSHIP_MODEL' } else { 'qwen2.5:1.5b' }
    $tags = (Invoke-Native docker @('exec', 'gbuzz-ollama-1', 'ollama', 'list') -Capture -AllowFailure) -join "`n"
    if ($tags -notmatch [regex]::Escape($relationshipModel)) {
        throw "Ollama does not have $relationshipModel (needed by the relationship projector). Pull it first: docker exec gbuzz-ollama-1 ollama pull $relationshipModel"
    }
    Ok "Ollama has $relationshipModel for fact extraction"
    $evidence = Join-Path $evidenceRoot "release-20261027-$stamp"
    New-Item -ItemType Directory -Path $evidence -Force | Out-Null
    $record = [pscustomobject][ordered]@{ created_at = $stamp; previous_trust_image = $previousImage; new_trust_image = $Tag;
        previous_branch = $previousBranch; release_branch = $Branch; compose_files = $runningFiles }
    $record | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $evidence 'rollback.json') -Encoding utf8
    Ok "Rollback record: $evidence\rollback.json"

    # Before the backup: backup-stack.ps1 renders the startup plan, which on the release branch
    # includes the relationships overlay and therefore needs these values.
    Step 'Relationship projector database login'
    if (-not (Get-DotEnvValue 'GCOR_RELATIONSHIP_DB_USER')) { Set-DotEnvValue 'GCOR_RELATIONSHIP_DB_USER' 'gcor_relationship_runtime' }
    if (-not (Get-DotEnvValue 'GCOR_RELATIONSHIP_DB_PASSWORD')) {
        $bytes = New-Object byte[] 32; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        Set-DotEnvValue 'GCOR_RELATIONSHIP_DB_PASSWORD' (-join ($bytes | ForEach-Object { $_.ToString('x2') }))
        Ok 'Generated a new secret for the relationship projector login (stored in .env only)'
    } else { Ok 'Relationship projector login already configured in .env' }
    $relationshipUser = Get-DotEnvValue 'GCOR_RELATIONSHIP_DB_USER'
    if ($relationshipUser -notmatch '^[a-z_][a-z0-9_]{0,62}$') { throw "GCOR_RELATIONSHIP_DB_USER must be a plain lower-case identifier: $relationshipUser" }

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
    $compose = New-ComposeArgs $runningFiles -WithRelationships
    try {
        Invoke-Native docker ($compose.Args + @('config', '--quiet')); Ok 'Compose configuration valid on the release branch (with the relationships overlay)'

        Step "Build $Tag"
        Invoke-Native docker @('build', '-f', 'proxy/Dockerfile', '-t', $Tag, '.')
        Ok 'Image built'

        Step 'Release unit gate inside the new image (no network)'
        $gateEnv = @('-e', 'POSTGRES_USER=gate', '-e', 'POSTGRES_PASSWORD=gate', '-e', 'POSTGRES_DB=gate', '-e', 'MINIO_ENDPOINT=minio:9000',
            '-e', 'MINIO_ROOT_USER=gate', '-e', 'MINIO_ROOT_PASSWORD=gate', '-e', 'INGEST_WEBHOOK_SECRET=gate', '-e', 'GCOR_PUBLIC_ORIGIN=http://127.0.0.1:5011', '-e', 'PYTHONPATH=/app')
        Invoke-Native docker (@('run', '--rm', '--network', 'none') + $gateEnv + @($Tag, 'python', '-m', 'unittest',
            'tests.test_enterprise_access', 'tests.test_buzz_chat', 'tests.test_buzz_synthesis',
            'tests.test_ingest_governance', 'tests.test_retrieval_quality', 'tests.test_unanswered', 'tests.test_temporal_facts'))
        Ok 'Access, chat, synthesis, governance, retrieval, unanswered-queue and temporal-facts tests passed in the release image'

        Step 'Build the relationship projector and test it inside its image (no network)'
        Invoke-Native docker ($compose.Args + @('build', $projectorService))
        Invoke-Native docker @('run', '--rm', '--network', 'none', "gbuzz-$projectorService", 'python', '-m', 'unittest', 'test_main')
        Ok 'Relationship projector image built; its unit tests passed'
    } catch {
        Warn "Build or gate failed: $($_.Exception.Message). Returning to $previousBranch; nothing was deployed."
        Invoke-Native git @('checkout', $previousBranch)
        throw
    }

    Step 'Database migrations (additive)'
    Invoke-Native docker ($compose.Args + @('run', '--rm', '--no-deps', 'gcor-migrate'))
    $ledger = @(Invoke-Psql "SELECT filename FROM gcor.schema_migrations WHERE filename IN ('0016_retrieval_performance.sql','0017_unanswered_questions.sql','0018_temporal_facts.sql') ORDER BY 1")
    if (($ledger | Where-Object { $_ -like '00*' }).Count -ne 3) { throw "Migrations 0016-0018 not recorded (ledger: $($ledger -join ', '))" }
    Ok 'Migrations 0016, 0017 and 0018 recorded'
    $vector = @(Invoke-Psql "SELECT extversion || '|' || (SELECT default_version FROM pg_available_extensions WHERE name='vector') FROM pg_extension WHERE extname='vector'") | Select-Object -First 1
    $installed, $available = "$vector" -split '\|', 2
    if ($installed -and $available -and $installed -ne $available -and -not $SkipVectorUpdate) {
        Invoke-Psql 'ALTER EXTENSION vector UPDATE' | Out-Null
        Ok "pgvector updated $installed -> $available"
    } else { Ok "pgvector $installed (image default $available)" }

    try {
        Step 'Deploy (gcor-proxy, gcor-enterprise, buzz-knowledge)'
        Set-DotEnvValue 'GCOR_TRUST_IMAGE' $Tag
        Wait-Healthy $compose
        Ok 'Services healthy on the new image'

        Step 'Relationship migrations and projector login'
        foreach ($file in (Get-ChildItem (Join-Path $repoRoot 'relationship-projector\migrations') -Filter '*.sql' | Sort-Object Name)) {
            Invoke-PsqlFile $file.FullName
            Ok "Applied $($file.Name)"
        }
        $roleSql = Join-Path $env:TEMP "gbuzz-relationship-role-$stamp.psql"
        @(
            "SET client_min_messages = warning;",
            "SELECT format('CREATE ROLE %I LOGIN', :'user') WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'user') \gexec",
            "SELECT format('ALTER ROLE %I LOGIN INHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %L', :'user', :'pw') \gexec",
            "SELECT format('GRANT gcor_relationship_projector TO %I', :'user') \gexec"
        ) | Set-Content -LiteralPath $roleSql -Encoding ascii
        try { Invoke-PsqlFile $roleSql @("user=$relationshipUser", "pw=$(Get-DotEnvValue 'GCOR_RELATIONSHIP_DB_PASSWORD')") }
        finally { Remove-Item -LiteralPath $roleSql -Force -ErrorAction SilentlyContinue }
        $grants = "$((Invoke-Psql "SELECT string_agg(r.rolname, ',' ORDER BY r.rolname) FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.roleid JOIN pg_roles u ON u.oid=m.member WHERE u.rolname=$("'" + $relationshipUser + "'")") | Select-Object -First 1)".Trim()
        if ($grants -ne 'gcor_relationship_projector') { throw "Relationship login $relationshipUser has unexpected memberships: '$grants'" }
        Ok "Login $relationshipUser is a member of gcor_relationship_projector only"

        Step 'Start the relationship projector (temporal facts on)'
        Invoke-Native docker ($compose.Args + @('up', '-d', '--no-deps', '--no-build', $projectorService))
        Start-Sleep -Seconds $ProjectorSettleSeconds
        $state = "$((Invoke-Native docker @('inspect', '--format', '{{.State.Status}} {{.RestartCount}}', $projectorContainer) -Capture) | Select-Object -First 1)".Trim()
        # Python logs warnings to stderr; capture both streams.
        $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
        try { $logs = (& docker logs --tail 200 $projectorContainer 2>&1 | ForEach-Object { "$_" } | Out-String) }
        finally { $ErrorActionPreference = $previous }
        $logs | Set-Content -LiteralPath (Join-Path $evidence 'relationship-projector.log') -Encoding utf8
        if ($state -notmatch '^running 0$') { throw "Relationship projector is not running cleanly ($state). Log: $evidence\relationship-projector.log" }
        if ($logs -match 'not writable|temporal facts unavailable|Traceback') { throw "Relationship projector reported a problem. Log: $evidence\relationship-projector.log" }
        Ok "Relationship projector running for $ProjectorSettleSeconds s without restarts or errors"
    } catch {
        Warn "Release failed after the build: $($_.Exception.Message)"
        Warn 'Rolling back the whole release (projector, image, branch).'
        Invoke-ReleaseRollback $record
        throw 'Release rolled back; the previous image and branch are active again.'
    }

    Step 'Verify'
    foreach ($port in @(5001, 5011)) {
        $ready = (Invoke-Native curl.exe @('-s', "http://127.0.0.1:$port/health/ready") -Capture -AllowFailure) -join ''
        if ($ready -notmatch '"status":"ready"') { Warn "Port $port not ready: $ready"; continue }
        $models = if ($ready -match '"models":(\{[^}]*\})') { $Matches[1] } else { 'n/a' }
        Ok "127.0.0.1:$port ready; models $models"
    }
    $facts = "$((Invoke-Psql "SELECT (SELECT count(*) FROM gcor.facts WHERE expired_at IS NULL) || '|' || (SELECT count(*) FROM gcor.fact_predicates) || '|' || (SELECT string_agg(status || '=' || n, ' ') FROM (SELECT status, count(*) n FROM gcor.relationship_projection GROUP BY status) s)") | Select-Object -First 1)".Trim()
    $current, $predicates, $projection = $facts -split '\|', 3
    Ok "gcor.facts: $current current fact(s); $predicates predicates in the vocabulary; projection status: $(if ($projection) { $projection } else { 'none yet' })"
    Warn 'Facts appear as approved documents are projected: about one local-model call per document, so allow some minutes.'
    Test-ClockSkew
    if ((Get-Command node -ErrorAction SilentlyContinue) -and (Test-Path $agentCli)) {
        $bytes = New-Object byte[] 32; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $env:BUZZ_PRIVATE_KEY = -join ($bytes | ForEach-Object { $_.ToString('x2') })
        $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
        try { $probe = (& node $agentCli facts --channel 00000000-0000-4000-8000-000000000000 'release process' 2>&1 | Out-String).Trim() }
        finally { $ErrorActionPreference = $previous; Remove-Item Env:\BUZZ_PRIVATE_KEY -ErrorAction SilentlyContinue }
        if ($probe -match 'Not permitted|403|membership') { Ok 'Signed agent request reached /api/facts/history and was refused for a non-member (as designed)' }
        else { Warn "Unexpected agent probe result: $probe" }
    }

    Step 'Done'
    Write-Host @"
    Released $Tag from $Branch (previous: $previousImage on $previousBranch).
    Relationship projector running with bi-temporal facts; startup plan includes it.
    Evidence and rollback record: $evidence

    Try it:
      * Agents: gcor-knowledge now also offers fact_history (re-run 'claude mcp list').
        CLI: node agent-tools\gcor-agent\dist\gcor-agent.mjs facts --channel <uuid> "<subject>"
      * /api/ask and /api/retrieve responses now include a 'facts' list.
      * Everything from release 2026-10-26: unanswered questions in the workspace,
        '!knowledge synthesize thread' in Buzz threads.

    Next: build the real evaluation set (docs\evaluation-runbook.md), including fact questions.
    Rollback: .\scripts\release-20261027.ps1 -Rollback
"@
    Write-Host 'RELEASE-20261027: SUCCESS'
}
catch {
    Write-Host "RELEASE-20261027: FAILED - $($_.Exception.Message)" -ForegroundColor Red
    throw
}
finally { Pop-Location; Stop-Transcript | Out-Null }
