[CmdletBinding(SupportsShouldProcess)]
param(
    # Push main to this remote after committing. Use -NoPush to review first.
    [switch]$NoPush,
    # Remote name to push to (default origin). With -RemoteUrl, the remote is created if missing.
    [string]$Remote = 'origin',
    [string]$RemoteUrl,
    # Folder that receives non-repository files found in the checkout (defaults to Documents).
    [string]$StrayFileDestination = (Join-Path ([Environment]::GetFolderPath('UserProfile')) 'Documents')
)

# Commits the September 2026 working tree in reviewable slices and pushes it.
# Safe to re-run: slices with nothing staged are skipped, hygiene steps are
# idempotent, and nothing is force-pushed.

Set-StrictMode -Version Latest
# Native commands (git) report through exit codes, and git writes ordinary progress to
# stderr. Windows PowerShell 5.1 turns any stderr line into a terminating error when
# $ErrorActionPreference is Stop, so keep it at Continue and check $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
$repoRoot = Split-Path -Parent $PSScriptRoot

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments)][string[]]$GitArgs)
    $output = @(& git @GitArgs 2>&1 | ForEach-Object { "$_" })
    if ($LASTEXITCODE -ne 0) { throw "git $($GitArgs -join ' ') failed:`n$($output -join "`n")" }
    return $output
}

function Test-GitTracked([string]$Path) {
    & git ls-files --error-unmatch -- $Path *>$null
    return ($LASTEXITCODE -eq 0)
}

function Get-GitLines {
    param([Parameter(ValueFromRemainingArguments)][string[]]$GitArgs)
    # Query helper: returns stdout lines, never throws on a non-zero exit.
    return @(& git @GitArgs 2>$null | ForEach-Object { "$_" } | Where-Object { $_ -ne '' })
}

function Add-Slice {
    param([string]$Subject, [string[]]$Body, [string[]]$Paths, [switch]$Everything)
    if ($Everything) {
        Invoke-Git add -A | Out-Null
    }
    else {
        $existing = @($Paths | Where-Object { Test-Path -LiteralPath $_ })
        # Deleted-but-tracked paths still need staging.
        $deleted = @($Paths | Where-Object { -not (Test-Path -LiteralPath $_) } | Where-Object { Test-GitTracked $_ })
        if ($existing.Count -gt 0) { Invoke-Git add -A -- @existing | Out-Null }
        if ($deleted.Count -gt 0) { Invoke-Git add -A -- @deleted | Out-Null }
    }
    $staged = @(Get-GitLines diff --cached --name-only)
    if ($staged.Count -eq 0) { Write-Host "skip  $Subject (nothing to commit)"; return }
    $message = @($Subject, '') + $Body
    if ($PSCmdlet.ShouldProcess("$($staged.Count) files", "commit '$Subject'")) {
        $tmp = New-TemporaryFile
        Set-Content -LiteralPath $tmp -Value ($message -join "`n") -Encoding utf8 -ErrorAction Stop
        Invoke-Git commit --quiet --file $tmp | Out-Null
        Remove-Item -LiteralPath $tmp -ErrorAction SilentlyContinue
        Write-Host ("commit {0}  {1}  ({2} files)" -f ((Get-GitLines rev-parse --short HEAD) -join ''), $Subject, $staged.Count)
    }
}

Push-Location $repoRoot
try {
    # --- preflight -----------------------------------------------------------
    if (((Get-GitLines rev-parse --is-inside-work-tree) -join '') -ne 'true') { throw "Not a git checkout: $repoRoot" }
    $branch = ((Get-GitLines rev-parse --abbrev-ref HEAD) -join '').Trim()
    if ($branch -ne 'main') { throw "Expected branch main, found '$branch'." }
    if (Test-Path -LiteralPath (Join-Path $repoRoot '.git/MERGE_HEAD')) { throw 'A merge is in progress; resolve it first.' }
    if (Test-Path -LiteralPath (Join-Path $repoRoot '.git/rebase-merge')) { throw 'A rebase is in progress; resolve it first.' }
    $knownRemotes = @(Get-GitLines remote)
    if ($Remote -notin $knownRemotes) {
        if ([string]::IsNullOrWhiteSpace($RemoteUrl)) { throw "Remote '$Remote' does not exist; pass -RemoteUrl to create it." }
        Invoke-Git remote add $Remote $RemoteUrl | Out-Null
        Write-Host "added remote $Remote -> $RemoteUrl"
    }
    elseif (-not [string]::IsNullOrWhiteSpace($RemoteUrl)) {
        Invoke-Git remote set-url $Remote $RemoteUrl | Out-Null
    }
    $resolvedUrl = ((Get-GitLines remote get-url $Remote) -join '').Trim()
    Write-Host "Repository $repoRoot on $branch -> $Remote ($resolvedUrl)"

    # --- hygiene: files that must not enter the release --------------------
    $strays = @(Get-ChildItem -LiteralPath $repoRoot -File -Filter '*.pdf')
    foreach ($stray in $strays) {
        if (Test-GitTracked $stray.Name) { continue } # tracked on purpose
        New-Item -ItemType Directory -Path $StrayFileDestination -Force -ErrorAction Stop | Out-Null
        $target = Join-Path $StrayFileDestination $stray.Name
        if ($PSCmdlet.ShouldProcess($stray.Name, "move out of the repository to $StrayFileDestination")) {
            Move-Item -LiteralPath $stray.FullName -Destination $target -Force -ErrorAction Stop
            Write-Host "moved  $($stray.Name) -> $target"
        }
    }
    foreach ($obsolete in @('.env copy.example', 'ISSUES_FIXED.txt')) {
        $path = Join-Path $repoRoot $obsolete
        if (-not (Test-Path -LiteralPath $path)) { continue }
        if ($PSCmdlet.ShouldProcess($obsolete, 'remove obsolete file')) {
            if (Test-GitTracked $obsolete) { Invoke-Git rm --quiet -- $obsolete | Out-Null }
            else { Remove-Item -LiteralPath $path -Force -ErrorAction Stop }
            Write-Host "removed $obsolete"
        }
    }
    # GitHub workflow files cannot be written remotely; install the pending copy.
    $pendingWorkflow = Join-Path $repoRoot 'scripts/ci.yml.pending'
    if (Test-Path -LiteralPath $pendingWorkflow) {
        if ($PSCmdlet.ShouldProcess('.github/workflows/ci.yml', 'install pending CI workflow')) {
            New-Item -ItemType Directory -Path (Join-Path $repoRoot '.github/workflows') -Force -ErrorAction Stop | Out-Null
            Move-Item -LiteralPath $pendingWorkflow -Destination (Join-Path $repoRoot '.github/workflows/ci.yml') -Force -ErrorAction Stop
            Write-Host 'installed scripts/ci.yml.pending -> .github/workflows/ci.yml'
        }
    }
    # The guarded-rollout overlay moved out of backups/. Keep the live pin working.
    $legacyOverlay = Join-Path $repoRoot 'backups/gcor-safeguards.override.json'
    $liveOverlay = Join-Path $repoRoot 'docker-compose.safeguards.override.json'
    if ((Test-Path -LiteralPath $legacyOverlay) -and -not (Test-Path -LiteralPath $liveOverlay)) {
        Copy-Item -LiteralPath $legacyOverlay -Destination $liveOverlay -ErrorAction Stop
        Write-Host 'copied backups/gcor-safeguards.override.json -> docker-compose.safeguards.override.json (gitignored)'
    }

    # --- slices --------------------------------------------------------------
    Add-Slice -Subject 'Fix the first CI run: proxy image context, Trivy pin, secret-scan allowlist' -Body @(
        'The proxy image copies clients/buzz-knowledge.mjs, so .dockerignore excludes only',
        'clients/buzz-desktop (the Node test project) instead of the whole clients tree.',
        'aquasecurity/trivy-action is pinned to the immutable v0.36.0 commit; the 0.32.0',
        'reference lacked its v prefix. .gitleaks.toml allowlists documented placeholders',
        '(change-me values, CHANGE_ME_ templates, npub1example identities, synthetic test',
        'credentials) while keeping the default rule set for everything else.',
        '',
        'Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>',
        'Claude-Session: https://claude.ai/code/session_01AW5NhXzSpGjbEGwFpHgfuw'
    ) -Paths @('.dockerignore', '.github/workflows/ci.yml', '.gitleaks.toml')

    Add-Slice -Subject 'Add transactional governance outbox, egress policy and production safeguards' -Body @(
        'Migration 0007 adds the governance outbox; approvals now commit state and an',
        'outbox event in one transaction and publish MinIO artifacts asynchronously.',
        'Shared streaming downloader enforces URL policy on every redirect hop and',
        'bounds remote bodies. Request-size middleware, /health/live and /health/ready,',
        'guarded proxy-only rollout with image rollback, MCP get_stack_health and the',
        '7 September production readiness review.'
    ) -Paths @(
        'migrations/0007_governance_outbox.sql',
        'proxy/egress.py', 'proxy/governance_outbox.py', 'proxy/request_limits.py',
        'recovery-controller/Dockerfile', 'recovery-controller/test_main.py', 'mcp-postgres-gcor',
        'tests/test_archival_helpers.py', 'tests/test_production_safeguards.py',
        'tests/test_governance_egress.py', 'tests/test_mcp_governance.py',
        'tests/integration/smoke.py', 'tests/integration/governance_outage.py',
        'tests/test_guarded_rollout.ps1', 'scripts/deploy-gcor-safeguards.ps1',
        'docs/governance-and-egress.md', 'docs/production-readiness-review-2026-09-07.md',
        '.gitignore', '.dockerignore'
    )

    Add-Slice -Subject 'Add Buzz identity boundary, enterprise workflows and knowledge workspace' -Body @(
        'Migration 0008 adds enterprise workflow tables. Signed Nostr request validation,',
        'live channel membership checks, document reader restrictions, scoped access',
        'policy, hybrid retrieval with lexical fallback, optional Graphiti retrieval,',
        'the review workspace UI, restore drill and knowledge evaluation tooling.'
    ) -Paths @(
        'migrations/0008_enterprise_workflows.sql',
        'proxy/main.py', 'proxy/access_policy.py', 'proxy/nostr_auth.py', 'proxy/document_parsing.py',
        'proxy/enterprise_workflows.py', 'proxy/governance_service.py',
        'proxy/workspace', 'proxy/requirements.txt', 'proxy/requirements.lock', 'proxy/Dockerfile',
        'projector', 'clients/buzz-knowledge.mjs',
        'tests/test_enterprise_access.py', 'tests/test_enterprise_workflows.py', 'tests/test_evaluation.py',
        'tests/test_buzz_client.mjs',
        'tests/integration/enterprise_boundaries.py',
        'scripts/evaluate-knowledge.py', 'scripts/restore-drill.py', 'scripts/restore-drill.ps1',
        'docker-compose.enterprise.yml',
        'docs/enterprise-pilot-setup.md', 'docs/enterprise-knowledge-readiness-2026-09-08.md', 'docs/enterprise-workflows.md'
    )

    Add-Slice -Subject 'Add Buzz Knowledge chat agent and desktop knowledge cards' -Body @(
        'Migration 0009 adds knowledge command tables. The buzz-knowledge service',
        'answers !knowledge commands in enrolled channels with synthesis, proposals,',
        'human approval, revisions and cited answers; Buzz Desktop renders sanitized',
        'knowledge cards with review controls. Includes the real-relay integration test.'
    ) -Paths @(
        'migrations/0009_buzz_knowledge_commands.sql',
        'proxy/buzz_chat.py', 'proxy/buzz_wiki.py', 'proxy/buzz_cards.py',
        'clients/buzz-desktop',
        'tests/test_buzz_chat.py', 'tests/test_buzz_cards.py',
        'tests/integration/buzz-relay.compose.yml',
        'docker-compose.buzz.yml',
        'scripts/setup-buzz-knowledge.py', 'scripts/check-buzz-knowledge.py',
        'scripts/prepare-buzz-desktop-cards.ps1', 'scripts/build-buzz-desktop-windows.ps1',
        'docs/buzz-chat-knowledge.md', 'docs/buzz-knowledge-cards.md', 'docs/buzz-living-knowledge.md', 'docs/collaborative-knowledge-objective.md'
    )

    Add-Slice -Subject 'Enforce knowledge evidence validity at read time' -Body @(
        'Migration 0010 adds gcor.knowledge_evidence_current(); hybrid retrieval, graph',
        'expansion, Graphiti candidate resolution, the enterprise document list/reader,',
        'revise, save and dependency approval all apply it, so deleted source messages,',
        'superseded or restricted dependencies and stale revisions hide dependent approved',
        'knowledge immediately, recursively, without waiting for the Knowledge worker.',
        'The worker reuses the same predicate for durable withdrawal. The isolated chat',
        'lifecycle test deletes a source with the worker idle and asserts the two-hop',
        'dependent answer is hidden while both rows still read approved.',
        '',
        'Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>',
        'Claude-Session: https://claude.ai/code/session_01AW5NhXzSpGjbEGwFpHgfuw'
    ) -Paths @(
        'migrations/0010_knowledge_evidence_validity.sql', 'tests/integration/buzz_chat_lifecycle.py',
        'docs/buzz-living-knowledge.md', 'docs/buzz-chat-knowledge.md'
    )

    Add-Slice -Subject 'Run services as a restricted PostgreSQL role with row-level security' -Body @(
        'Migration 0011 creates gcor_app (data access on gcor.*, read-only relay tables,',
        'no DDL, NOSUPERUSER, NOBYPASSRLS, ledger read-only). gcor-migrate creates the',
        'GCOR_DB_USER login role from migrations/runtime-role.psql after each pass, and',
        'every runtime service connects as it when GCOR_DB_USER/GCOR_DB_PASSWORD are set.',
        'Migration 0012 adds channel/access-level RLS on documents, chunks and nodes;',
        'proxy/db_scope.py tags pooled connections with the signed identity scope so the',
        'policies bind to the caller. The integration stack runs the whole lifecycle suite',
        'as that role and rls_scope.py proves isolation with unfiltered SQL; fixtures that',
        'seed relay tables or alter constraints use POSTGRES_ADMIN_USER.',
        '',
        'Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>',
        'Claude-Session: https://claude.ai/code/session_01AW5NhXzSpGjbEGwFpHgfuw'
    ) -Paths @(
        'migrations/0011_runtime_roles.sql', 'migrations/runtime-role.psql', 'tests/integration/admin_db.py',
        'migrations/0012_row_level_security.sql', 'proxy/db_scope.py', 'tests/integration/rls_scope.py',
        'tests/integration/nostr_membership.py', 'tests/integration/workspace_lifecycle.py',
        'tests/integration/buzz_relay_lifecycle.py', 'tests/integration/governance_faults.py',
        'docs/production-safeguards.md'
    )

    Add-Slice -Subject 'Reduce infrastructure privileges: scoped object keys, split networks, socket proxy' -Body @(
        'minio-init creates a GCOR service user limited to the GCOR bucket and prefixed',
        'channel buckets when GCOR_S3_* and GCOR_CHANNEL_BUCKET_PREFIX are set; the proxy',
        'uses it instead of the root keys and tolerates buckets it cannot administer.',
        'PostgreSQL, Redis, MinIO and FalkorDB move to an internal data-net; the Docker',
        'socket is held only by docker-socket-proxy (CONTAINERS=1 ALLOW_RESTARTS=1 POST=0)',
        'on an internal control-net, and the recovery controller speaks to it over TCP.',
        '',
        'Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>',
        'Claude-Session: https://claude.ai/code/session_01AW5NhXzSpGjbEGwFpHgfuw'
    ) -Paths @(
        'recovery-controller/main.py', 'docs/adr/0003-bounded-stack-recovery-controller.md'
    )

    Add-Slice -Everything -Subject 'Make the production release baseline reproducible' -Body @(
        'docker-compose.production.yml is standalone again: the Alertmanager receiver',
        'override moves to docker-compose.observability-production.yml. The guarded',
        'rollout overlay moves from gitignored backups/ to a root-level gitignored file',
        'with a tracked example. gcor-migrate records applied files in',
        'gcor.schema_migrations and skips unchanged ones. CI validates every documented',
        'overlay combination and runs the desktop card view tests through a pinned',
        'package.json. Adds the 9 September readiness assessment and this commit script.',
        '',
        'Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>',
        'Claude-Session: https://claude.ai/code/session_01AW5NhXzSpGjbEGwFpHgfuw'
    )

    $remaining = @(Get-GitLines status --porcelain)
    if ($remaining.Count -gt 0) { Write-Warning "Unexpected leftovers after slicing:`n$($remaining -join "`n")" }

    # --- push ----------------------------------------------------------------
    & git fetch --quiet $Remote main *>$null   # tolerate an empty new repository
    $upstream = "$Remote/main"
    & git rev-parse --verify --quiet $upstream *>$null
    if ($LASTEXITCODE -eq 0) { $ahead = [int]((Get-GitLines rev-list --count "$upstream..HEAD") -join '') }
    else { $ahead = [int]((Get-GitLines rev-list --count HEAD) -join '') }
    Write-Host "main is $ahead commit(s) ahead of $upstream."
    if ($NoPush) { Write-Host "Skipping push (-NoPush). Run: git push -u $Remote main"; return }
    if ($ahead -gt 0 -and $PSCmdlet.ShouldProcess("$Remote main", 'push')) {
        Invoke-Git push -u $Remote main | Out-Null
        Write-Host "Pushed. CI: $($resolvedUrl -replace '\.git$','')/actions"
    }
}
finally { Pop-Location }
