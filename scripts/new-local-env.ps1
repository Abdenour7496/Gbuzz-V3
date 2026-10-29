param(
    [switch]$Force,
    [switch]$EnsureStackApiSecret
)

$envFile = Join-Path $PSScriptRoot "..\.env"
if ($EnsureStackApiSecret) {
    if (-not (Test-Path -LiteralPath $envFile)) {
        throw ".env does not exist. Run this script without -EnsureStackApiSecret to create it."
    }
    $lines = @(Get-Content -LiteralPath $envFile)
    $existingIndex = -1
    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match '^STACK_API_SECRET=') { $existingIndex = $index; break }
    }
    if ($existingIndex -ge 0 -and -not [string]::IsNullOrWhiteSpace(($lines[$existingIndex] -split '=', 2)[1])) {
        Write-Output "STACK_API_SECRET is already configured in $envFile"
        exit 0
    }
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $value = ([BitConverter]::ToString($bytes) -replace '-', '').ToLowerInvariant()
    if ($existingIndex -ge 0) { $lines[$existingIndex] = "STACK_API_SECRET=$value" }
    else { $lines += "STACK_API_SECRET=$value" }
    Set-Content -LiteralPath $envFile -Value $lines -Encoding ascii
    Write-Output "Configured a generated STACK_API_SECRET in $envFile"
    exit 0
}

if ((Test-Path $envFile) -and -not $Force) {
    throw ".env already exists. Use -Force only when you intend to replace its stable secrets."
}

$hexSecret = {
    param([int]$ByteCount)
    $bytes = New-Object byte[] $ByteCount
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    ([BitConverter]::ToString($bytes) -replace '-', '').ToLowerInvariant()
}

$base64Secret = {
    param([int]$ByteCount)
    $bytes = New-Object byte[] $ByteCount
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    [Convert]::ToBase64String($bytes).Replace('+', 'A').Replace('/', 'B').Replace('=', '')
}

$stackSecret = & $hexSecret 32
$relayPrivateKey = & $hexSecret 32
$gitHookSecret = & $hexSecret 32
$postgresPassword = & $base64Secret 32
$gcorDbPassword = & $base64Secret 32
$redisPassword = & $base64Secret 32
$s3AccessKey = "buzz$(& $hexSecret 12)"
$s3SecretKey = & $base64Secret 32
$gcorS3SecretKey = & $base64Secret 32

$content = @"
# Generated for a local Buzz + GCOR deployment. Keep this file private and stable.
BUZZ_IMAGE=ghcr.io/block/buzz:sha-7ebf1be
BUZZ_HTTP_PORT=3000
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
BUZZ_REQUIRE_AUTH_TOKEN=false
BUZZ_REQUIRE_RELAY_MEMBERSHIP=false
BUZZ_ALLOW_IP_OA_AUTH=true
BUZZ_AUTO_MIGRATE=true
BUZZ_GIT_CONFORMANCE_PROBE=true
RUST_LOG=buzz_relay=info,buzz_db=info,buzz_auth=info,buzz_pubsub=info,tower_http=info
BUZZ_RELAY_PRIVATE_KEY=$relayPrivateKey
BUZZ_GIT_HOOK_HMAC_SECRET=$gitHookSecret
POSTGRES_DB=buzz
POSTGRES_USER=buzz
POSTGRES_PASSWORD=$postgresPassword
GCOR_DB_USER=gcor_runtime
GCOR_DB_PASSWORD=$gcorDbPassword
REDIS_PASSWORD=$redisPassword
BUZZ_S3_ACCESS_KEY=$s3AccessKey
BUZZ_S3_SECRET_KEY=$s3SecretKey
BUZZ_S3_BUCKET=buzz-media
GCOR_S3_BUCKET=buzz-gcor
GCOR_S3_ACCESS_KEY=gcor-service
GCOR_S3_SECRET_KEY=$gcorS3SecretKey
GCOR_CHANNEL_BUCKET_PREFIX=gcor-ch-
EMBEDDING_BACKEND=ollama
EMBEDDING_MODEL=nomic-embed-text
GENERATION_MODEL=qwen2.5:1.5b
PROJECTOR_EVENT_KINDS=9,40002,45001,45003
PROJECTOR_POLL_SECONDS=2
PROJECTOR_BATCH_SIZE=50
EMBEDDING_DIMS=768
OPENAI_API_KEY=
INGEST_WEBHOOK_SECRET=$stackSecret
STACK_API_SECRET=$stackSecret
ENFORCE_STACK_API_SECRET=true
GCOR_PROXY_PORT=5001
GCOR_MCP_PORT=8765
GCOR_PROXY_BIND_ADDR=127.0.0.1
GCOR_MCP_BIND_ADDR=127.0.0.1
"@

Set-Content -Path $envFile -Value $content -NoNewline -Encoding ascii
Write-Output "Created $envFile"
