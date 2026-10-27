"""Fail-closed, read-only service credentials for a bounded enterprise pilot.

This is a service authorization boundary, not an employee SSO implementation.
Only credential hashes are configured; caller-provided scope never grants access.
"""
import asyncio
import hashlib
import json
from contextvars import ContextVar
from dataclasses import dataclass

from starlette.responses import JSONResponse


@dataclass(frozen=True)
class Principal:
    subject: str
    channel_id: str
    access_level: str
    agent_id: str | None = None
    role: str = 'reader'
    workload: bool = False
    operations: frozenset[str] = frozenset()


WORKSPACE_PATHS = {'/api/workspace/channels', '/api/workspace/documents', '/api/workspace/detail',
                   '/api/workspace/review', '/api/workspace/feedback', '/api/workspace/jobs',
                   '/api/workspace/submit', '/api/workspace/job-action', '/api/audit-packs',
                   '/api/workspace/unanswered', '/api/workspace/unanswered/resolve'}
READ_PATHS = {'/api/ask', '/api/ask/reply', '/api/retrieve', '/api/facts/history'}
WORKLOAD_OPERATIONS = {'/api/ingest':'ingest','/api/retrieve':'retrieve','/api/ask':'retrieve','/api/facts/history':'retrieve',
                       '/api/ask/reply':'reply','/api/audit-packs':'audit.export'}


def _dependency_unavailable(error: BaseException) -> bool:
    """True for database/network failures (not for malformed or invalid proofs)."""
    try:
        import asyncpg
        db_errors = (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, asyncpg.CannotConnectNowError,
                     asyncpg.TooManyConnectionsError, asyncpg.AdminShutdownError, asyncpg.CrashShutdownError)
    except Exception:  # asyncpg is always installed in the service images; stay importable without it
        db_errors = ()
    return isinstance(error, (ConnectionError, OSError, TimeoutError) + db_errors)


current_principal: ContextVar[Principal | None] = ContextVar("gcor_principal", default=None)


def load_credentials(raw: str) -> dict[str, Principal]:
    entries = json.loads(raw)
    if not isinstance(entries, list):
        raise ValueError("GCOR_SCOPED_CREDENTIALS must be a JSON array")
    result = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {"sha256", "subject", "channel_id", "access_level", "agent_id"}:
            raise ValueError("Invalid scoped credential fields")
        digest = entry.get("sha256", "")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Credential sha256 must be a lowercase SHA-256 digest")
        if digest in result:
            raise ValueError("Duplicate credential digest")
        for key in ("subject", "channel_id", "access_level"):
            if not isinstance(entry.get(key), str) or not entry[key].strip():
                raise ValueError(f"Credential requires {key}")
        if entry.get("agent_id") is not None and (not isinstance(entry["agent_id"], str) or not entry["agent_id"].strip()):
            raise ValueError("Invalid agent_id")
        result[digest] = Principal(entry["subject"], entry["channel_id"], entry["access_level"], entry.get("agent_id"))
    return result


def load_workload_channels(raw: str) -> dict[str, frozenset[str]]:
    """Optional per-workload channel allowlist ("channels": [uuid, ...]). Absent = every channel."""
    from uuid import UUID
    result = {}
    for entry in json.loads(raw):
        if isinstance(entry, dict) and 'channels' in entry:
            channels = entry['channels']
            if not isinstance(channels, list) or not channels:
                raise ValueError('Workload channels must be a non-empty list of channel UUIDs')
            result[entry['sha256']] = frozenset(str(UUID(str(c))) for c in channels)
    return result


def load_workloads(raw: str) -> dict[str, Principal]:
    entries=json.loads(raw)
    if not isinstance(entries,list): raise ValueError('GCOR_WORKLOAD_CREDENTIALS must be a JSON array')
    result={};allowed=frozenset(WORKLOAD_OPERATIONS.values())
    for entry in entries:
        if not isinstance(entry,dict) or not {'sha256','subject','operations'}<=set(entry)<={'sha256','subject','operations','channels'}: raise ValueError('Invalid workload credential fields')
        digest,subject,operations=entry['sha256'],entry['subject'],entry['operations']
        if not isinstance(digest,str) or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest): raise ValueError('Invalid workload digest')
        if not isinstance(subject,str) or not subject.strip() or not isinstance(operations,list) or not operations: raise ValueError('Workload requires subject and operations')
        requested=frozenset(operations)
        if not requested<=allowed or any(not isinstance(item,str) for item in operations): raise ValueError('Unknown workload operation')
        if digest in result: raise ValueError('Duplicate workload digest')
        result[digest]=Principal(subject,'','',role='service',workload=True,operations=requested)
    return result


class ScopedAccess:
    def __init__(self, app, *, credentials: str = "[]", workloads: str = "[]", mode: str = "legacy", nostr=None,
                 allow_legacy_unscoped: bool = False):
        self.app = app
        if mode not in {"legacy", "scoped", "buzz"}:
            raise ValueError("GCOR_ACCESS_MODE must be legacy, scoped or buzz")
        self.credentials = load_credentials(credentials)
        self.workloads = load_workloads(workloads)
        self.workload_channels = load_workload_channels(workloads)
        self.mode = mode
        self.nostr = nostr
        self.allow_legacy_unscoped = allow_legacy_unscoped
        if mode == "buzz" and nostr is None:
            raise ValueError("Buzz mode requires Nostr configuration")
        if mode == "scoped" and not self.credentials:
            raise ValueError("Scoped mode requires configured credentials")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        path = scope.get("path", "")
        # Only health probes are public in scoped mode; docs/metrics are denied.
        if path in {"/health", "/health/live", "/health/ready", "/workspace", "/workspace.js", "/buzz-knowledge.mjs"}:
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers", []))
        authorization = headers.get(b"authorization")
        principal = None
        workload_authorization=headers.get(b'x-gcor-workload-authorization')
        if workload_authorization is not None:
            scheme,_,token=workload_authorization.partition(b' ');operation=WORKLOAD_OPERATIONS.get(path)
            digest=hashlib.sha256(token).hexdigest() if scheme.lower()==b'bearer' and token else ''
            template=self.workloads.get(digest) if digest else None
            channel=headers.get(b'x-gcor-channel-id',b'').decode('ascii',errors='ignore')
            access_level=headers.get(b'x-gcor-access-level',b'').decode('ascii',errors='ignore')
            try:
                from uuid import UUID
                channel=str(UUID(channel))
            except ValueError: channel=''
            bound=self.workload_channels.get(digest)
            if template is None or not channel or access_level not in {'public','private'} or operation not in template.operations or scope['method']!='POST' or (bound is not None and channel not in bound):
                return await JSONResponse({'detail':'Invalid workload identity, scope or operation'},status_code=403)(scope,receive,send)
            principal=Principal(template.subject,channel,access_level,role='service',workload=True,operations=template.operations)
        if authorization is not None and principal is None:
            scheme, _, token = authorization.partition(b" ")
            if self.mode != "buzz" and scheme.lower() == b"bearer" and token and len(token) <= 16384:
                principal = self.credentials.get(hashlib.sha256(token).hexdigest())
            elif scheme.lower() == b"nostr" and token and len(token) <= 16384 and self.nostr is not None:
                if scope["method"] != "POST" or path not in READ_PATHS | WORKSPACE_PATHS:
                    return await JSONResponse({"detail": "Read-only identity endpoint"}, status_code=403)(scope, receive, send)
                try:
                    body = bytearray()
                    async with asyncio.timeout(15):
                        while True:
                            message = await receive()
                            if message["type"] == "http.disconnect":
                                return
                            body.extend(message.get("body", b""))
                            if len(body) > 1024 * 1024:
                                return await JSONResponse({"detail": "Identity request too large"}, status_code=413)(scope, receive, send)
                            if not message.get("more_body", False):
                                break
                    raw_body = bytes(body)
                    principal = await self.nostr.authenticate(token, scope, raw_body)
                    original_receive = receive
                    replayed = False
                    async def replay():
                        nonlocal replayed
                        if replayed:
                            return await original_receive()
                        replayed = True
                        return {"type": "http.request", "body": raw_body, "more_body": False}
                    receive = replay
                except PermissionError:
                    return await JSONResponse({"detail": "Authorization proof rejected or membership inactive"}, status_code=403)(scope, receive, send)
                except Exception as error:
                    if _dependency_unavailable(error):
                        # Membership could not be checked because the database is unreachable. Say so
                        # (503, retryable) instead of reporting the caller's valid signature as invalid.
                        return await JSONResponse({"detail": "Identity check temporarily unavailable; retry"}, status_code=503,
                                                  headers={"Retry-After": "2"})(scope, receive, send)
                    principal = None
            if principal is None:
                return await JSONResponse({"detail": "Invalid scoped credential"}, status_code=401)(scope, receive, send)
        elif principal is None and self.mode in {"scoped", "buzz"}:
            return await JSONResponse({"detail": "Scoped credential required"}, status_code=401)(scope, receive, send)
        elif principal is None and self.mode == 'legacy' and self.allow_legacy_unscoped:
            principal=Principal('ingestion-worker','','',role='service',workload=True)
        permitted = READ_PATHS | WORKSPACE_PATHS if self.mode == 'buzz' else READ_PATHS
        if principal is not None and not principal.workload and (scope["method"] != "POST" or path not in permitted):
            return await JSONResponse({"detail": "Read-only credential cannot access this endpoint"}, status_code=403)(scope, receive, send)
        reset = current_principal.set(principal)
        try:
            if principal is not None and self.mode == 'buzz':
                released = blocked = False
                async def release_checked(message):
                    nonlocal released, blocked
                    if blocked:
                        return
                    if message['type'] == 'http.response.start' and not released:
                        released = True
                        if not await self.nostr.still_authorized(principal):
                            blocked = True
                            return await JSONResponse({"detail": "Membership revoked before response release"}, status_code=403)(scope, receive, send)
                    await send(message)
                await self.app(scope, receive, release_checked)
            else:
                await self.app(scope, receive, send)
        finally:
            current_principal.reset(reset)
