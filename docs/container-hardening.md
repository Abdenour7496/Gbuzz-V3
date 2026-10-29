# Container hardening

The base `docker-compose.yml` now carries the runtime hardening that previously lived only in the
knowledge-trust overlay, so every installation gets it, including a fresh cloud install.

| Service | User | Root filesystem | Capabilities | `no-new-privileges` | Memory ceiling | Processes |
| --- | --- | --- | --- | --- | --- | --- |
| gcor-proxy (and gcor-enterprise, buzz-knowledge, which extend it) | 10001 | read-only, `/tmp` tmpfs | none | yes | `GCOR_PROXY_MEM_LIMIT` 2g | `GCOR_PIDS_LIMIT` 256 |
| gcor-event-projector | 10001 | read-only | none | yes | `GCOR_PROJECTOR_MEM_LIMIT` 512m | 256 |
| mcp-postgres-gcor | 10001 | read-only | none | yes | `GCOR_MCP_MEM_LIMIT` 512m | 256 |
| docker-socket-proxy (built from this repo) | root, socket access only | read-only | none | yes | 128m | 64 |
| recovery-controller | root, writes its audit volume | read-only | none | yes | 256m | 64 |
| relay, postgres, redis, minio, ollama | image default | image default | image default | yes | `RELAY_MEM_LIMIT` 1g, `POSTGRES_MEM_LIMIT` 4g, `REDIS_MEM_LIMIT` 512m, `MINIO_MEM_LIMIT` 1g, `OLLAMA_MEM_LIMIT` 12g | 512, 1024, 256, 512, 512 |

Memory limits are ceilings, not reservations; raise them in `.env` if a host runs bigger models
or larger databases. The proxy, projector and MCP images also set `USER 10001:10001`, so they run
unprivileged even when started outside Compose.

The third-party `tecnativa/docker-socket-proxy:latest` image is replaced by the repository's own
socket boundary (`recovery-controller/socket_proxy.py`), already used on the lab through the
knowledge-trust overlay. It lists and restarts only containers labelled for this stack's recovery.
The `minio-skip` overlay uses the pinned `alpine:3.22.1` image instead of `curlimages/curl:latest`.

## Check after deploying

- `docker compose ps` shows every service healthy.
- `docker inspect --format '{{.Config.User}} {{.HostConfig.ReadonlyRootfs}} {{.HostConfig.Memory}}' <container>`
  matches the table.
- Recovery still works: `/api/status` on the recovery controller lists the monitored services.

On Linux hosts, scripts that bind-mount a host directory into a GCOR container (backup and
restore-drill helpers) need that directory writable by UID 10001. Docker Desktop on Windows maps
permissions permissively, so the lab is unaffected.
