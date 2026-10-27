import unittest
import os
from unittest.mock import AsyncMock, patch
import httpx
import server


class McpGovernanceTest(unittest.IsolatedAsyncioTestCase):
    def test_listener_matches_container_configuration(self):
        self.assertEqual(os.getenv("FASTMCP_HOST", "0.0.0.0"), server.mcp.settings.host)
        self.assertEqual(int(os.getenv("FASTMCP_PORT", "8765")), server.mcp.settings.port)

    async def test_governance_tools_forward_optional_idempotency_key(self):
        with patch.object(server, "proxy_post", AsyncMock(return_value={})) as post:
            await server.approve_knowledge(target_document_id="document", idempotency_key="approve-1")
            self.assertEqual("approve-1", post.call_args.kwargs["idempotency_key"])
            await server.transition_knowledge("archived", target_document_id="document", idempotency_key="archive-1")
            self.assertEqual("archive-1", post.call_args.kwargs["idempotency_key"])

    async def test_fact_history_tool_posts_to_the_scoped_history_endpoint(self):
        with patch.object(server, "proxy_post", AsyncMock(return_value={"facts": []})) as post:
            self.assertEqual({"facts": []}, await server.fact_history("Release process", channel_id="c", view="true_at", as_of="2026-05-01"))
        path, payload = post.call_args.args
        self.assertEqual("/api/facts/history", path)
        self.assertEqual(("Release process", "c", "true_at", "2026-05-01"), (payload["subject"], payload["channel_id"], payload["view"], payload["as_of"]))

    async def test_outbox_tool_reads_authenticated_proxy_helper(self):
        with patch.object(server, "proxy_get", AsyncMock(return_value={"pending": 1})) as get:
            self.assertEqual({"pending": 1}, await server.get_governance_outbox())
            get.assert_awaited_once_with("/api/governance/outbox")

    async def test_proxy_helper_sends_headers_without_changing_payload(self):
        captured = []
        client_class = httpx.AsyncClient
        def handler(request):
            captured.append(request)
            return httpx.Response(200, json={"ok": True})
        with patch.object(server.httpx, "AsyncClient", side_effect=lambda **kwargs: client_class(transport=httpx.MockTransport(handler), **kwargs)), \
                patch.object(server, "STACK_API_SECRET", "test-secret"):
            await server.proxy_post("/api/knowledge/approve", {"note": "ok"}, "key-1")
        self.assertEqual("key-1", captured[0].headers["idempotency-key"])
        self.assertEqual("test-secret", captured[0].headers["x-gcor-webhook-secret"])
        self.assertEqual(b'{"note":"ok"}', captured[0].content)
