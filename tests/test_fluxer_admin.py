"""Tests for the Fluxer admin router and gateway client.

Covers config handling, message-command parsing, reply chunking, and a
full gateway round-trip against a fake WebSocket gateway server.
"""

import asyncio
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiohttp
from aiohttp import web

import fluxer_admin
from fluxer_admin import FluxerAdminRouter
from fluxer_gateway import FluxerGateway


def make_config(**overrides):
    fluxer = {
        "instance_url": "http://127.0.0.1:1",
        "token": "tok",
        "admin_channel_id": "111",
        "allowed_user_ids": ["42"],
        "poll_events_seconds": 5,
    }
    fluxer.update(overrides)
    return {
        "fluxer": fluxer,
        "bots": {"aria": {"url": "http://127.0.0.1:9101", "token": "t"}},
        "_config_dir": ".",
    }


class ConfigTests(unittest.TestCase):
    def test_requires_instance_url(self):
        with self.assertRaises(ValueError):
            FluxerAdminRouter({"fluxer": {"token": "t", "admin_channel_id": "1"}, "bots": {}})

    def test_requires_token(self):
        with self.assertRaises(ValueError):
            FluxerAdminRouter(make_config(instance_url="", token=""))

    def test_ids_kept_as_strings(self):
        router = FluxerAdminRouter(make_config(admin_channel_id=111, allowed_user_ids=[42, "43"]))
        self.assertEqual(router.admin_channel_id, "111")
        self.assertEqual(router.allowed_user_ids, {"42", "43"})
        self.assertIn("aria", router.bots)


class CommandParsingTests(unittest.IsolatedAsyncioTestCase):
    async def test_message_routes_to_bot(self):
        router = FluxerAdminRouter(make_config())
        router.session = aiohttp.ClientSession()
        sent = []

        async def fake_run(target, command, args):
            return ["pong"]

        async def fake_send(channel_id, text):
            sent.append((channel_id, text))

        router._run_command = fake_run
        router._send_chunks = fake_send
        try:
            await router._on_message(
                {
                    "channel_id": "111",
                    "content": "ARIA status now",
                    "author": {"id": "42", "bot": False},
                }
            )
        finally:
            await router.session.close()
        self.assertEqual(sent, [("111", "**aria**\npong")])

    async def test_ignores_wrong_channel_and_users(self):
        router = FluxerAdminRouter(make_config())
        called = []

        async def fake_run(target, command, args):
            called.append(target)
            return []

        router._run_command = fake_run
        router._send_chunks = lambda *a: asyncio.sleep(0)
        await router._on_message({"channel_id": "999", "content": "aria status", "author": {"id": "42"}})
        await router._on_message({"channel_id": "111", "content": "aria status", "author": {"id": "7"}})
        await router._on_message({"channel_id": "111", "content": "aria status", "author": {"id": "42", "bot": True}})
        await router._on_message({"channel_id": "111", "content": "unknownbot status", "author": {"id": "42"}})
        await router._on_message({"channel_id": "111", "content": "solo", "author": {"id": "42"}})
        self.assertEqual(called, [])

    def test_chunks_respects_limit(self):
        text = "\n".join(f"line-{i}-{'x' * 100}" for i in range(50))
        chunks = FluxerAdminRouter._chunks(text, limit=1900)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 1900)
        self.assertEqual("\n".join(chunks), text)


class FakeGateway:
    """Minimal fake Fluxer gateway + REST server for round-trip tests."""

    def __init__(self):
        self.received_messages = []
        self.identify_payload = None
        self.heartbeats = 0
        self.runner = None
        self.port = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/gateway", self._ws_handler)
        app.router.add_get("/api/v1/users/@me", self._me)
        app.router.add_post("/api/v1/channels/{cid}/messages", self._create_message)
        app.router.add_get("/.well-known/fluxer", self._well_known)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]

    async def stop(self):
        await self.runner.cleanup()

    async def _well_known(self, request):
        return web.json_response(
            {
                "endpoints": {
                    "api_public": f"http://127.0.0.1:{self.port}/api",
                    "gateway": f"ws://127.0.0.1:{self.port}/gateway",
                }
            }
        )

    async def _me(self, request):
        return web.json_response({"id": "1", "username": "relay-bot", "bot": True})

    async def _create_message(self, request):
        cid = request.match_info["cid"]
        body = await request.json()
        self.received_messages.append((cid, body.get("content", "")))
        return web.json_response({"id": "9", "channel_id": cid, "content": body.get("content", "")})

    async def _ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_str(json.dumps({"op": 10, "d": {"heartbeat_interval": 60000}}))
        ready_sent = False
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            payload = json.loads(msg.data)
            op = payload.get("op")
            if op == 2:
                self.identify_payload = payload.get("d")
                await ws.send_str(
                    json.dumps(
                        {
                            "op": 0,
                            "t": "READY",
                            "s": 1,
                            "d": {"user": {"id": "1", "username": "relay-bot"}},
                        }
                    )
                )
                ready_sent = True
            elif op == 1:
                self.heartbeats += 1
                await ws.send_str(json.dumps({"op": 11, "d": None}))
            if ready_sent and not getattr(self, "_message_sent", False):
                self._message_sent = True
                await ws.send_str(
                    json.dumps(
                        {
                            "op": 0,
                            "t": "MESSAGE_CREATE",
                            "s": 2,
                            "d": {
                                "id": "5",
                                "channel_id": "111",
                                "content": "aria ping",
                                "author": {"id": "42", "bot": False},
                            },
                        }
                    )
                )
        return ws


class GatewayRoundTripTests(unittest.IsolatedAsyncioTestCase):
    async def test_gateway_round_trip(self):
        server = FakeGateway()
        await server.start()
        try:
            config = make_config(
                instance_url=f"http://127.0.0.1:{server.port}",
                api_url="",  # force discovery via /.well-known/fluxer
                gateway_url="",
            )
            router = FluxerAdminRouter(config)

            async def fake_admin_api(bot, command, args):
                return [f"echo:{command}:{args}"]

            router._run_admin_api_command = fake_admin_api
            router.poll_events_seconds = 3600  # don't poll during test

            async def run_router():
                timeout = aiohttp.ClientTimeout(total=30)
                router.session = aiohttp.ClientSession(timeout=timeout)
                await router._discover_endpoints()
                router._gateway = FluxerGateway(
                    router.gateway_url,
                    router.token,
                    on_ready=router._on_ready,
                    on_message=router._on_message,
                    session=router.session,
                )
                task = asyncio.create_task(router._gateway.connect())
                await router._ready.wait()
                # give MESSAGE_CREATE a moment to be handled
                for _ in range(50):
                    if server.received_messages:
                        break
                    await asyncio.sleep(0.1)
                await router._gateway.close()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                await router.session.close()

            await asyncio.wait_for(run_router(), timeout=15)

            self.assertIsNotNone(server.identify_payload)
            self.assertEqual(server.identify_payload["token"], "tok")
            self.assertEqual(server.identify_payload["intents"], 0)
            self.assertGreaterEqual(server.heartbeats, 1)
            self.assertEqual(server.received_messages, [("111", "**aria**\necho:ping:")])
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
