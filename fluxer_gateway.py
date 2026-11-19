#!/usr/bin/env python3
"""Minimal Fluxer gateway client over a Discord-compatible WebSocket.

Fluxer's gateway speaks the same wire protocol as Discord: JSON payloads
with ``op`` codes (0 dispatch, 1 heartbeat, 2 identify, 10 hello, 11
heartbeat ACK), sequence numbers in ``s``, and dispatch event names in
``t`` (``READY``, ``MESSAGE_CREATE``, ...).  Fluxer does not implement
the intents system, so ``intents`` is sent as 0.

The client reconnects with backoff and re-identifies after a disconnect;
there is no resume support, so READY fires again on every reconnect.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import aiohttp

logger = logging.getLogger(__name__)

OP_DISPATCH = 0
OP_HEARTBEAT = 1
OP_IDENTIFY = 2
OP_RECONNECT = 7
OP_INVALID_SESSION = 9
OP_HELLO = 10
OP_HEARTBEAT_ACK = 11

MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]
ReadyHandler = Callable[[dict[str, Any]], Awaitable[None]]


class FluxerGateway:
    """Discord-compatible gateway client for Fluxer bots.

    ``on_message`` receives the raw MESSAGE_CREATE dispatch payload
    (snake_case wire fields).  ``on_ready`` receives the READY payload.
    """

    def __init__(
        self,
        gateway_url: str,
        token: str,
        *,
        on_ready: ReadyHandler,
        on_message: MessageHandler,
        session: aiohttp.ClientSession,
        reconnect_delay_seconds: float = 5.0,
        max_reconnect_delay_seconds: float = 60.0,
    ):
        self.gateway_url = gateway_url
        self.token = token
        self.on_ready = on_ready
        self.on_message = on_message
        self.session = session
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.max_reconnect_delay_seconds = max_reconnect_delay_seconds

        self._seq: int | None = None
        self._closed = False
        self._heartbeat_task: asyncio.Task | None = None

    async def connect(self) -> None:
        """Connect and serve events until :meth:`close` is called."""
        delay = self.reconnect_delay_seconds
        while not self._closed:
            try:
                await self._run_once()
                delay = self.reconnect_delay_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("gateway connection failed; retrying", exc_info=True)
            if self._closed:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.max_reconnect_delay_seconds)

    async def close(self) -> None:
        self._closed = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _run_once(self) -> None:
        async with self.session.ws_connect(self.gateway_url, heartbeat=None) as ws:
            hello = await ws.receive_json()
            if hello.get("op") != OP_HELLO:
                raise RuntimeError(f"expected HELLO, got op={hello.get('op')}")
            interval_ms = float((hello.get("d") or {}).get("heartbeat_interval", 41250))
            self._seq = None
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws, interval_ms / 1000.0))
            try:
                await self._send_identify(ws)
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    await self._handle(json.loads(msg.data), ws)
            finally:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None

    async def _handle(self, payload: dict[str, Any], ws: aiohttp.ClientWebSocketResponse) -> None:
        op = payload.get("op")
        if op == OP_DISPATCH:
            self._seq = payload.get("s", self._seq)
            event = payload.get("t")
            data = payload.get("d") or {}
            if event == "READY":
                logger.info("gateway READY as %s", (data.get("user") or {}).get("username"))
                await self.on_ready(data)
            elif event == "MESSAGE_CREATE":
                await self.on_message(data)
        elif op == OP_HEARTBEAT:
            await self._send(ws, OP_HEARTBEAT, self._seq)
        elif op == OP_RECONNECT:
            raise ConnectionError("server requested reconnect")
        elif op == OP_INVALID_SESSION:
            raise ConnectionError("invalid session")
        elif op == OP_HEARTBEAT_ACK:
            pass

    async def _heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse, interval: float) -> None:
        try:
            while True:
                await self._send(ws, OP_HEARTBEAT, self._seq)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("heartbeat loop ended", exc_info=True)

    async def _send_identify(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        await self._send(
            ws,
            OP_IDENTIFY,
            {
                "token": self.token,
                "intents": 0,  # Fluxer does not implement intents
                "properties": {
                    "os": "linux",
                    "browser": "fluxer-admin-relay",
                    "device": "fluxer-admin-relay",
                },
            },
        )

    @staticmethod
    async def _send(ws: aiohttp.ClientWebSocketResponse, op: int, data: Any) -> None:
        await ws.send_str(json.dumps({"op": op, "d": data}))
