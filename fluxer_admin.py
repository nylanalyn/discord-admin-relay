#!/usr/bin/env python3
"""Fluxer admin router for HTTP bot APIs and Eggdrop partyline commands.

Drop-in replacement for ``discord_admin.py`` on a Fluxer instance.  Fluxer
is wire-compatible with Discord's core HTTP API and gateway, but does not
implement bot slash commands yet, so only message commands are served.
"""

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

import aiohttp

from discord_admin import DiscordAdminRouter, load_config, setup_logging
from fluxer_gateway import FluxerGateway

logger = logging.getLogger(__name__)


class FluxerAdminRouter:
    def __init__(self, config: dict[str, Any]):
        fluxer_config = config.get("fluxer") or {}
        self.instance_url = str(fluxer_config.get("instance_url", "")).rstrip("/")
        if not self.instance_url:
            raise ValueError("fluxer.instance_url is required")
        self.token = fluxer_config.get("token", "")
        if not self.token:
            raise ValueError("fluxer.token is required")

        # IDs are snowflakes; keep them as strings for comparison.
        self.admin_channel_id = str(fluxer_config["admin_channel_id"])
        self.allowed_user_ids = {
            str(v) for v in fluxer_config.get("allowed_user_ids", []) if v not in (None, "")
        }
        self.poll_events_seconds = float(fluxer_config.get("poll_events_seconds", 5.0))
        self.api_url = str(fluxer_config.get("api_url", "")).rstrip("/")
        self.gateway_url = str(fluxer_config.get("gateway_url", ""))

        self.session: aiohttp.ClientSession | None = None
        self.bots = DiscordAdminRouter._load_bots(
            config.get("bots") or {}, Path(config.get("_config_dir") or ".")
        )
        self._event_since = {name: 0 for name in self.bots}
        self._gateway: FluxerGateway | None = None
        self._poll_task: asyncio.Task | None = None
        self._ready = asyncio.Event()

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=120)
        self.session = aiohttp.ClientSession(timeout=timeout)
        if not self.api_url or not self.gateway_url:
            await self._discover_endpoints()
        assert self.api_url and self.gateway_url

        user = await self._get_me()
        logger.info(f"Fluxer admin router connected as {user.get('username')} ({user.get('id')})")

        self._gateway = FluxerGateway(
            self.gateway_url,
            self.token,
            on_ready=self._on_ready,
            on_message=self._on_message,
            session=self.session,
        )
        gateway_task = asyncio.create_task(self._gateway.connect())
        try:
            await self._ready.wait()
            await gateway_task
        finally:
            await self.close()

    async def close(self) -> None:
        if self._gateway:
            await self._gateway.close()
        if self._poll_task:
            self._poll_task.cancel()
        if self.session:
            await self.session.close()

    async def _discover_endpoints(self) -> None:
        """Resolve API and gateway URLs from the instance's well-known document."""
        assert self.session is not None
        url = f"{self.instance_url}/.well-known/fluxer"
        async with self.session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()
        endpoints = data.get("endpoints") or {}
        if not self.api_url:
            api_public = str(endpoints.get("api_public") or "").rstrip("/")
            if not api_public:
                raise ValueError("instance did not advertise endpoints.api_public; set fluxer.api_url")
            self.api_url = f"{api_public}/v1"
        if not self.gateway_url:
            gateway = str(endpoints.get("gateway") or "")
            if not gateway:
                raise ValueError("instance did not advertise endpoints.gateway; set fluxer.gateway_url")
            sep = "&" if "?" in gateway else "?"
            self.gateway_url = f"{gateway}{sep}v=1&encoding=json"

    async def _get_me(self) -> dict[str, Any]:
        assert self.session is not None
        async with self.session.get(
            f"{self.api_url}/users/@me", headers=self._headers()
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _headers(self) -> dict[str, str]:
        # Fluxer requires the "Bot " scheme prefix for bot tokens;
        # "Bearer" is reserved for OAuth2 access tokens and gets 401.
        return {"Authorization": f"Bot {self.token}"}

    async def _on_ready(self, data: dict[str, Any]) -> None:
        self._ready.set()
        if not self._poll_task or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_events_loop())

    async def _on_message(self, data: dict[str, Any]) -> None:
        author = data.get("author") or {}
        if author.get("bot"):
            return
        if str(data.get("channel_id", "")) != self.admin_channel_id:
            return
        if self.allowed_user_ids and str(author.get("id", "")) not in self.allowed_user_ids:
            return

        text = str(data.get("content") or "").strip()
        if not text:
            return
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            return

        trigger = parts[0].lower()
        bot = self.bots.get(trigger)
        if not bot:
            return
        command = parts[1]
        args = parts[2] if len(parts) > 2 else ""
        messages = await self._run_command(trigger, command, args)
        await self._send_chunks(self.admin_channel_id, self._format_reply(trigger, messages))

    async def _send_chunks(self, channel_id: str, text: str) -> None:
        assert self.session is not None
        url = f"{self.api_url}/channels/{channel_id}/messages"
        for chunk in self._chunks(text):
            async with self.session.post(
                url, json={"content": chunk}, headers=self._headers()
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error(f"failed to send message to {channel_id}: {resp.status} {body}")

    async def _run_command(self, target: str, command: str, args: str) -> list[str]:
        bot = self.bots[target]
        bot_type = bot.get("type", "admin_api")
        try:
            if bot_type == "admin_api":
                return await self._run_admin_api_command(bot, command, args)
            if bot_type == "eggdrop":
                return await bot["client"].run_command(command, args)
            return [f"unknown bot type {bot_type!r}"]
        except Exception as e:
            logger.error(f"failed to route command to {target}", exc_info=True)
            return [f"failed to reach {target}: {e}"]

    async def _run_admin_api_command(
        self, bot: dict[str, Any], command: str, args: str
    ) -> list[str]:
        url = f"{bot['url'].rstrip('/')}/v1/command"
        headers = {"Authorization": f"Bearer {bot['token']}"}
        payload = {"command": command, "args": args}
        assert self.session is not None
        async with self.session.post(url, json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status >= 400:
                return [f"admin API error {resp.status}: {data.get('error', data)}"]
            messages = data.get("messages") or []
            return [str(item) for item in messages] or ["ok."]

    async def _poll_events_loop(self) -> None:
        await self._ready.wait()
        while True:
            await self._poll_events()
            await asyncio.sleep(self.poll_events_seconds)

    async def _poll_events(self) -> None:
        assert self.session is not None
        for target, bot in self.bots.items():
            if bot.get("type", "admin_api") != "admin_api":
                continue
            url = f"{bot['url'].rstrip('/')}/v1/events"
            headers = {"Authorization": f"Bearer {bot['token']}"}
            params = {"since": str(self._event_since.get(target, 0))}
            try:
                async with self.session.get(url, headers=headers, params=params) as resp:
                    if resp.status >= 400:
                        continue
                    data = await resp.json()
            except Exception:
                logger.debug(f"event poll failed for {target}", exc_info=True)
                continue

            for event in data.get("events") or []:
                self._event_since[target] = max(self._event_since.get(target, 0), int(event["id"]))
                await self._send_chunks(
                    self.admin_channel_id, f"**{target}**\n{event.get('message', '')}"
                )

    @staticmethod
    def _format_reply(target: str, messages: list[str]) -> str:
        return f"**{target}**\n" + "\n".join(messages)

    @staticmethod
    def _chunks(text: str, limit: int = 1900) -> list[str]:
        chunks = []
        current = ""
        for line in text.splitlines() or [""]:
            if len(current) + len(line) + 1 > limit:
                chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)
        return chunks


async def async_main(config_path: str) -> None:
    config = load_config(config_path)
    setup_logging(config)
    router = FluxerAdminRouter(config)
    try:
        await router.start()
    finally:
        await router.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared Fluxer admin router")
    parser.add_argument("--config", default="fluxer_admin.yaml", help="Path to fluxer_admin.yaml")
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
