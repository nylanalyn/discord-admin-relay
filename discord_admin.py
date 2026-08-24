#!/usr/bin/env python3
"""Discord admin router for HTTP bot APIs and Eggdrop partyline commands."""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any

from eggdrop_telnet import EggdropTelnetClient

import aiohttp
import yaml
from dotenv import load_dotenv

try:
    import discord
    from discord import app_commands
except ImportError:  # pragma: no cover - exercised by runtime environment
    discord = None
    app_commands = None

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict[str, Any]:
    config_file = Path(path).resolve()
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    load_dotenv(config_file.parent / ".env")
    load_dotenv()
    with open(config_file, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    config = interpolate_env(raw)
    config["_config_dir"] = str(config_file.parent)
    return config


def interpolate_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_env(v) for v in value]
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        inner = value[2:-1]
        if ":-" in inner:
            name, default = inner.split(":-", 1)
        else:
            name, default = inner, None
        resolved = os.getenv(name, default)
        if resolved is None:
            raise ValueError(f"Environment variable {name} not set")
        return resolved
    return value


def setup_logging(config: dict[str, Any]) -> None:
    log_config = config.get("logging") or {}
    level = getattr(logging, str(log_config.get("level", "INFO")).upper(), logging.INFO)
    fmt = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_config.get("file"):
        handlers.insert(0, logging.FileHandler(log_config["file"]))
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


class DiscordAdminRouter:
    def __init__(self, config: dict[str, Any]):
        if discord is None:
            raise RuntimeError("discord.py is not installed. Install the project dependencies first.")

        discord_config = config.get("discord") or {}
        self.token = discord_config.get("token", "")
        if not self.token:
            raise ValueError("discord.token is required")

        self.guild_id = self._optional_int(discord_config.get("guild_id"))
        self.admin_channel_id = int(discord_config["admin_channel_id"])
        self.allowed_user_ids = {
            int(v) for v in discord_config.get("allowed_user_ids", [])
            if v not in (None, "")
        }
        self.poll_events_seconds = float(discord_config.get("poll_events_seconds", 5.0))
        self.command_name = str(discord_config.get("slash_command", "bot"))
        self.message_commands_enabled = self._as_bool(discord_config.get("message_commands_enabled", True))

        intents = discord.Intents.default()
        intents.message_content = self.message_commands_enabled
        self.client = discord.Client(intents=intents)
        self.tree = app_commands.CommandTree(self.client)
        self.session: aiohttp.ClientSession | None = None
        self.bots = self._load_bots(config.get("bots") or {}, Path(config.get("_config_dir") or "."))
        self._event_since = {name: 0 for name in self.bots}
        self._poll_task: asyncio.Task | None = None

        self.client.event(self.on_ready)
        self.client.event(self.on_message)
        self._register_slash_command()

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=120)
        self.session = aiohttp.ClientSession(timeout=timeout)
        await self.client.start(self.token)

    async def close(self) -> None:
        if not self.client.is_closed():
            await self.client.close()
        if self.session:
            await self.session.close()

    async def on_ready(self):
        logger.info(f"Discord admin router connected as {self.client.user}")
        guild = discord.Object(id=self.guild_id) if self.guild_id else None
        if guild:
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Synced slash commands to guild {self.guild_id}")
        else:
            await self.tree.sync()
            logger.info("Synced global slash commands")
        if not self._poll_task or self._poll_task.done():
            self._poll_task = self.client.loop.create_task(self._poll_events_loop())

    async def on_message(self, message):
        if message.author.bot:
            return
        if message.channel.id != self.admin_channel_id:
            return
        if self.allowed_user_ids and message.author.id not in self.allowed_user_ids:
            return

        text = message.content.strip()
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
        await self._reply_to_channel(message.channel, trigger, command, args)

    def _register_slash_command(self) -> None:
        @self.tree.command(name=self.command_name, description="Run a bot admin command")
        @app_commands.describe(target="Bot trigger name", command="Command name", args="Command arguments")
        async def bot_command(interaction, target: str, command: str, args: str = ""):
            if interaction.channel_id != self.admin_channel_id:
                await interaction.response.send_message("wrong channel", ephemeral=True)
                return
            if self.allowed_user_ids and interaction.user.id not in self.allowed_user_ids:
                await interaction.response.send_message("not allowed", ephemeral=True)
                return
            target = target.lower()
            if target not in self.bots:
                await interaction.response.send_message(f"unknown bot trigger: {target}", ephemeral=True)
                return
            await interaction.response.defer()
            messages = await self._run_command(target, command, args)
            await self._send_chunks(interaction.followup.send, self._format_reply(target, messages))

    async def _reply_to_channel(self, channel, target: str, command: str, args: str) -> None:
        messages = await self._run_command(target, command, args)
        for chunk in self._chunks(self._format_reply(target, messages)):
            await channel.send(chunk)

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
        await self.client.wait_until_ready()
        channel = None
        for _ in range(10):
            channel = self.client.get_channel(self.admin_channel_id)
            if channel is not None:
                break
            await asyncio.sleep(2)
        if channel is None:
            logger.warning(f"admin channel {self.admin_channel_id} not found after retries; event polling disabled")
            return
        while not self.client.is_closed():
            await self._poll_events(channel)
            await asyncio.sleep(self.poll_events_seconds)

    async def _poll_events(self, channel) -> None:
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
                for chunk in self._chunks(f"**{target}**\n{event.get('message', '')}"):
                    await channel.send(chunk)

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

    @staticmethod
    async def _send_chunks(sender, text: str) -> None:
        for chunk in DiscordAdminRouter._chunks(text):
            await sender(chunk)

    @staticmethod
    def _load_bots(raw_bots: dict[str, Any], config_dir: Path) -> dict[str, dict[str, Any]]:
        bots: dict[str, dict[str, Any]] = {}
        for trigger, bot in raw_bots.items():
            normalized = str(trigger).lower()
            target_type = str(bot.get("type", "admin_api")).strip().lower()

            env_file = str(bot.get("env_file", "")).strip()
            if env_file:
                env_path = Path(env_file).expanduser()
                if not env_path.is_absolute():
                    env_path = config_dir / env_path
                load_dotenv(env_path)

            if target_type == "admin_api":
                url = str(bot.get("url", "")).strip()
                token = str(bot.get("token", "")).strip()
                token_env = str(bot.get("token_env", "")).strip()
                if not token and token_env:
                    token = str(os.getenv(token_env, "")).strip()
                if not url or not token:
                    raise ValueError(f"bot {trigger} requires url and token")
                bots[normalized] = {"type": "admin_api", "url": url, "token": token}
            elif target_type == "eggdrop":
                username = str(bot.get("username", "")).strip()
                password = str(bot.get("password", "")).strip()
                password_env = str(bot.get("password_env", "")).strip()
                if not password and password_env:
                    password = str(os.getenv(password_env, "")).strip()
                if not username or not password:
                    raise ValueError(
                        f"eggdrop bot {trigger} requires username and password"
                    )
                client = EggdropTelnetClient(
                    username=username,
                    password=password,
                    host=str(bot.get("host", "127.0.0.1")),
                    port=int(bot.get("port", 6465)),
                    encoding=str(bot.get("encoding", "utf-8")),
                    login_timeout_seconds=float(
                        bot.get("login_timeout_seconds", 10.0)
                    ),
                    command_timeout_seconds=float(
                        bot.get("command_timeout_seconds", 8.0)
                    ),
                    response_idle_seconds=float(
                        bot.get("response_idle_seconds", 1.0)
                    ),
                    max_bytes=int(bot.get("max_bytes", 65536)),
                )
                bots[normalized] = {"type": "eggdrop", "client": client}
            else:
                raise ValueError(
                    f"bot {trigger} has unsupported type {target_type!r}"
                )
        if not bots:
            raise ValueError("at least one bot must be configured")
        return bots

    @staticmethod
    def _optional_int(value) -> int | None:
        if value in (None, ""):
            return None
        return int(value)

    @staticmethod
    def _as_bool(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off", ""}
        return bool(value)


async def async_main(config_path: str) -> None:
    config = load_config(config_path)
    setup_logging(config)
    router = DiscordAdminRouter(config)
    try:
        await router.start()
    finally:
        await router.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared Discord admin router")
    parser.add_argument("--config", default="discord_admin.yaml", help="Path to discord_admin.yaml")
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
