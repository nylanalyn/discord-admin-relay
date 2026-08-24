"""Minimal async telnet client for Eggdrop partyline commands.

Each ``run_command`` call opens a fresh TCP connection, performs the
partyline username/password login, sends one dot-command, collects the
output, and closes the connection.  This avoids stale sessions and
unsolicited partyline chatter being mistaken for command output.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

# Telnet protocol constants (RFC 854)
IAC = 0xFF  # interpret as command
DONT = 0xFE
DO = 0xFD
WONT = 0xFC
WILL = 0xFB
SB = 0xFA  # subnegotiation begin
SE = 0xF0  # subnegotiation end

_BAD_PASSWORD_MARKERS = ("bad password", "invalid password", "wrong password")


class EggdropTelnetError(RuntimeError):
    """Raised when the Eggdrop partyline login or command fails."""


class EggdropTelnetClient:
    """Async client that runs one dot-command per TCP session.

    ``run_command`` is serialized per-instance via an :class:`asyncio.Lock`
    so rapid Discord commands do not trigger simultaneous partyline logins.
    """

    def __init__(
        self,
        username: str,
        password: str,
        host: str = "127.0.0.1",
        port: int = 6465,
        encoding: str = "utf-8",
        login_timeout_seconds: float = 10.0,
        command_timeout_seconds: float = 8.0,
        response_idle_seconds: float = 1.0,
        max_bytes: int = 65536,
    ) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.encoding = encoding
        self.login_timeout_seconds = float(login_timeout_seconds)
        self.command_timeout_seconds = float(command_timeout_seconds)
        self.response_idle_seconds = float(response_idle_seconds)
        self.max_bytes = int(max_bytes)
        self._lock = asyncio.Lock()

    async def run_command(self, command: str, args: str = "") -> list[str]:
        async with self._lock:
            return await self._run_command_locked(command, args)

    async def _run_command_locked(self, command: str, args: str = "") -> list[str]:
        dot_command = self._format_command(command, args)
        reader: asyncio.StreamReader
        writer: asyncio.StreamWriter
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
        except OSError as exc:
            raise EggdropTelnetError(f"eggdrop connection failed: {exc}") from exc

        try:
            await self._login(reader, writer)
            writer.write((dot_command + "\r\n").encode(self.encoding))
            await writer.drain()
            output = await self._collect_output(reader, writer)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # pragma: no cover - best-effort cleanup
                pass
        return output

    # ------------------------------------------------------------------ login

    async def _login(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        banner = await self._read_until_idle(
            reader, writer, timeout=self.login_timeout_seconds
        )
        if banner is None:
            raise EggdropTelnetError("eggdrop login failed or connection closed")
        writer.write((self.username + "\r\n").encode(self.encoding))
        await writer.drain()

        password_prompt = await self._read_until_idle(
            reader, writer, timeout=self.login_timeout_seconds
        )
        if password_prompt is None:
            raise EggdropTelnetError("eggdrop login failed or connection closed")
        writer.write((self.password + "\r\n").encode(self.encoding))
        await writer.drain()

        login_text = await self._read_until_idle(
            reader, writer, timeout=self.login_timeout_seconds
        )
        if login_text is None:
            raise EggdropTelnetError("eggdrop login failed or connection closed")
        lowered = login_text.decode(self.encoding, errors="replace").lower()
        for marker in _BAD_PASSWORD_MARKERS:
            if marker in lowered:
                raise EggdropTelnetError("eggdrop login failed")

    # ---------------------------------------------------------------- output

    async def _collect_output(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> list[str]:
        raw = await self._read_until_idle(
            reader,
            writer,
            timeout=self.command_timeout_seconds,
            idle=self.response_idle_seconds,
        )
        if raw is None:
            return ["ok."]
        if len(raw) > self.max_bytes:
            raise EggdropTelnetError(
                f"eggdrop response exceeded {self.max_bytes} bytes"
            )
        text = raw.decode(self.encoding, errors="replace")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.strip() for line in text.split("\n")]
        lines = [line for line in lines if line]
        return lines or ["ok."]

    # ------------------------------------------------------------- read helper

    async def _read_until_idle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        timeout: float,
        idle: float | None = None,
    ) -> bytes | None:
        """Read from *reader*, handling telnet negotiation, until no bytes
        arrive for *idle* seconds or *timeout* elapses.

        Returns the accumulated payload bytes (telnet IAC sequences stripped),
        or ``None`` if the stream closed before any data arrived.
        """
        if idle is None:
            idle = self.response_idle_seconds
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        buf = bytearray()
        saw_any_data = False
        closed = False

        while True:
            now = loop.time()
            remaining = deadline - now
            if remaining <= 0:
                break
            wait = min(idle, remaining)
            try:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=wait)
            except asyncio.TimeoutError:
                # Idle window elapsed with no new data.
                break
            except Exception:
                closed = True
                break
            if not chunk:
                closed = True
                break

            payload, responses = self._strip_telnet(chunk)
            if responses:
                writer.write(responses)
                try:
                    await writer.drain()
                except Exception:
                    closed = True
                    break
            if payload:
                buf.extend(payload)
                saw_any_data = True
            # Loop again to keep reading until idle or timeout.

        if not saw_any_data and closed:
            return None
        return bytes(buf)

    # --------------------------------------------------------- telnet stripping

    @staticmethod
    def _strip_telnet(data: bytes) -> tuple[bytes, bytes]:
        """Remove telnet IAC sequences from *data*.

        Returns ``(payload, responses)`` where *responses* is the byte string
        the caller should write back (WONT/DONT replies for DO/WILL).
        """
        payload = bytearray()
        responses = bytearray()
        i = 0
        n = len(data)
        while i < n:
            b = data[i]
            if b != IAC:
                payload.append(b)
                i += 1
                continue
            if i + 1 >= n:
                # Incomplete IAC at end; drop it.
                break
            cmd = data[i + 1]
            if cmd == IAC:
                # Literal 0xFF escaped as IAC IAC.
                payload.append(0xFF)
                i += 2
                continue
            if cmd in (DO, DONT, WILL, WONT):
                if i + 2 >= n:
                    break
                option = data[i + 2]
                if cmd == DO:
                    responses.extend(bytes([IAC, WONT, option]))
                elif cmd == WILL:
                    responses.extend(bytes([IAC, DONT, option]))
                # DONT / WONT: ignore (no response needed).
                i += 3
                continue
            if cmd == SB:
                # Skip subnegotiation until IAC SE.
                j = i + 2
                while j + 1 < n:
                    if data[j] == IAC and data[j + 1] == SE:
                        break
                    j += 1
                i = j + 2
                continue
            # Unknown IAC command; skip the IAC + cmd.
            i += 2
        return bytes(payload), bytes(responses)

    # --------------------------------------------------------- command format

    @staticmethod
    def _format_command(command: str, args: str) -> str:
        cmd = command.strip().lstrip(".")
        if not cmd:
            raise EggdropTelnetError("empty eggdrop command")
        full = f".{cmd}"
        stripped_args = args.strip()
        if stripped_args:
            full = f"{full} {stripped_args}"
        return full