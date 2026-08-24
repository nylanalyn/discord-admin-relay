"""Tests for eggdrop_telnet.EggdropTelnetClient.

Uses asyncio.start_server to emulate an Eggdrop partyline and verifies
login negotiation, command formatting, and bad-password handling.
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from eggdrop_telnet import (
    EggdropTelnetClient,
    EggdropTelnetError,
)

IAC = 0xFF
DO = 0xFD
WILL = 0xFB
WONT = 0xFC
DONT = 0xFE


class FakeEggdropServer:
    """Minimal Eggdrop partyline emulator for tests."""

    def __init__(self, *, bad_password: bool = False):
        self.bad_password = bad_password
        self.server: asyncio.AbstractServer | None = None
        self.host = "127.0.0.1"
        self.port = 0
        self.received_command = b""
        self.received_username = b""
        self.received_password = b""
        self.received_negotiation_response = b""
        self.command_output = (
            b"Partyline users:\r\nnylan\r\nbot> "
        )

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle, self.host, 0
        )
        sock = self.server.sockets[0]
        self.port = sock.getsockname()[1]

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # Send IAC DO 1 (echo) negotiation at the start.
        writer.write(bytes([IAC, DO, 1]))
        # Send login banner.
        writer.write(b"\r\nWelcome to the Eggdrop partyline.\r\n")
        writer.write(b"Please enter your nickname.\r\n")
        await writer.drain()

        # Read first IAC response from client (should be IAC WONT 1).
        # Then read username line.
        self.received_negotiation_response = await self._read_exactly(
            reader, 3
        )

        username_line = await reader.readline()
        self.received_username = username_line
        writer.write(b"\r\nEnter your password.\r\n")
        await writer.drain()

        password_line = await reader.readline()
        self.received_password = password_line
        if self.bad_password:
            writer.write(b"\r\nBad password.\r\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        writer.write(b"\r\nLogged in as nylan.\r\n")
        await writer.drain()

        # Read the dot-command line.
        cmd_line = await reader.readline()
        self.received_command = cmd_line
        # Send the command output.
        writer.write(self.command_output)
        await writer.drain()
        # Brief pause before closing to let the client collect output.
        await asyncio.sleep(0.2)
        writer.close()
        await writer.wait_closed()

    async def _read_exactly(self, reader: asyncio.StreamReader, n: int) -> bytes:
        return await reader.readexactly(n)


class EggdropTelnetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = FakeEggdropServer()
        await self.server.start()

    async def asyncTearDown(self):
        await self.server.stop()

    async def test_run_command_who(self):
        client = EggdropTelnetClient(
            username="nylan",
            password="secret",
            host=self.server.host,
            port=self.server.port,
            login_timeout_seconds=5.0,
            command_timeout_seconds=5.0,
            response_idle_seconds=0.5,
        )
        messages = await client.run_command("who", "")
        self.assertEqual(messages, ["Partyline users:", "nylan", "bot>"])
        self.assertEqual(self.server.received_command, b".who\r\n")
        # Verify the client responded IAC WONT 1 to IAC DO 1.
        self.assertEqual(
            self.server.received_negotiation_response,
            bytes([IAC, WONT, 1]),
        )

    async def test_run_command_op_with_args(self):
        client = EggdropTelnetClient(
            username="nylan",
            password="secret",
            host=self.server.host,
            port=self.server.port,
            login_timeout_seconds=5.0,
            command_timeout_seconds=5.0,
            response_idle_seconds=0.5,
        )
        await client.run_command(".op", "#chan nick")
        self.assertEqual(self.server.received_command, b".op #chan nick\r\n")

    async def test_bad_password_raises(self):
        self.server = FakeEggdropServer(bad_password=True)
        await self.server.start()
        client = EggdropTelnetClient(
            username="nylan",
            password="wrong",
            host=self.server.host,
            port=self.server.port,
            login_timeout_seconds=5.0,
            command_timeout_seconds=5.0,
            response_idle_seconds=0.5,
        )
        with self.assertRaises(EggdropTelnetError) as ctx:
            await client.run_command("who", "")
        self.assertEqual(str(ctx.exception), "eggdrop login failed")


if __name__ == "__main__":
    unittest.main()
