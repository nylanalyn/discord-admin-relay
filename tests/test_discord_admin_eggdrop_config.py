"""Tests for DiscordAdminRouter._load_bots typed target handling."""

import os
import sys
import unittest
from pathlib import Path

# Ensure we can import discord_admin from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# discord_admin.py imports discord.py at module level; patch if missing.
import discord_admin


class LoadBotsTests(unittest.TestCase):
    def test_admin_api_entry_without_type_defaults(self):
        raw = {
            "aria": {
                "url": "http://127.0.0.1:9101",
                "token": "tok",
            }
        }
        bots = discord_admin.DiscordAdminRouter._load_bots(raw, Path("."))
        self.assertEqual(bots["aria"]["type"], "admin_api")
        self.assertEqual(bots["aria"]["url"], "http://127.0.0.1:9101")
        self.assertEqual(bots["aria"]["token"], "tok")

    def test_eggdrop_entry_returns_client(self):
        raw = {
            "eggdrop": {
                "type": "eggdrop",
                "username": "nylan",
                "password": "secret",
                "host": "127.0.0.1",
                "port": 6465,
            }
        }
        bots = discord_admin.DiscordAdminRouter._load_bots(raw, Path("."))
        self.assertEqual(bots["eggdrop"]["type"], "eggdrop")
        from eggdrop_telnet import EggdropTelnetClient
        self.assertIsInstance(bots["eggdrop"]["client"], EggdropTelnetClient)

    def test_eggdrop_entry_password_env(self):
        os.environ["EGGDROP_PARTYLINE_PASSWORD"] = "envpass"
        try:
            raw = {
                "eggdrop": {
                    "type": "eggdrop",
                    "username": "nylan",
                    "password_env": "EGGDROP_PARTYLINE_PASSWORD",
                }
            }
            bots = discord_admin.DiscordAdminRouter._load_bots(raw, Path("."))
            self.assertEqual(bots["eggdrop"]["type"], "eggdrop")
            self.assertEqual(bots["eggdrop"]["client"].password, "envpass")
        finally:
            del os.environ["EGGDROP_PARTYLINE_PASSWORD"]

    def test_eggdrop_missing_password_raises(self):
        raw = {
            "eggdrop": {
                "type": "eggdrop",
                "username": "nylan",
            }
        }
        with self.assertRaises(ValueError) as ctx:
            discord_admin.DiscordAdminRouter._load_bots(raw, Path("."))
        self.assertIn("requires username and password", str(ctx.exception))

    def test_unsupported_type_raises(self):
        raw = {
            "bad": {
                "type": "unknown",
                "url": "http://x",
                "token": "t",
            }
        }
        with self.assertRaises(ValueError) as ctx:
            discord_admin.DiscordAdminRouter._load_bots(raw, Path("."))
        self.assertIn("unsupported type", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
