# fluxer-admin-relay

Admin command relay for HTTP bot APIs and Eggdrop partyline, running on a
[Fluxer](https://fluxer.com) instance instead of Discord. The `main` branch
keeps the Discord (discord.py) variant; this branch is the Fluxer port.

Fluxer is wire-compatible with Discord's core HTTP API (`/v1`, snowflake
IDs, `Authorization: Bearer`) and gateway WebSocket (opcodes, `READY` /
`MESSAGE_CREATE` dispatches), so the bot routing, admin API calls, event
polling, and Eggdrop telnet handling are unchanged. Two differences:

- Fluxer does not implement bot slash commands yet, so only message
  commands (`<bot> <command> <args>` in the admin channel) are served.
- No intents; the gateway IDENTIFY sends `intents: 0`.

## Setup

```sh
pip install .
cp fluxer_admin.example.yaml fluxer_admin.yaml   # fill in env vars / values
fluxer-admin-relay --config fluxer_admin.yaml
```

Config points at your Fluxer instance via `fluxer.instance_url`; the API and
gateway URLs are discovered from `/.well-known/fluxer` (or set
`fluxer.api_url` / `fluxer.gateway_url` explicitly). Create a bot and get a
token from your instance's developer settings, add it to the admin channel,
and set `FLUXER_ADMIN_CHANNEL_ID` plus the allowed user IDs.

The `bots:` section is identical to the Discord variant: `admin_api` bots
(URL + bearer token) and `eggdrop` partyline bots are both supported, and
`admin_api` bots are polled for events that are relayed into the admin
channel.
