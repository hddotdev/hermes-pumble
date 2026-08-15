# Hermes Pumble Platform Plugin

A community platform adapter that connects Hermes Agent to Pumble through the
[Pumble API Key addon](https://pumble.com/api).

## What it does

- Polls Pumble channels for new messages
- Deduplicates messages by Pumble message ID
- Sends Hermes replies with `POST /sendMessage`
- Supports channel allowlists, user allowlists, optional DM polling, and
  optional `@hermes` mention gating
- Registers as a normal Hermes `kind: platform` plugin, with no Hermes core
  changes

Pumble's API-key addon provides REST reads/writes but does not expose a
long-lived inbound event stream, so this adapter intentionally uses polling.
That makes it simple and portable, but it is not as low-latency as a native
websocket or webhook integration.

## Install locally

```bash
mkdir -p "$HERMES_HOME/plugins/pumble-platform"
cp plugin.yaml __init__.py adapter.py "$HERMES_HOME/plugins/pumble-platform/"
hermes plugins enable pumble-platform
```

For a direct Git checkout:

```bash
git clone https://github.com/hdsysdev/hermes-pumble.git
cp -r hermes-pumble "$HERMES_HOME/plugins/pumble-platform"
hermes plugins enable pumble-platform
```

## Configure

Install the **API Key** addon in Pumble and create a key. Put the key in
`$HERMES_HOME/.env`:

```dotenv
PUMBLE_API_KEY=replace-me
PUMBLE_CHANNELS=general,24-character-channel-id
PUMBLE_HOME_CHANNEL=general
PUMBLE_ALLOWED_USERS=24-character-user-id
PUMBLE_POLL_SECONDS=5
```

The API key is workspace-wide and should be treated like a password. Use a
separate service account if possible. Do not commit it or paste it into an
issue/chat.

Optional controls:

```dotenv
# Include direct-message channels; off by default.
PUMBLE_INCLUDE_DMS=false

# Require an @hermes mention in monitored channels; off by default.
PUMBLE_REQUIRE_MENTION=true
PUMBLE_MENTION=@hermes

# Ignore messages created by the authenticated Pumble user.
PUMBLE_USER_ID=your-pumble-user-id
```

Then restart the gateway:

```bash
hermes plugins enable pumble-platform
systemctl --user reload hermes-gateway.service
hermes status --all
```

The adapter only monitors public/non-DM channels by default. Set
`PUMBLE_CHANNELS` to restrict it to selected channels. `PUMBLE_HOME_CHANNEL`
is used for cron/notification delivery.

## Current limitations

- Polling latency is normally one polling interval.
- There is no inbound webhook/event API in the API-key addon used here.
- The first poll establishes a baseline and does not replay existing history.
- Media and thread replies are not yet implemented; text messages and standard
  channel replies are supported.

## Development

The adapter imports Hermes runtime modules when loaded by Hermes. For local
syntax/tests, use the Hermes virtualenv and source tree on `PYTHONPATH`:

```bash
PYTHONPATH=/home/debian/.hermes/hermes-agent \
  /home/debian/.hermes/hermes-agent/venv/bin/python -m unittest discover -s tests -v
```

## License

MIT
