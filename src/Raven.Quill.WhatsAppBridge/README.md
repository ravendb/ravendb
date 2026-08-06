# Raven.Quill.WhatsAppBridge

Node service hosting one WhatsApp linked-device session (via [Baileys](https://github.com/WhiskeySockets/Baileys)) per Quill `WhatsAppPersonal` channel. Runs inside the Quill appliance container as the `04-whatsapp` s6 service; the .NET web app is its only client.

## How it fits together

- Session credentials live under `<dataDir>/sessions/{database}/{channelId}/` (multi-file Baileys auth state). Linked sessions are resumed from disk on boot; never-paired sessions leave nothing behind and are lazily recreated by the web app's pairing endpoint.
- Authentication between the two processes is a shared token: the web app mints `<dataDir>/bridge-token` on startup, the bridge blocks at boot until the file exists, and every HTTP call in either direction carries it in `X-Quill-Bridge-Token`.
- Inbound messages are pushed to the web app at `POST {RAVEN_QUILL_WEB_INTERNAL_URL}/internal/whatsapp/inbound` with three retries; replies come back through `POST /sessions/{database}/{channelId}/send`.

## HTTP API (loopback only)

| Route | Result |
| --- | --- |
| `POST /sessions/{database}/{channelId}` | 202 - idempotent start/resume; begins pairing when no credentials exist |
| `GET /sessions/{database}/{channelId}` | `{ state, qr, qrExpiresAt, phoneNumber, lastError }`; 404 when unknown |
| `POST /sessions/{database}/{channelId}/restart` | 202 - wipes credentials when logged out, then re-pairs |
| `POST /sessions/{database}/{channelId}/send` | `{ messageId }`; 409 when the session is not connected |
| `DELETE /sessions/{database}/{channelId}` | 204 - logs out (best effort) and wipes credentials |
| `GET /healthz` | `{ sessions, connected }` (unauthenticated) |

Session states: `starting -> pairing -> connected <-> disconnected -> loggedOut`. The QR payload rotates while pairing; after WhatsApp's QR budget is spent the session parks in `disconnected` ("pairing timed out") until an explicit restart - the bridge never loops QR generation on its own.

## Configuration

| Env var | Default |
| --- | --- |
| `RAVEN_QUILL_WHATSAPP_BRIDGE_LISTEN` | `127.0.0.1:8447` |
| `RAVEN_QUILL_WHATSAPP_DATA_DIR` | `/var/lib/quill/whatsapp` |
| `RAVEN_QUILL_WEB_INTERNAL_URL` | `http://127.0.0.1:5000` |
| `RAVEN_QUILL_WHATSAPP_BRIDGE_LOG_LEVEL` | `info` |

## Development

```bash
npm ci
npm run typecheck
npm test
npm run build && RAVEN_QUILL_WHATSAPP_DATA_DIR=/tmp/wa npm start
```

`patches/` holds a one-line fix for Baileys 7.0.0-rc14 (applied by patch-package on install):
`sendMessageAck` dereferences `creds.me.id` unconditionally, which crashes while acking the
pairing notifications - before pairing completes there is no `creds.me` - and breaks QR
linking entirely. Drop the patch when a Baileys release fixes it.

## Manual QA script (real phone)

1. Boot the appliance, create an app and an agent, then add a **WhatsApp Personal** channel from the dashboard.
2. Scan the QR in the pairing panel with a test phone (WhatsApp > Settings > Linked devices > Link a device). The panel should flip to Connected with the phone number.
3. Send a text to that number from another phone; the agent's reply should arrive in the chat, and the conversation should appear in the dashboard attributed to the channel.
4. Send `/clear`, confirm the confirmation reply and that the next message starts a fresh conversation.
5. Send an image; expect the "text only" fallback reply.
6. Disable the channel in the dashboard; messages should be dropped. Re-enable; replies resume.
7. Delete the channel; the phone's Linked devices list should show the device gone.

Note: personal WhatsApp linking uses an unofficial protocol and is intended for QA and testing; WhatsApp may ban numbers that misuse it.

## Session credentials are secrets

`creds.json` is equivalent to the linked device itself - anyone holding it can run the WhatsApp session. The directory is created `0700` on the appliance's persistent volume; deleting the channel wipes it. Restoring an old volume backup resurrects the link.
