# Quill — running locally

End-to-end: stand up the appliance in Docker, point it at a Northwind PostgreSQL source it mirrors via CDC,
provision an AI agent, and chat with it through an embeddable iframe.

The appliance is a single Docker image bundling an **nginx** TLS/SNI front (`:443`) + **RavenDB** (secure) +
the **appliance web app** (`:5000`), supervised by s6. The canonical way to run it locally is
`docker/quill/compose/docker-compose.yml`.

```
browser ─HTTPS :443─> nginx (routes by SNI, one wildcard cert)
                         ├─ dashboard.* / api.* ─(TLS terminate)──────> appliance web :5000
                         ├─ public.*            ─(TLS terminate)──────> appliance web :5000  (/embed)
                         └─ db.* / a.*          ─(TLS passthrough, mTLS)─> RavenDB (in-container, secure)
appliance web ──> RavenDB ──CDC──> PostgreSQL (Northwind) ── mirrors ──> RavenDB collections
agent turn ──> appliance ──> RavenDB AI ──> OpenAI / Ollama (LLM)
```

---

## Prerequisites

- **Docker** (Desktop on Windows/macOS, or engine on Linux) with **Docker Compose**.
- A real **`QUILL_LICENSE_KEY`** issued via the Quill sign-up flow. The appliance activates against
  `api.ravendb.net` on first boot (no offline / demo-zip fallback anymore — see RavenDB-26985).
- An **LLM**: an OpenAI API key **or** a reachable Ollama endpoint. *The agent can't answer without one* —
  the AI Helper only drafts the agent/CDC **config** (via the real RavenDB AI API), not the model replies.
- A **PostgreSQL** server you can load Northwind into, with **`wal_level = logical`** and a login that has the
  **REPLICATION** attribute (the built-in `postgres` superuser qualifies).

---

## 1. PostgreSQL + Northwind

**Option A — throwaway container** (CDC-ready out of the box):

```bash
docker run -d --name nw-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
  postgres:16 -c wal_level=logical
```

**Option B — your existing server**: just confirm it's CDC-ready:

```bash
psql "<conn>" -tAc "SHOW wal_level;"                                   # must print: logical
psql "<conn>" -tAc "SELECT rolreplication FROM pg_roles WHERE rolname=current_user;"  # must be: t
```

**Load Northwind** (the canonical 830-orders dump ships in the repo as zipped SQL):

```bash
unzip -p test/SlowTests/Data/Quill/npgsql.northwind-full.create.zip > /tmp/nw-create.sql
unzip -p test/SlowTests/Data/Quill/npgsql.northwind-full.insert.zip > /tmp/nw-insert.sql

psql "postgresql://postgres:postgres@<host>:5432/postgres" -c "CREATE DATABASE northwind;"
psql "postgresql://postgres:postgres@<host>:5432/northwind" -v ON_ERROR_STOP=1 -f /tmp/nw-create.sql
psql "postgresql://postgres:postgres@<host>:5432/northwind" -v ON_ERROR_STOP=1 -f /tmp/nw-insert.sql
psql "postgresql://postgres:postgres@<host>:5432/northwind" -tAc \
  "SELECT 'orders='||count(*) FROM orders;"                          # expect 830
```

> No local `psql`? Run any of the above inside a throwaway container:
> `docker run -i --rm postgres:16 psql "<conn>" …`

**The connection string the appliance will use** must be reachable **from inside the appliance container**:
- Postgres on your LAN/another host → `Host=<lan-ip>;Port=5432;Database=northwind;Username=postgres;Password=…`
- Postgres on the Docker host (Docker Desktop) → `Host=host.docker.internal;Port=5432;Database=northwind;Username=postgres;Password=…`

(`localhost` will NOT work — inside the container it means the container itself.)

---

## 2. Configure and start Quill

### Get the image

The compose file references `ravendb/quill:${TAG:-latest}`. Either build it locally or pull it from
Docker Hub, depending on where you are in the release cycle:

**Pre-release / local dev** — no image has been published yet (or you're testing an unmerged change).
Build from source:

```bash
./docker/quill/scripts/build.sh --tag ravendb/quill:latest
```

Rebuild any time the Dockerfile or `src/Raven.Quill*` changes.

The image is always compiled from source: `docker/quill/Dockerfile` builds `Raven.Server`,
`rvn`, and `Raven.Studio` from the current repo state.

**Post-release** — a published image exists on Docker Hub. Compose pulls it automatically on
`docker compose up`, or you can prefetch:

```bash
docker pull ravendb/quill:latest             # or a pinned tag: ravendb/quill:0.1.0
```

### Configure `.env`

```bash
cd docker/quill/compose
cp .env.example .env
# Fill in QUILL_API_KEY (operator login) and QUILL_LICENSE_KEY (license key).
```

The image **activates itself at startup** using `QUILL_LICENSE_KEY` — no operator action. Status walks
`NeedsActivation → Redeeming → Restarting → Ready` (~30–60s after boot).

### Start — production posture

```bash
docker compose up
```

- Publishes `:443` — the nginx TLS front is the only external surface.
- Named volume `quill-data` backs `/var/lib/quill` (settings, activation setup package, RavenDB store).
- Watch activation status:
  ```bash
  docker compose logs -f quill                                          # live activation output
  # or from a second terminal:
  docker exec quill curl -s http://127.0.0.1:5000/api/bootstrap/status  # {"state":"Ready"} when done
  ```

---

## 3. Sign in to the dashboard

Once status is `Ready`, open **`https://dashboard.<your-slug>.myquill.ai/`** — the domain baked into the
wildcard cert your setup package carries. `<your-slug>` is whatever your Quill sign-up flow gave you
(check with `docker exec quill cat /var/lib/quill/setup/A/settings.json | grep PublicServerUrl`).
Example: slug `bagheera` → `https://dashboard.bagheera.myquill.ai/`. `*.<slug>.myquill.ai` A records
resolve to 127.0.0.1, so no hosts-file edit is needed. You land on **/login** — enter the **API key**
from `.env` (`QUILL_API_KEY`) and continue; the server issues a `Secure` session cookie and drops you
on the dashboard.

If `:443` isn't up yet, the container is still in pre-activation — the wildcard cert only lands after
the setup package downloads. Wait for `bootstrap/status` to report `Ready`, then retry the URL.

Programmatic / `api.*` callers skip the login screen and pass the key on every request as an
`X-Api-Key` header instead; the CLI steps below use that. The same login from the CLI:

```bash
curl -ki -X POST https://dashboard.<your-slug>.myquill.ai/api/auth/login \
  -H "Content-Type: application/json" -d '{"apiKey":"<your-QUILL_API_KEY>"}'   # 200 + Set-Cookie: quill.session=...
```

---

## 4. Create the app (CDC wizard)

In the appliance UI, run the wizard with your Northwind connection string (provider **`Npgsql`**):

1. **Connect** — reachability check.
2. **Discover** — lists `customers`, `orders`, `products`, … (`hasPermissionToSetup: true`).
3. **Map** — accept the suggested Northwind mapping (demo mode → Customers / Orders / Products).
4. **Provision** — give an app name (e.g. `Northwind Demo`). The slug is derived from it
   (lowercased, hyphenated — `northwind-demo`); pass an optional explicit `slug` to override.
   Creates the per-app DB + CDC task and starts the initial load (Customers 91 / Orders 830 /
   Products 77 mirror into RavenDB).

Note the **slug** (the app's database name; it also appears in public embed URLs). Verify the app exists (admin `/api/*` needs the API key —
either the `X-Api-Key` header, as below, or the session cookie from the dashboard login):

```bash
curl -sk https://dashboard.<your-slug>.myquill.ai/api/apps/ -H "X-Api-Key: <your-QUILL_API_KEY>"   # [{ "slug": "...", "database": "...", ... }]
```

---

## 5. AI connection string + agent + channel

Replace `SLUG` and the key/host below. `/suggest/agent` asks the real RavenDB AI API to draft agent
candidate(s); the example below then provisions a hand-written `product-catalog` agent whose query inputs
are model-filled (so it works over the iframe).

```bash
SLUG=northwind-demo
API_KEY=<your-QUILL_API_KEY>   # admin /api/* needs it on every call

# (a) AI connection string  — OpenAI
curl -sk -X POST https://dashboard.<your-slug>.myquill.ai/api/apps/$SLUG/ai/connection-strings \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"demo-llm\",\"identifier\":\"demo-llm\",\"modelType\":\"Chat\",
       \"openAiSettings\":{\"apiKey\":\"$OPENAI_API_KEY\",\"endpoint\":\"https://api.openai.com/\",\"model\":\"gpt-4.1-mini\"}}"
#   — or Ollama: "ollamaSettings":{"uri":"http://host.docker.internal:11434/","model":"llama3.1"}

# (b) ask the AI Helper to draft agent config candidate(s) (real RavenDB AI API)
curl -sk -X POST https://dashboard.<your-slug>.myquill.ai/api/apps/$SLUG/suggest/agent \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" -d '{"mode":"from-data"}'
#   returns AI-drafted candidate(s); names/shape depend on the model and your CDC mapping

# (c) provision the product-catalog agent (connectionStringName injected)
curl -sk -X POST https://dashboard.<your-slug>.myquill.ai/api/apps/$SLUG/setup/agent \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"identifier":"product-catalog","name":"Product Catalog Assistant","connectionStringName":"demo-llm",
       "systemPrompt":"You are a product-catalog assistant for the Northwind store. Help shoppers search the catalog, compare prices, and check stock availability. Mention when a product is discontinued. Only answer from the catalog data returned by the query tools.",
       "sampleObject":"{\"reply\":\"\"}",
       "queries":[{"name":"searchProducts","description":"Searches products by name; returns price, stock, and the Discontinued flag.","query":"from Products where search(ProductName, $term)","parametersSampleObject":"{ \"term\": \"tea*\" }"}]}'

# (d) bind an iframe channel
curl -sk -X POST https://dashboard.<your-slug>.myquill.ai/api/apps/$SLUG/setup/channel \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"type":"iframe","agentId":"product-catalog","allowedOrigins":[]}'
#   -> {"channelId":"..."}
#   allowedOrigins is required. [] means "any site may frame it"; otherwise list exact
#   scheme://host[:port] entries.

# (e) mint a per-user embed link for the channel (short-lived, invocation-capped)
curl -sk -X POST https://dashboard.<your-slug>.myquill.ai/api/apps/$SLUG/embed-links \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"channelId":"...","ttlSeconds":3600,"maxInvocations":100}'
#   -> {"token":"...","url":"https://public.<your-slug>.myquill.ai/apps/'$SLUG'/embed/<token>", ...}
```

> The UI exposes the same steps (AI connection strings, agent Review form, channels) if you prefer clicking.

---

## 6. Chat

Open the **`url` returned by the mint call** — `https://public.<your-slug>.myquill.ai/apps/<app-slug>/embed/<token>` —
and chat — e.g. *"Do you have Chai? Price, stock, discontinued?"* or *"Search for products with
'lager' in the name."* The token is the bearer credential: there is no static public widget URL,
and links die at their TTL / invocation cap (or on revoke). Smoke-test from the CLI:

```bash
curl -sk -N -X POST https://public.<your-slug>.myquill.ai/apps/<app-slug>/embed/<token>/chat \
  -H "Content-Type: application/json" -d '{"prompt":"Do you have Chai? Give its price and stock."}'
# NDJSON: {"type":"chunk","text":"…"} … {"type":"done","answer":{"reply":"…"},"conversationId":"chats/…"}
```

---

## 7. Embed on your own site

A minted link is a bearer credential with a TTL and a turn cap, so there is no static widget URL — every
visitor gets their own link, minted by **your backend**. The dashboard's channel page prints this flow,
pre-filled with your slug and channel id.

Your server calls `POST /api/apps/{slug}/embed-links` with the operator key and hands the page nothing
but the returned `url`.

> **`X-Api-Key` is server-side only.** It unlocks the whole appliance, and the endpoint sends no CORS
> headers, so a browser `fetch` to it fails preflight anyway. Agent parameters are bound at mint time
> precisely so a visitor cannot choose them.

```html
<iframe id="quill" title="Assistant" style="width:100%;height:600px;border:0"></iframe>

<script type="module">
    // your own endpoint: it mints server-side and returns just { url }
    async function openSession() {
        const response = await fetch("/api/quill-session", { method: "POST" });
        if (!response.ok) throw new Error(`Could not start the assistant: ${response.status}`);

        document.getElementById("quill").src = (await response.json()).url;
    }

    window.addEventListener("message", (event) => {
        if (event.origin !== "https://public.<your-slug>.myquill.ai") return;

        const message = event.data;
        if (message?.source !== "raven-quill" || message.version !== 1) return;
        if (message.type === "expired") void openSession(); // mint a fresh link and keep chatting
    });

    void openSession();
</script>
```

### What the frame tells the host page

A live embed posts these to its parent. Every one is data-free, so the target origin is `"*"`; validate
`source` and `version`, and check `event.origin` against your appliance's `public.*` host.

| `type` | Payload | Meaning |
|---|---|---|
| `ready` | — | The widget mounted. Hide your own loader. |
| `expired` | `{ reason: "expired" \| "limit" }` | Terminal. Mint a new link and reset `iframe.src`. |
| `error` | `{ message }` | Something failed; the frame is showing its own message. |

`expired` also arrives from the server's own notice page, so a link that died *before* the page was served
reports the same thing as one that died mid-conversation. There is no `resize`: the widget is a full-height
panel that scrolls internally, so size the iframe with CSS.

---

## Teardown

```bash
cd docker/quill/compose
docker compose down                 # stop + remove the container; keep the volume
docker compose down -v              # also drop quill-data (wipes quill-config + per-app DBs)
```
```bash
docker rm -f nw-postgres            # if you used the throwaway Postgres
# or, for an existing server: DROP DATABASE northwind;
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Bootstrap stuck at `NeedsActivation` | Startup activation had nothing to redeem. Set `QUILL_LICENSE_KEY` in `.env` to a real license key issued by the Quill sign-up flow and confirm the appliance can reach `api.ravendb.net`. Check `docker compose logs` for the activation line. |
| `401 Unauthorized` on `/api/*` (or bounced to `/login`) | Missing/wrong API key or an expired session. Pass `-H "X-Api-Key: <key>"` (the `QUILL_API_KEY` from `.env`) or sign in again. `QUILL_API_KEY` is **required** — auth fails closed when it's unset. |
| `https://…:443` connection refused | nginx only starts after activation extracts the wildcard cert. During pre-activation, check status via `docker exec quill curl -s http://127.0.0.1:5000/api/bootstrap/status` or watch `docker compose logs quill`. If `:443` never comes up after activation reports `Ready`, check the `03-proxy` service in the logs. |
| Wizard **Connect** fails | The connection-string host isn't reachable **from the container**. Use a LAN IP or `host.docker.internal`, not `localhost`. |
| Discover: `wal_level is 'replica'…` / no permission to set up | Set `wal_level = logical` and restart Postgres; grant the login `REPLICATION`. |
| Chat returns an `error` frame | `docker compose logs` for the real exception. Common ones below. |
| `UnsuccessfulAiRequestException: 401 invalid_api_key` | The LLM key is wrong/expired. Re-POST `ai/connection-strings` with a valid key (test it: `curl -H "Authorization: Bearer $KEY" https://api.openai.com/v1/models`). |
| `MissingAiAgentParameterException: Parameter 'customerId' is missing` | The agent declares a **caller-supplied** agent-level parameter the iframe can't provide (e.g. `order-support`). Use `product-catalog` / `sales-insights`, whose inputs are model-filled query params. |
| Agent runs but finds no rows | Check the query matches the mirrored field **types**, not just names — e.g. Northwind's `Discontinued` mirrors as integer `0/1`, so `Discontinued = false` matches nothing; filter on `= 0` or drop it. Confirm the CDC initial load finished (collection counts via Studio or `collections/stats`). |
| The iframe is blank, nothing in the browser console you can act on | Load the embed URL directly in a tab. A 410 renders "This conversation has ended" (expired, revoked, or a disabled channel — mint a new link). If the page loads directly but not in the iframe, the host page's origin isn't on the channel's `allowedOrigins` (check the browser console for the CSP `frame-ancestors` rejection). |

---

## Production hardening (beyond this runbook)

This is the local-run posture. For a real deployment:

- **Use a high-entropy `QUILL_API_KEY`** (not a weak demo value). The server logs a startup warning if the
  key is short; treat it as a hard requirement in production.
