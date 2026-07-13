# RavenDB Quill — demo runbook (from scratch)

End-to-end: stand up the appliance in Docker, point it at a Northwind PostgreSQL source it mirrors via CDC,
provision an AI agent, and chat with it through an embeddable iframe.

The appliance is a single Docker image bundling an **nginx** TLS/SNI front (`:443`) + **RavenDB** (secure) +
the **appliance web app** (`:5000`), supervised by s6. `up.ps1` builds and runs it; `down.ps1` tears it down.

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

- **Docker** (Desktop on Windows/macOS, or engine on Linux).
- **PowerShell** (to run `up.ps1` / `down.ps1`).
- A **RavenDB setup-package zip** — a secured-setup zip containing `license.json`,
  `admin.client.certificate.*.pfx`, and `A/settings.json` (with a `PublicServerUrl`). Generate it from the
  RavenDB setup wizard / RavenDB Cloud, or `rvn create-setup-package`. *Bootstrap cannot run without it.*
- An **LLM**: an OpenAI API key **or** a reachable Ollama endpoint. *The agent can't answer without one* —
  the AI Helper only drafts the agent/CDC **config** (via the real RavenDB AI API), not the model replies.
- A **PostgreSQL** server you can load Northwind into, with **`wal_level = logical`** and a login that has the
  **REPLICATION** attribute (the built-in `postgres` superuser qualifies).

---

## 1. One-time: supply the license file the image needs

The Dockerfile copies `docker/ai-appliance/license.json` into the bundled RavenDB. It is **gitignored**
(never committed) — you must provide it. Extract it from your setup-package zip:

```bash
unzip -p /path/to/setup-package.zip license.json > docker/ai-appliance/license.json
```

> Skipping this fails the build at `COPY docker/ai-appliance/license.json … not found`.

---

## 2. PostgreSQL + Northwind

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

## 3. Build + run the appliance

```powershell
.\scripts\ai-appliance\up.ps1 -LicenseKey '<setup-package-token>' -RavenApiEnv test
```

- First build is long (publishes RavenDB + the appliance + builds the React frontend); rebuilds are cached.
- `up.ps1` runs the container with the operator API key (`QUILL_API_KEY`, default **`egor`**), publishes
  the nginx TLS front on **:443** (HTTPS) and the web app on **:5000** (first-run / pre-activation), and
  tails logs. At startup the appliance pulls its setup package from the real license API using
  `-LicenseKey` (a real emitted token) — there is no local-zip / offline mode.
- Useful flags: `-LicenseKey <token>` (real setup-package token — **required** to activate),
  `-Rebuild` (no-cache), `-Port <n>` (host :5000 port), `-HttpsPort <n>` (host :443 port),
  `-ApiKey <key>` (operator login key, default `egor`), `-WithStudio` (import the admin client cert so the
  browser can reach RavenDB Studio at `https://db.egor-ai.ravendb.run/`),
  `-RavenApiEnv <env>` (route the AI Helper **and the setup-package download** to `{env}.api.ravendb.net`, e.g. `test`; unset → production).

The appliance **activates itself at startup** — no operator action. Status walks
`NeedsActivation → Redeeming → Restarting → Ready` (~30–60s after the build). Watch it:

```bash
curl -s http://localhost:5000/api/bootstrap/status      # wait for {"state":"Ready"}
```

---

## 4. Sign in to the dashboard

Activation is automatic (step 3) — there is no activation screen. Once status is `Ready`, open
**https://dashboard.egor-ai.ravendb.run/** (nginx terminates TLS with the package's wildcard cert;
`*.ravendb.run` resolves to 127.0.0.1). You land on **/login** — enter the **API key** you ran with
(`QUILL_API_KEY`, default `egor`) and continue; the server issues a `Secure` session cookie and drops you
on the dashboard. (Pre-activation, before `:443` is up, the SPA is also on `http://localhost:5000`.)

Programmatic / `api.*` callers skip the login screen and pass the key on every request as an
`X-Api-Key` header instead; the CLI steps below use that. The same login from the CLI:

```bash
curl -i -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" -d '{"apiKey":"egor"}'   # 200 + Set-Cookie: quill.session=...
```

---

## 5. Create the app (CDC wizard)

In the appliance UI, run the wizard with your Northwind connection string (provider **`Npgsql`**):

1. **Connect** — reachability check.
2. **Discover** — lists `customers`, `orders`, `products`, … (`hasPermissionToSetup: true`).
3. **Map** — accept the suggested Northwind mapping (demo mode → Customers / Orders / Products).
4. **Provision** — give an app name (e.g. `northwind-demo`). Creates the per-app DB + CDC task and starts the
   initial load (Customers 91 / Orders 830 / Products 77 mirror into RavenDB).

Note the **slug** (the app's database name). Verify the app exists (admin `/api/*` needs the API key —
either the `X-Api-Key` header, as below, or the session cookie from the dashboard login):

```bash
curl -s http://localhost:5000/api/apps/ -H "X-Api-Key: egor"   # [{ "slug": "...", "database": "...", ... }]
```

---

## 6. AI connection string + agent + channel

Replace `SLUG` and the key/host below. `/suggest/agent` asks the real RavenDB AI API to draft agent
candidate(s); the example below then provisions a hand-written `product-catalog` agent whose query inputs
are model-filled (so it works over the iframe).

```bash
SLUG=northwind-demo
API_KEY=egor   # operator key (QUILL_API_KEY); admin /api/* needs it on every call

# (a) AI connection string  — OpenAI
curl -s -X POST http://localhost:5000/api/apps/$SLUG/ai/connection-strings \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"demo-llm\",\"identifier\":\"demo-llm\",\"modelType\":\"Chat\",
       \"openAiSettings\":{\"apiKey\":\"$OPENAI_API_KEY\",\"endpoint\":\"https://api.openai.com/\",\"model\":\"gpt-4.1-mini\"}}"
#   — or Ollama: "ollamaSettings":{"uri":"http://host.docker.internal:11434/","model":"llama3.1"}

# (b) ask the AI Helper to draft agent config candidate(s) (real RavenDB AI API)
curl -s -X POST http://localhost:5000/api/apps/$SLUG/suggest/agent \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" -d '{"mode":"from-data"}'
#   returns AI-drafted candidate(s); names/shape depend on the model and your CDC mapping

# (c) provision the product-catalog agent (connectionStringName injected)
curl -s -X POST http://localhost:5000/api/apps/$SLUG/setup/agent \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"identifier":"product-catalog","name":"Product Catalog Assistant","connectionStringName":"demo-llm",
       "systemPrompt":"You are a product-catalog assistant for the Northwind store. Help shoppers search the catalog, compare prices, and check stock availability. Mention when a product is discontinued. Only answer from the catalog data returned by the query tools.",
       "sampleObject":"{\"reply\":\"\"}",
       "queries":[{"name":"searchProducts","description":"Searches products by name; returns price, stock, and the Discontinued flag.","query":"from Products where search(ProductName, $term)","parametersSampleObject":"{ \"term\": \"tea*\" }"}]}'

# (d) bind an iframe channel
curl -s -X POST http://localhost:5000/api/apps/$SLUG/setup/channel \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"type":"iframe","agentId":"product-catalog","allowedOrigins":[]}'
#   -> {"widgetId":"wgt_..."}
```

> The UI exposes the same steps (AI connection strings, agent Review form, channels) if you prefer clicking.

---

## 7. Chat

Open **http://localhost:5000/embed/<widgetId>** and chat — e.g. *"Do you have Chai? Price, stock,
discontinued?"* or *"Search for products with 'lager' in the name."* Smoke-test from the CLI:

```bash
curl -s -N -X POST http://localhost:5000/embed/<widgetId>/chat \
  -H "Content-Type: application/json" -d '{"prompt":"Do you have Chai? Give its price and stock."}'
# NDJSON: {"type":"chunk","text":"…"} … {"type":"done","answer":{"reply":"…"},"conversationId":"chats/…"}
```

---

## Teardown

```powershell
.\scripts\ai-appliance\down.ps1            # stop + remove the container (-PurgeData also drops the volume)
```
```bash
docker rm -f nw-postgres                    # if you used the throwaway Postgres
# or, for an existing server: DROP DATABASE northwind;
rm docker/ai-appliance/license.json         # the build-context license you supplied (gitignored)
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Build fails: `COPY docker/ai-appliance/license.json … not found` | You skipped step 1 — extract `license.json` from the setup-package zip into `docker/ai-appliance/`. |
| Bootstrap stuck at `NeedsActivation` | Startup activation had no token. Pass a real `-LicenseKey` (`QUILL_LICENSE_KEY`) and ensure the license API is reachable (`-RavenApiEnv test` → test.api.ravendb.net). Check `docker logs ai-appliance-demo` for the activation line. |
| `401 Unauthorized` on `/api/*` (or bounced to `/login`) | Missing/wrong API key or an expired session. Pass `-H "X-Api-Key: <key>"` (the `QUILL_API_KEY` you ran with, default `egor`) or sign in again. `QUILL_API_KEY` is **required** — auth fails closed when it's unset. |
| `https://…:443` connection refused | nginx only starts after activation extracts the wildcard cert. Pre-activation use `http://localhost:5000`; once `/api/bootstrap/status` is `Ready`, retry `:443`. If it never comes up, check `docker logs ai-appliance-demo` for the `03-proxy` service. |
| Wizard **Connect** fails | The connection-string host isn't reachable **from the container**. Use a LAN IP or `host.docker.internal`, not `localhost`. |
| Discover: `wal_level is 'replica'…` / no permission to set up | Set `wal_level = logical` and restart Postgres; grant the login `REPLICATION`. |
| Chat returns an `error` frame | `docker logs ai-appliance-demo` for the real exception. Common ones below. |
| `UnsuccessfulAiRequestException: 401 invalid_api_key` | The LLM key is wrong/expired. Re-POST `ai/connection-strings` with a valid key (test it: `curl -H "Authorization: Bearer $KEY" https://api.openai.com/v1/models`). |
| `MissingAiAgentParameterException: Parameter 'customerId' is missing` | The agent declares a **caller-supplied** agent-level parameter the iframe can't provide (e.g. `order-support`). Use `product-catalog` / `sales-insights`, whose inputs are model-filled query params. |
| Agent runs but finds no rows | Check the query matches the mirrored field **types**, not just names — e.g. Northwind's `Discontinued` mirrors as integer `0/1`, so `Discontinued = false` matches nothing; filter on `= 0` or drop it. Confirm the CDC initial load finished (collection counts via Studio or `collections/stats`). |
| Want to inspect mirrored data | Run `up.ps1 -WithStudio` (imports the admin client cert), then open `https://db.egor-ai.ravendb.run/` — nginx passes `db.*` through to RavenDB Studio and the browser prompts for the client cert. |

---

## Production hardening (beyond this demo)

This runbook is the demo posture. For a real deployment:

- **Publish only `:443`** (the nginx SNI front), not `:5000`. The plain-HTTP `:5000` surface is a
  first-run/dev convenience; exposing it publicly would let a session ride over HTTP. The session cookie
  is `Secure` on the real `https://dashboard.*` flow regardless (it's only non-Secure on the local
  `:5000` fallback).
- **Use a high-entropy `QUILL_API_KEY`** (not the demo `egor`). The server logs a startup warning if the
  key is short; treat it as a hard requirement in production.
