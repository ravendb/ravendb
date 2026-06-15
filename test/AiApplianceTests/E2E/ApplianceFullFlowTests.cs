using System.Net;
using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests.E2E;

/// End-to-end happy path for the AI Appliance. Drives the full feature flow:
/// license redemption -> CDC wizard (Connect / Discover / Map / Test / Provision)
/// -> initial load -> AI agent + iFrame channel. Written upfront with all 13 step
/// assertions present; goes RED at the first unimplemented step. Each slice in
/// the plan's "Roadmap to E2E GREEN" turns one more assertion GREEN.
///
/// Prereqs:
///   - RAVEN_NPGSQL_CONNECTION_STRING env var pointing at a Postgres the test
///     infrastructure can create + drop databases on.
///   - *.egor-ai.ravendb.run DNS -> 127.0.0.1 (the wildcard cert from the
///     embedded setup-package zip is for that domain).
///
/// Optional: set APPLIANCE_E2E_HOLD=1 to park the test after T12 so you can
/// open the live iFrame in a browser.
public class ApplianceFullFlowTests(ITestOutputHelper output) : CdcSinkIntegrationTestBase(output)
{
    private const string HardcodedLicenseKey = "egor-ai-test-license";

    [RavenFact(RavenTestCategory.AiAppliance | RavenTestCategory.Sinks, NpgSqlRequired = true)]
    public async Task EndToEnd_FullApplianceFlow_PostgresSourceToIFrameAgent_Works()
    {
        // ---------- T1. Mock license API serving the real setup-package zip ----------
        // The zip carries a real license + admin cert and is never committed. Caller supplies its
        // location via APPLIANCE_E2E_SETUP_PACKAGE_PATH. CI will substitute a synthetic mock zip
        // through the same env var.
        var zipPath = Environment.GetEnvironmentVariable("APPLIANCE_E2E_SETUP_PACKAGE_PATH");
        if (string.IsNullOrWhiteSpace(zipPath))
        {
            // A missing prerequisite is a "can't run here", not a failure — skip
            // (matches how the rest of the integration suite gates on env). The
            // test is already Postgres-gated via NpgSqlRequired.
            Assert.Skip("Set APPLIANCE_E2E_SETUP_PACKAGE_PATH to the absolute path of the setup-package zip " +
                "(the one with your real RavenDB license + cert) to run this end-to-end test.");
        }
        // A path that IS set but points nowhere is a misconfiguration, not an
        // absent prerequisite — keep that a hard failure.
        Assert.True(File.Exists(zipPath),
            $"APPLIANCE_E2E_SETUP_PACKAGE_PATH points at '{zipPath}' but no file is there.");
        var zipBytes = await File.ReadAllBytesAsync(zipPath);

        // T14 asserts a real streamed agent reply, so the agent's AI connection
        // string (T11a) needs a live OpenAI key. Same env var the rest of the
        // AI-integration suite uses — skip (don't fail) when it's absent.
        var openAiKey = RavenTestHelper.EnvironmentVariables.AiIntegrationOpenAiApiKey;
        if (string.IsNullOrWhiteSpace(openAiKey))
        {
            Assert.Skip("Set RAVEN_AI_INTEGRATION_OPENAI_API_KEY to a real OpenAI key — T11a provisions an OpenAI " +
                "connection string and T14 asserts a real streamed reply through it.");
        }

        await using var licenseApi = await MockLicenseApi.StartAsync(HardcodedLicenseKey, zipBytes);
        var setupRoot = NewDataPath(forceCreateDir: true, prefix: "egor-ai-setup");

        // ---------- T2. Appliance starts in NEEDS-ACTIVATION ----------
        // Single owner: the WAF registers `store` as a singleton in its DI
        // container, which disposes IDisposable singletons during host shutdown
        // — so no `using` here. (RavenTestBase tracks the store separately for
        // class teardown; that's a second touch but DocumentStore.Dispose is
        // idempotent, so it's a no-op when the WAF got there first.)
        var store = GetDocumentStore();
        using var factory = new ApplianceWebApplicationFactory(
            licenseApiUrl: licenseApi.BaseAddress,
            setupPackagePath: setupRoot,
            applianceStore: store);
        var client = factory.CreateClient();

        // The JSON status API returns the BootstrapPhase enum as its PascalCase
        // name (kebab-case is reserved for non-JSON surfaces like /healthz — see
        // BootstrapPhaseExtensions).
        var statusBefore = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
        Assert.Equal("NeedsActivation", statusBefore.GetProperty("state").GetString());

        var healthBefore = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, healthBefore.StatusCode);

        // ---------- T3. Redeem license, wait for READY ----------
        var redeem = await client.PostAsJsonAsync("/api/bootstrap/redeem-license",
            new { licenseKey = HardcodedLicenseKey });
        Assert.True(redeem.IsSuccessStatusCode,
            $"redeem returned {redeem.StatusCode}: {await redeem.Content.ReadAsStringAsync()}");

        await WaitForBootstrapStateAsync(client, expected: "Ready", timeoutMs: 60_000);

        var healthAfter = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, healthAfter.StatusCode);

        // ---------- T4. Source Postgres with the full canonical Northwind ----------
        // The `northwind-full` dataset (test/SlowTests/Data/npgsql.northwind-full.{create,insert}.sql)
        // is the standard 830-orders / 91-customers / 77-products dump, table
        // names are lowercase plural ("customers", "orders", "products"), all
        // columns snake_case.
        using var sqlTeardown = WithSqlDatabase(MigrationProvider.NpgSQL,
            out var pgConnStr, out _, dataSet: "northwind-full", includeData: true);

        // ---------- T5. Connect (reachability probe) ----------
        // Connect is now a plain SQL test-connection: "can we open a connection to
        // the source?" All CDC-readiness verification moved into Discover (the merged
        // /admin/cdc-sink/schema), so no table list is sent here.
        var connectResp = await client.PostAsJsonAsync("/api/setup/connect", new
        {
            provider         = "Npgsql",
            connectionString = pgConnStr,
        });
        Assert.True(connectResp.IsSuccessStatusCode,
            $"connect returned {connectResp.StatusCode}: {await connectResp.Content.ReadAsStringAsync()}");
        var connect = await connectResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(connect.GetProperty("success").GetBoolean(),
            $"connect (reachability) should succeed against a live Postgres; payload: {connect}");

        // ---------- T6. Discover schema ----------
        var discoverResp = await client.PostAsJsonAsync("/api/setup/discover",
            new { provider = "Npgsql", connectionString = pgConnStr });
        Assert.True(discoverResp.IsSuccessStatusCode,
            $"discover returned {discoverResp.StatusCode}: {await discoverResp.Content.ReadAsStringAsync()}");
        var schema = await discoverResp.Content.ReadFromJsonAsync<JsonElement>();

        var tableNames = schema.GetProperty("tables").EnumerateArray()
            .Select(t => t.GetProperty("sourceTableName").GetString()!.ToLowerInvariant())
            .ToHashSet();
        Assert.NotEmpty(tableNames);
        Assert.Contains("customers", tableNames);
        Assert.Contains("orders",    tableNames);
        Assert.Contains("products",  tableNames);

        // Verification is now folded into discovery. The source is provably CDC-ready
        // (T10 below runs a real initial load over a logical-replication slot), so the
        // merged /schema must report success and the connecting user's setup permission.
        Assert.True(schema.GetProperty("success").GetBoolean(),
            $"discover should report a CDC-ready source; payload: {schema}");
        Assert.True(schema.GetProperty("hasPermissionToSetup").GetBoolean(),
            "the connecting user provisions CDC in T10, so hasPermissionToSetup must be true");

        // ---------- T7. Map: POST a pre-built CdcSinkConfiguration for Northwind ----------
        var configFixturePath = Path.Combine(AppContext.BaseDirectory, "E2E", "Fixtures", "northwind-cdc-config.json");
        Assert.True(File.Exists(configFixturePath),
            $"Pre-built CDC config fixture missing at {configFixturePath}. Populated by the W3 Map slice.");
        var configJson = await File.ReadAllTextAsync(configFixturePath);

        var mapResp = await client.PostAsync("/api/setup/map",
            new StringContent(configJson, Encoding.UTF8, "application/json"));
        Assert.True(mapResp.IsSuccessStatusCode,
            $"map returned {mapResp.StatusCode}: {await mapResp.Content.ReadAsStringAsync()}");

        // ---------- T8. Test-mapping ----------
        // "customers" exists in the Map fixture and has 91 rows in the
        // northwind-full dataset.
        var testResp = await client.PostAsJsonAsync("/api/setup/test-mapping",
            new { sourceTableName = "customers", maxRows = 50 });
        Assert.True(testResp.IsSuccessStatusCode,
            $"test-mapping returned {testResp.StatusCode}: {await testResp.Content.ReadAsStringAsync()}");
        var testResult = await testResp.Content.ReadFromJsonAsync<TestCdcSinkMappingResult>();
        Assert.NotNull(testResult);
        Assert.True(testResult!.Results.Count > 0,
            $"expected non-empty test-mapping result; errors=[{string.Join("; ", testResult.Errors)}]");

        // ---------- T9. Provision ----------
        var provisionResp = await client.PostAsJsonAsync("/api/setup/provision",
            new { appName = "northwind-demo" });
        Assert.True(provisionResp.IsSuccessStatusCode,
            $"provision returned {provisionResp.StatusCode}: {await provisionResp.Content.ReadAsStringAsync()}");
        var provisionJson = await provisionResp.Content.ReadFromJsonAsync<JsonElement>();
        var appDocId = provisionJson.GetProperty("id").GetString();
        var slug     = provisionJson.GetProperty("slug").GetString();
        Assert.False(string.IsNullOrEmpty(appDocId));
        Assert.False(string.IsNullOrEmpty(slug));

        // ---------- T9b. cdc/progress live feed (WebSocket) ----------
        // During the initial-load window, the bridge proxies RavenDB's native
        // cdc-sink/performance/live feed. Assert at least one (non-close) frame
        // relays through — the ticket's "progress event during initial load" AC.
        var cdcWsClient = factory.Server.CreateWebSocketClient();
        var cdcWsUri = new Uri(factory.Server.BaseAddress, $"api/apps/{slug}/cdc/progress");
        using (var cdcCts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        using (var cdcWs = await cdcWsClient.ConnectAsync(cdcWsUri, cdcCts.Token))
        {
            var frameBuffer = new byte[16 * 1024];
            var frame = await cdcWs.ReceiveAsync(new ArraySegment<byte>(frameBuffer), cdcCts.Token);
            Assert.NotEqual(WebSocketMessageType.Close, frame.MessageType);
        }

        // ---------- T10. Wait for initial load ----------
        // The northwind-full dataset has 830 orders; 800 is a comfortable
        // floor that still proves the bulk of the dump made it through CDC.
        await WaitForPerAppCdcInitialLoadAsync(store, perAppDatabase: slug!, configName: $"{slug}-cdc", timeoutMs: 120_000);
        var ordersCount = await WaitForPerAppDocumentCountAsync(store, perAppDatabase: slug!, collectionName: "Orders", expectedCount: 800, timeoutMs: 30_000);
        Assert.True(ordersCount >= 800,
            $"expected >=800 Orders after initial load, got {ordersCount}");

        // ---------- T11a. Create the AI connection string ----------
        // Wizard step: operator picks "add new" on the LLM step and submits
        // their LLM details (provider, endpoint, model, api key). OpenAI here
        // so T14 can stream a real agent reply against the CDC-mirrored data.
        var csResp = await client.PostAsJsonAsync($"/api/apps/{slug}/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                openAiSettings = new { apiKey = openAiKey, endpoint = "https://api.openai.com/", model = "gpt-4.1-mini" }
            });
        Assert.True(csResp.IsSuccessStatusCode,
            $"ai connection-string returned {csResp.StatusCode}: {await csResp.Content.ReadAsStringAsync()}");

        // ---------- T11b. Provision agent referencing the CS ----------
        // The T12 channel step and T14 embed chat resolve this agent from the
        // per-app database (no compile-time registry). sampleObject uses
        // PascalCase keys so the streamed reply field ("Reply", resolved at
        // runtime from sampleObject's first property) matches the keys the model
        // emits and the reply streams incrementally as chunks.
        var agentResp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/agent",
            new
            {
                identifier = "demo-agent",
                name = "Support Bot",
                systemPrompt = "You are a helpful Northwind support agent.",
                connectionStringName = "demo-llm",
                sampleObject = "{\"Reply\":\"A friendly reply for the user.\",\"Related\":[\"orders/1\"]}",
            });
        Assert.True(agentResp.IsSuccessStatusCode,
            $"agent returned {agentResp.StatusCode}: {await agentResp.Content.ReadAsStringAsync()}");
        var agentJson = await agentResp.Content.ReadFromJsonAsync<JsonElement>();
        var agentId = agentJson.GetProperty("agentId").GetString();
        Assert.False(string.IsNullOrEmpty(agentId));

        // ---------- T12. iFrame channel ----------
        var channelResp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = new[] { "http://localhost" } });
        Assert.True(channelResp.IsSuccessStatusCode,
            $"channel returned {channelResp.StatusCode}: {await channelResp.Content.ReadAsStringAsync()}");
        var channelJson = await channelResp.Content.ReadFromJsonAsync<JsonElement>();
        var widgetId = channelJson.GetProperty("widgetId").GetString();
        Assert.False(string.IsNullOrEmpty(widgetId));

        // ---------- T12b. Mint a per-user embed link (RavenDB-26775) ----------
        // The widgetId is the durable config anchor; the customer's backend mints a
        // short-lived, invocation-capped token link per end-user. The token is the
        // bearer credential in the iframe URL — there is no static public widget URL.
        var linkResp = await client.PostAsJsonAsync($"/api/apps/{slug}/embed-links",
            new { agentId, ttlSeconds = 3600, maxInvocations = 10 });
        Assert.True(linkResp.IsSuccessStatusCode,
            $"mint embed-link returned {linkResp.StatusCode}: {await linkResp.Content.ReadAsStringAsync()}");
        var linkJson = await linkResp.Content.ReadFromJsonAsync<JsonElement>();
        var token = linkJson.GetProperty("token").GetString();
        Assert.Matches("^[a-f0-9]{32}$", token!);
        Assert.EndsWith($"/embed/{token}", linkJson.GetProperty("url").GetString());

        // ---------- T13. Embed page renders ----------
        var embedResp = await client.GetAsync($"/embed/{token}");
        Assert.Equal(HttpStatusCode.OK, embedResp.StatusCode);
        Assert.Contains("text/html", embedResp.Content.Headers.ContentType?.ToString() ?? "");
        var embedHtml = await embedResp.Content.ReadAsStringAsync();
        Assert.False(string.IsNullOrWhiteSpace(embedHtml), "embed page body was empty");
        Assert.Contains(token!, embedHtml);

        // ---------- T14. Embed chat streams a real agent reply ----------
        // Public token route -> AgentRouter -> the per-app "demo-agent" registered
        // in T11b -> the OpenAI CS from T11a. The demo's closing moment: a browser
        // chatting with the agent over the CDC-mirrored Postgres data. Parameters
        // and the conversation are owned by the minted link; the body is just the prompt.
        using var chatCts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var chatReq = new HttpRequestMessage(HttpMethod.Post, $"/embed/{token}/chat")
        {
            Content = JsonContent.Create(new { prompt = "Say hello in one short sentence." }),
        };
        var chatResp = await client.SendAsync(chatReq, HttpCompletionOption.ResponseHeadersRead, chatCts.Token);
        Assert.True(chatResp.IsSuccessStatusCode,
            $"embed chat returned {chatResp.StatusCode}: {await chatResp.Content.ReadAsStringAsync(chatCts.Token)}");
        Assert.Contains("application/x-ndjson", chatResp.Content.Headers.ContentType?.ToString() ?? "");

        var (replyText, sawDone, error, _) = await ReadEmbedChatAsync(chatResp, chatCts.Token);
        Assert.True(string.IsNullOrEmpty(error), $"embed chat emitted an error frame: {error}");
        Assert.True(sawDone, "embed chat stream did not emit a 'done' frame");
        Assert.False(string.IsNullOrWhiteSpace(replyText), "embed chat produced no reply text");

        // ---------- T14b. The link owns the conversation across turns ----------
        // Turn 2 reuses the same token; the server binds it to the same conversation
        // (the public surface no longer accepts a client-supplied conversation id).
        using var chat2Cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var chat2Req = new HttpRequestMessage(HttpMethod.Post, $"/embed/{token}/chat")
        {
            Content = JsonContent.Create(new { prompt = "Repeat your previous greeting in the same words." }),
        };
        var chat2Resp = await client.SendAsync(chat2Req, HttpCompletionOption.ResponseHeadersRead, chat2Cts.Token);
        Assert.True(chat2Resp.IsSuccessStatusCode,
            $"embed chat turn 2 returned {chat2Resp.StatusCode}: {await chat2Resp.Content.ReadAsStringAsync(chat2Cts.Token)}");

        var (reply2, sawDone2, error2, _) = await ReadEmbedChatAsync(chat2Resp, chat2Cts.Token);
        Assert.True(string.IsNullOrEmpty(error2), $"embed chat turn 2 emitted an error frame: {error2}");
        Assert.True(sawDone2, "embed chat turn 2 did not emit a 'done' frame");
        Assert.False(string.IsNullOrWhiteSpace(reply2), "embed chat turn 2 produced no reply text");

        // ---------- Optional manual park ----------
        if (Environment.GetEnvironmentVariable("APPLIANCE_E2E_HOLD") == "1")
        {
            Console.WriteLine($"Embed URL: {client.BaseAddress}embed/{token}");
            Console.WriteLine("Test parked. Ctrl+C to exit.");
            await Task.Delay(Timeout.Infinite);
        }
    }

    /// <summary>
    /// Reads the embed chat NDJSON stream, accumulating <c>chunk</c> text until
    /// a <c>done</c> or <c>error</c> frame (or end of stream). Also captures the
    /// <c>conversationId</c> echoed in the <c>done</c> frame so the caller can
    /// continue the thread.
    /// </summary>
    private static async Task<(string Reply, bool SawDone, string? Error, string? ConversationId)> ReadEmbedChatAsync(
        HttpResponseMessage resp, CancellationToken ct)
    {
        var sb = new StringBuilder();
        var sawDone = false;
        string? error = null;
        string? conversationId = null;

        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var reader = new StreamReader(stream);

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            using var doc = JsonDocument.Parse(line);
            var type = doc.RootElement.GetProperty("type").GetString();
            if (type == "chunk")
            {
                sb.Append(doc.RootElement.GetProperty("text").GetString());
            }
            else if (type == "done")
            {
                sawDone = true;
                if (doc.RootElement.TryGetProperty("conversationId", out var cid) && cid.ValueKind == JsonValueKind.String)
                    conversationId = cid.GetString();

                // Fall back to the final structured answer when the reply didn't
                // stream incrementally, so we still assert a real reply arrived.
                if (sb.Length == 0 &&
                    doc.RootElement.TryGetProperty("answer", out var answer) &&
                    answer.ValueKind == JsonValueKind.Object &&
                    answer.TryGetProperty("reply", out var reply) &&
                    reply.ValueKind == JsonValueKind.String)
                {
                    sb.Append(reply.GetString());
                }

                break;
            }
            else if (type == "error")
            {
                error = doc.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "error";
                break;
            }
        }

        return (sb.ToString(), sawDone, error, conversationId);
    }

    /// <summary>
    /// Per-app variant of <see cref="WaitForCdcInitialLoadAsync(IDocumentStore, string, int)"/>.
    /// The base helper resolves the in-process DocumentDatabase from the store's
    /// default DB; we need to target the per-app DB Provision just created.
    /// </summary>
    private async Task WaitForPerAppCdcInitialLoadAsync(IDocumentStore store, string perAppDatabase, string configName, int timeoutMs)
    {
        var db = await Databases.GetDocumentDatabaseInstanceFor(store, perAppDatabase);
        var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
        if (process == null)
            throw new InvalidOperationException($"CDC Sink process '{configName}' not found on '{perAppDatabase}'");

        var completed = await Task.WhenAny(process.InitialLoadCompleted, Task.Delay(timeoutMs));
        if (completed != process.InitialLoadCompleted)
            throw new TimeoutException($"CDC Sink '{configName}' on '{perAppDatabase}' initial load did not complete within {timeoutMs}ms");

        await process.InitialLoadCompleted; // propagate exception if any
    }

    /// <summary>
    /// Per-app variant of <see cref="WaitForDocumentCountAsync(IDocumentStore, string, int, int)"/>.
    /// Opens the session against the per-app database explicitly.
    /// </summary>
    private static async Task<int> WaitForPerAppDocumentCountAsync(IDocumentStore store, string perAppDatabase, string collectionName, int expectedCount, int timeoutMs)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var count = 0;
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            using (var session = store.OpenAsyncSession(perAppDatabase))
            {
                count = await session.Query<dynamic>(collectionName: collectionName).CountAsync();
                if (count >= expectedCount)
                    return count;
            }

            await Task.Delay(250);
        }
        return count;
    }

    private static async Task WaitForBootstrapStateAsync(HttpClient client, string expected, int timeoutMs)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            try
            {
                var status = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
                if (status.GetProperty("state").GetString() == expected)
                    return;
            }
            catch (Exception)
            {
                // status endpoint may be momentarily unavailable mid-bootstrap; keep polling.
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"BootstrapState did not reach '{expected}' within {timeoutMs}ms.");
    }
}
