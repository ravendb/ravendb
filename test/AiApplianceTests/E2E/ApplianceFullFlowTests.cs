using System.Net;
using System.Net.Http.Json;
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
        Assert.False(string.IsNullOrWhiteSpace(zipPath),
            "Set APPLIANCE_E2E_SETUP_PACKAGE_PATH to the absolute path of the setup-package zip " +
            "(the one with your real RavenDB license + cert).");
        Assert.True(File.Exists(zipPath),
            $"APPLIANCE_E2E_SETUP_PACKAGE_PATH points at '{zipPath}' but no file is there.");
        var zipBytes = await File.ReadAllBytesAsync(zipPath);

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
            applianceStore: store,
            configureOptions: opts =>
            {
                // Default LlmProvider is "openai" with an empty API key —
                // RavenDB rejects the connection-string put with "ApiKey
                // field cannot be empty" when T11 (W7 provision-agent)
                // fires. Switching to Ollama means no key is required.
                // We don't actually exercise the LLM in T11 — only the
                // connection-string + agent-doc PUT against the per-app
                // RavenDB; T12 doesn't need the LLM either. Tests that
                // exercise live chat would override this back to OpenAI
                // with a real key.
                opts.LlmProvider = "ollama";
                opts.LlmEndpoint = "http://localhost:11434/";
                opts.LlmModel = "llama3.1";
            });
        var client = factory.CreateClient();

        var statusBefore = await client.GetFromJsonAsync<JsonElement>("/api/bootstrap/status");
        Assert.Equal("needs-activation", statusBefore.GetProperty("state").GetString());

        var healthBefore = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, healthBefore.StatusCode);

        // ---------- T3. Redeem license, wait for READY ----------
        var redeem = await client.PostAsJsonAsync("/api/bootstrap/redeem-license",
            new { licenseKey = HardcodedLicenseKey });
        Assert.True(redeem.IsSuccessStatusCode,
            $"redeem returned {redeem.StatusCode}: {await redeem.Content.ReadAsStringAsync()}");

        await WaitForBootstrapStateAsync(client, expected: "ready", timeoutMs: 60_000);

        var healthAfter = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, healthAfter.StatusCode);

        // ---------- T4. Source Postgres with the full canonical Northwind ----------
        // The `northwind-full` dataset (test/SlowTests/Data/npgsql.northwind-full.{create,insert}.sql)
        // is the standard 830-orders / 91-customers / 77-products dump, table
        // names are lowercase plural ("customers", "orders", "products"), all
        // columns snake_case.
        using var sqlTeardown = WithSqlDatabase(MigrationProvider.NpgSQL,
            out var pgConnStr, out _, dataSet: "northwind-full", includeData: true);

        // ---------- T5. Connect (CDC verify) ----------
        // Server-side /admin/cdc-sink/verify requires at least one TableNames
        // entry -- it does table-level CDC capability checks, not just
        // server-level prerequisite checks.
        var connectResp = await client.PostAsJsonAsync("/api/setup/connect", new
        {
            provider         = "Npgsql",
            connectionString = pgConnStr,
            tableNames       = new[] { "customers", "orders", "products" },
        });
        Assert.True(connectResp.IsSuccessStatusCode,
            $"connect returned {connectResp.StatusCode}: {await connectResp.Content.ReadAsStringAsync()}");
        var verify = await connectResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(verify.GetProperty("success").GetBoolean(),
            $"verify should succeed; payload: {verify}");

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

        // ---------- T10. Wait for initial load ----------
        // The northwind-full dataset has 830 orders; 800 is a comfortable
        // floor that still proves the bulk of the dump made it through CDC.
        await WaitForPerAppCdcInitialLoadAsync(store, perAppDatabase: slug!, configName: $"{slug}-cdc", timeoutMs: 120_000);
        var ordersCount = await WaitForPerAppDocumentCountAsync(store, perAppDatabase: slug!, collectionName: "Orders", expectedCount: 800, timeoutMs: 30_000);
        Assert.True(ordersCount >= 800,
            $"expected >=800 Orders after initial load, got {ordersCount}");

        // ---------- T11. AI agent ----------
        var agentResp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/agent",
            new { framing = "customer-support" });
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

        // ---------- T13. Optional manual park ----------
        if (Environment.GetEnvironmentVariable("APPLIANCE_E2E_HOLD") == "1")
        {
            Console.WriteLine($"Embed URL: {client.BaseAddress}embed/{widgetId}");
            Console.WriteLine("Test parked. Ctrl+C to exit.");
            await Task.Delay(Timeout.Infinite);
        }
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
