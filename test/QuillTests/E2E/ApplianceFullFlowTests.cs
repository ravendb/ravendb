using System.Net;
using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Quill.AiHelper;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests.E2E;

public class ApplianceFullFlowTests(ITestOutputHelper output) : CdcSinkIntegrationTestBase(output)
{
    private const string HardcodedLicenseKey = "egor-ai-test-license";

    [RavenFact(RavenTestCategory.Quill | RavenTestCategory.Sinks, NpgSqlRequired = true)]
    public async Task EndToEnd_FullApplianceFlow_PostgresSourceToIFrameAgent_Works()
    {
        var zipPath = Environment.GetEnvironmentVariable("APPLIANCE_E2E_SETUP_PACKAGE_PATH");
        if (string.IsNullOrWhiteSpace(zipPath))
        {
            Assert.Skip("Set APPLIANCE_E2E_SETUP_PACKAGE_PATH to the absolute path of the setup-package zip " +
                "(the one with your real RavenDB license + cert) to run this end-to-end test.");
        }
        Assert.True(File.Exists(zipPath),
            $"APPLIANCE_E2E_SETUP_PACKAGE_PATH points at '{zipPath}' but no file is there.");
        var zipBytes = await File.ReadAllBytesAsync(zipPath);

        var openAiKey = RavenTestHelper.EnvironmentVariables.AiIntegrationOpenAiApiKey;
        if (string.IsNullOrWhiteSpace(openAiKey))
        {
            Assert.Skip("Set RAVEN_AI_INTEGRATION_OPENAI_API_KEY to a real OpenAI key — T11a provisions an OpenAI " +
                "connection string and T14 asserts a real streamed reply through it.");
        }

        var setupRoot = NewDataPath(forceCreateDir: true, prefix: "egor-ai-setup");

        var store = GetDocumentStore();
        using var factory = new ApplianceWebApplicationFactory(
            setupPackagePath: setupRoot,
            applianceStore: store,
            configureOptions: opts => opts.LicenseKey = HardcodedLicenseKey,
            configureServices: services =>
            {
                services.RemoveAll<ILicenseClient>();
                services.AddSingleton<ILicenseClient>(new FakeLicenseClient(HardcodedLicenseKey, zipBytes));
            });
        var client = factory.CreateClient();

        await WaitForBootstrapStateAsync(client, expected: "Ready", timeoutMs: 60_000);

        var healthAfter = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, healthAfter.StatusCode);

        using var sqlTeardown = WithSqlDatabase(MigrationProvider.NpgSQL,
            out var pgConnStr, out _, dataSet: "northwind-full", includeData: true);

        // the app id the FE supplies from the first wizard step; every step carries it so the per-app wizard doc lines up
        const string appSlug = "northwind-demo";

        var connectResp = await client.PostAsJsonAsync("/api/setup/connect", new
        {
            provider         = "Npgsql",
            connectionString = pgConnStr,
            slug             = appSlug,
        });
        Assert.True(connectResp.IsSuccessStatusCode,
            $"connect returned {connectResp.StatusCode}: {await connectResp.Content.ReadAsStringAsync()}");
        var connect = await connectResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(connect.GetProperty("success").GetBoolean(),
            $"connect (reachability) should succeed against a live Postgres; payload: {connect}");

        var discoverResp = await client.PostAsJsonAsync("/api/setup/discover",
            new { provider = "Npgsql", connectionString = pgConnStr, slug = appSlug });
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

        Assert.True(schema.GetProperty("success").GetBoolean(),
            $"discover should report a CDC-ready source; payload: {schema}");
        Assert.True(schema.GetProperty("hasPermissionToSetup").GetBoolean(),
            "the connecting user provisions CDC in T10, so hasPermissionToSetup must be true");

        var configFixturePath = Path.Combine(AppContext.BaseDirectory, "E2E", "Fixtures", "northwind-cdc-config.json");
        Assert.True(File.Exists(configFixturePath),
            $"Pre-built CDC config fixture missing at {configFixturePath}. Populated by the W3 Map slice.");
        var configNode = JsonNode.Parse(await File.ReadAllTextAsync(configFixturePath))!;
        configNode["slug"] = appSlug;   // the FE-supplied app id keys the wizard doc; the fixture is otherwise app-agnostic

        var mapResp = await client.PostAsync("/api/setup/map",
            new StringContent(configNode.ToJsonString(), Encoding.UTF8, "application/json"));
        Assert.True(mapResp.IsSuccessStatusCode,
            $"map returned {mapResp.StatusCode}: {await mapResp.Content.ReadAsStringAsync()}");

        var testResp = await client.PostAsJsonAsync("/api/setup/test-mapping",
            new { sourceTableName = "customers", maxRows = 50, slug = appSlug });
        Assert.True(testResp.IsSuccessStatusCode,
            $"test-mapping returned {testResp.StatusCode}: {await testResp.Content.ReadAsStringAsync()}");
        var testResult = await testResp.Content.ReadFromJsonAsync<TestCdcSinkMappingResult>();
        Assert.NotNull(testResult);
        Assert.True(testResult!.Results.Count > 0,
            $"expected non-empty test-mapping result; errors=[{string.Join("; ", testResult.Errors)}]");

        var provisionResp = await client.PostAsJsonAsync("/api/setup/provision",
            new { appName = "Northwind Demo", slug = appSlug });
        Assert.True(provisionResp.IsSuccessStatusCode,
            $"provision returned {provisionResp.StatusCode}: {await provisionResp.Content.ReadAsStringAsync()}");
        var provisionJson = await provisionResp.Content.ReadFromJsonAsync<JsonElement>();
        var appDocId = provisionJson.GetProperty("id").GetString();
        var slug     = provisionJson.GetProperty("slug").GetString();
        Assert.Equal("northwind-demo", slug);
        Assert.False(string.IsNullOrEmpty(appDocId));

        var cdcWsClient = factory.Server.CreateWebSocketClient();
        cdcWsClient.ConfigureRequest = request =>
            request.Headers[Raven.Quill.Auth.ApiKeyAuthenticationHandler.HeaderName] =
                ApplianceWebApplicationFactory.TestApiKey;
        var cdcWsUri = new Uri(factory.Server.BaseAddress, $"api/apps/{slug}/cdc/progress");
        using (var cdcCts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        using (var cdcWs = await cdcWsClient.ConnectAsync(cdcWsUri, cdcCts.Token))
        {
            var frameBuffer = new byte[16 * 1024];
            var frame = await cdcWs.ReceiveAsync(new ArraySegment<byte>(frameBuffer), cdcCts.Token);
            Assert.NotEqual(WebSocketMessageType.Close, frame.MessageType);
        }

        await WaitForPerAppCdcInitialLoadAsync(store, perAppDatabase: slug!, configName: $"{slug}-cdc", timeoutMs: 120_000);
        var ordersCount = await WaitForPerAppDocumentCountAsync(store, perAppDatabase: slug!, collectionName: "Orders", expectedCount: 800, timeoutMs: 30_000);
        Assert.True(ordersCount >= 800,
            $"expected >=800 Orders after initial load, got {ordersCount}");

        var perf = await WaitForPopulatedCdcPerformanceAsync(client, slug!, timeoutMs: 30_000);
        Assert.True(perf.GetProperty("enabled").GetBoolean(),
            "cdc performance should report enabled after provisioning");
        Assert.True(perf.GetProperty("recentBatches").GetArrayLength() > 0,
            "expected at least one recent CDC batch after the initial load");
        Assert.True(perf.GetProperty("recentWrites").GetInt64() > 0,
            $"expected recentWrites>0 from the mirrored dump; got {perf.GetProperty("recentWrites").GetInt64()}");
        Assert.NotEqual("error", perf.GetProperty("status").GetString());

        var csResp = await client.PostAsJsonAsync($"/api/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                openAiSettings = new { apiKey = openAiKey, endpoint = "https://api.openai.com/", model = "gpt-4.1-mini" }
            });
        Assert.True(csResp.IsSuccessStatusCode,
            $"ai connection-string returned {csResp.StatusCode}: {await csResp.Content.ReadAsStringAsync()}");

        // sampleObject keys are PascalCase so the streamed "Reply" field (resolved at runtime from its first property) matches the model's output.
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

        var channelResp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = new[] { "http://localhost" } });
        Assert.True(channelResp.IsSuccessStatusCode,
            $"channel returned {channelResp.StatusCode}: {await channelResp.Content.ReadAsStringAsync()}");
        var channelJson = await channelResp.Content.ReadFromJsonAsync<JsonElement>();
        var channelId = channelJson.GetProperty("channelId").GetString();
        Assert.False(string.IsNullOrEmpty(channelId));

        var linkResp = await client.PostAsJsonAsync($"/api/apps/{slug}/embed-links",
            new { channelId, ttlSeconds = 3600, maxInvocations = 10 });
        Assert.True(linkResp.IsSuccessStatusCode,
            $"mint embed-link returned {linkResp.StatusCode}: {await linkResp.Content.ReadAsStringAsync()}");
        var linkJson = await linkResp.Content.ReadFromJsonAsync<JsonElement>();
        var token = linkJson.GetProperty("token").GetString();
        Assert.Matches("^[a-f0-9]{32}$", token!);
        Assert.EndsWith($"/apps/{slug}/embed/{token}", linkJson.GetProperty("url").GetString());

        var embedResp = await client.GetAsync($"/apps/{slug}/embed/{token}");
        Assert.Equal(HttpStatusCode.OK, embedResp.StatusCode);
        Assert.Contains("text/html", embedResp.Content.Headers.ContentType?.ToString() ?? "");
        var embedHtml = await embedResp.Content.ReadAsStringAsync();
        Assert.False(string.IsNullOrWhiteSpace(embedHtml), "embed page body was empty");
        Assert.Contains(token!, embedHtml);

        using var chatCts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var chatReq = new HttpRequestMessage(HttpMethod.Post, $"/apps/{slug}/embed/{token}/chat")
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

        using var chat2Cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var chat2Req = new HttpRequestMessage(HttpMethod.Post, $"/apps/{slug}/embed/{token}/chat")
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
    }

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

    private static async Task<JsonElement> WaitForPopulatedCdcPerformanceAsync(HttpClient client, string slug, int timeoutMs)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        JsonElement perf;
        do
        {
            perf = await client.GetFromJsonAsync<JsonElement>($"/api/apps/{slug}/cdc/performance");
            if (perf.GetProperty("recentBatches").GetArrayLength() > 0 &&
                perf.GetProperty("recentWrites").GetInt64() > 0)
                return perf;
            await Task.Delay(500);
        } while (sw.ElapsedMilliseconds < timeoutMs);

        return perf;
    }

    private async Task WaitForPerAppCdcInitialLoadAsync(IDocumentStore store, string perAppDatabase, string configName, int timeoutMs)
    {
        var db = await Databases.GetDocumentDatabaseInstanceFor(store, perAppDatabase);
        var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
        if (process == null)
            throw new InvalidOperationException($"CDC Sink process '{configName}' not found on '{perAppDatabase}'");

        var completed = await Task.WhenAny(process.InitialLoadCompleted, Task.Delay(timeoutMs));
        if (completed != process.InitialLoadCompleted)
            throw new TimeoutException($"CDC Sink '{configName}' on '{perAppDatabase}' initial load did not complete within {timeoutMs}ms");

        await process.InitialLoadCompleted;
    }

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
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"BootstrapState did not reach '{expected}' within {timeoutMs}ms.");
    }
}
