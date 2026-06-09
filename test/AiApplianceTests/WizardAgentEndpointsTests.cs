using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// W7 + W8 endpoint coverage. W7 = POST /api/apps/{slug}/setup/agent
/// (provision agent against the per-app DB). W8 = POST /api/apps/{slug}/setup/channel
/// (register a channel-instance doc in the app DB per design §3.4). Both are
/// exercised end-to-end in ApplianceFullFlowTests T11/T12; this suite is the
/// focused unit coverage.
///
/// Test isolation: each test uses its own GetDocumentStore() — the store's
/// auto-named DB IS the appliance's config DB for that test. Per-app DBs get
/// a Guid suffix so parallel / serial test runs don't step on each other.
public class WizardAgentEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // ---- W7 ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/setup/agent",
            new { name = "Support Bot", systemPrompt = "You help.", connectionStringName = "demo-llm" });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_agentId_and_references_operator_provided_cs()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Operator creates the AI connection string first (the dashboard's
        // "pick existing OR add new" step). The agent provisioning then
        // references that CS by name — same wire pattern the dashboard will
        // use.
        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                name = "Support Bot",
                systemPrompt = "You are a helpful support agent for the Northwind store.",
                connectionStringName = "demo-llm",
            });

        Assert.True(resp.IsSuccessStatusCode,
            $"agent returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var agentId = json.GetProperty("agentId").GetString();
        Assert.False(string.IsNullOrEmpty(agentId), $"agentId was empty: {json}");

        // The strong assertion: the registered agent references the operator's
        // CS, not any appliance-side default. Without this check the test
        // passes vacuously regardless of what CS the agent ended up using.
        var agents = await store.Maintenance.ForDatabase(perAppDb)
            .SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal("demo-llm", agent.ConnectionStringName);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_accepts_explicit_null_parameters()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        // Over-posting "parameters": null binds a null Parameters list. Provisioning
        // serializes the agent through conventions (AddOrUpdateAiAgentOperation ->
        // DefaultConverter.ToBlittable), which tolerates the null, so the POST must succeed.
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                name = "Support Bot",
                systemPrompt = "You help.",
                connectionStringName = "demo-llm",
                parameters = (object?)null,
            });

        Assert.True(resp.IsSuccessStatusCode,
            $"agent returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");

        // Read-back must also work: GetAiAgentsOperation deserializes the stored agent
        // (null Parameters included) through conventions without error.
        var agents = await store.Maintenance.ForDatabase(perAppDb).SendAsync(new GetAiAgentsOperation());
        Assert.Single(agents.AiAgents);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_400_for_missing_connection_string_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { name = "Support Bot", systemPrompt = "You help." });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_400_for_unknown_connection_string_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // No POST to /ai/connection-strings — the named CS doesn't exist.
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { name = "Support Bot", systemPrompt = "You help.", connectionStringName = "ghost-llm" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_400_when_server_rejects_configuration()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        // A tool-query name with a space passes the appliance's intake gates (which
        // only validate Actions/SubAgents), but the server's ValidateConfiguration
        // rejects it (ToolNameChecker = ^[a-zA-Z0-9_-]+$). Operator input that fails
        // server-side validation must surface as a 400, not a 500 from the
        // bubbled RavenException.
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                name = "Support Bot",
                systemPrompt = "You help.",
                connectionStringName = "demo-llm",
                queries = new[] { new { name = "bad name", description = "lookup", query = "from Orders" } },
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_persists_request_name_prompt_and_queries()
    {
        // The core behavioural change: the agent's brain (name, system prompt,
        // RQL tool queries) comes from the request and is persisted verbatim to
        // the per-app database — there is no hardcoded placeholder agent.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                identifier = "support-bot",
                name = "Support Bot",
                systemPrompt = "You are a Northwind support agent. Answer using the orders data.",
                connectionStringName = "demo-llm",
                queries = new[]
                {
                    new
                    {
                        name = "findOrdersByCustomer",
                        description = "Find orders for a given customer id.",
                        query = "from Orders where Customer = $customerId",
                        // A parameterized tool query must declare its parameters
                        // shape; RavenDB rejects the agent otherwise.
                        parametersSampleObject = """{"customerId":"ALFKI"}""",
                    },
                },
            });

        Assert.True(resp.IsSuccessStatusCode,
            $"agent returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");

        var agents = await store.Maintenance.ForDatabase(perAppDb)
            .SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal("support-bot", agent.Identifier);
        Assert.Equal("Support Bot", agent.Name);
        Assert.Equal("You are a Northwind support agent. Answer using the orders data.", agent.SystemPrompt);
        var query = Assert.Single(agent.Queries);
        Assert.Equal("findOrdersByCustomer", query.Name);
        Assert.Equal("from Orders where Customer = $customerId", query.Query);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_400_for_missing_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Create the CS so the only reason to 400 is the missing name (not an
        // unresolved connection string).
        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { systemPrompt = "You help.", connectionStringName = "demo-llm" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_400_for_missing_system_prompt()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Create the CS so the only reason to 400 is the missing systemPrompt.
        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { name = "Support Bot", connectionStringName = "demo-llm" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_creates_agent_when_sample_object_omitted()
    {
        // RavenDB's AddOrUpdateAiAgentOperation requires either OutputSchema or
        // SampleObject. The endpoint defaults SampleObject when both are omitted
        // so the minimal frontend body (name + prompt + CS) still provisions.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { name = "Support Bot", systemPrompt = "You help.", connectionStringName = "demo-llm" });

        Assert.True(resp.IsSuccessStatusCode,
            $"agent returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");

        var agents = await store.Maintenance.ForDatabase(perAppDb)
            .SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);

        // The omitted SampleObject must have been defaulted to DefaultSampleObject ({"reply":""}).
        // Parse rather than string-compare so server-side JSON normalization (whitespace) can't
        // make the assertion brittle.
        using var sample = JsonDocument.Parse(agent.SampleObject);
        Assert.True(sample.RootElement.TryGetProperty("reply", out var reply));
        Assert.Equal("", reply.GetString());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_rejects_actions()
    {
        // Demo subset: model-side Actions are not smoke-tested; reject at intake
        // rather than silently provisioning an agent that behaves unexpectedly.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                name = "Support Bot",
                systemPrompt = "You help.",
                connectionStringName = "demo-llm",
                actions = new[] { new { name = "sendEmail", description = "send an email" } },
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_rejects_sub_agents()
    {
        // Demo subset: server-side SubAgents aren't smoke-tested; reject at intake
        // (symmetry with Agent_endpoint_rejects_actions).
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                name = "Support Bot",
                systemPrompt = "You help.",
                connectionStringName = "demo-llm",
                subAgents = new[] { new { name = "helper", systemPrompt = "assist" } },
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    // ---- W8 ----

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("demo-agent")]   // valid agentId — still 404 because slug is unknown
    [InlineData("ghost-agent")]  // invalid agentId — L1: must NOT leak via differential 400 vs 404
    public async Task Channel_endpoint_returns_404_for_unknown_slug(string agentId)
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = new[] { "http://localhost" } });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_widgetId_for_known_app()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } });

        Assert.True(resp.IsSuccessStatusCode,
            $"channel returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var widgetId = json.GetProperty("widgetId").GetString();
        Assert.False(string.IsNullOrEmpty(widgetId), $"widgetId was empty: {json}");
        Assert.StartsWith("wgt_", widgetId);
        // H1: 128-bit random (Base64url-encoded) → exactly 22 chars after the
        // padding is trimmed (16 bytes × 4/3 = 21.33 → 22 chars without padding),
        // plus the 'wgt_' prefix (4) = 26 total. Earlier 32-bit form produced 12;
        // assert against the actual lower bound, not a loose >=24.
        Assert.True(widgetId.Length >= 26,
            $"widgetId length {widgetId.Length} is below the 128-bit-entropy floor (expected ≥26 incl. 'wgt_' prefix): '{widgetId}'");
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_501_for_recognized_but_unimplemented_type()
    {
        // whatsapp/telegram are valid ChannelType values but not yet implemented
        // (RavenDB-26631), so the per-type switch dispatches to a 501 stub rather
        // than provisioning.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "whatsapp", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });

        Assert.Equal(HttpStatusCode.NotImplemented, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_unknown_type()
    {
        // An unrecognized type string can't bind to the ChannelType enum, so the
        // request fails model binding -> 400 (before the handler runs).
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "carrier-pigeon", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_400_when_allowedOrigins_missing()
    {
        // "Embeddable from anywhere" must be an explicit opt-in ([]). Omitting
        // the property is a 400, not a silently-open embed.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_400_when_type_missing()
    {
        // A missing 'type' binds the now-nullable enum to null; the handler must
        // reject it with 400 rather than silently defaulting to IFrame.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_same_widgetId_under_concurrent_calls()
    {
        // C2 from Copilot review #4362803113: the previous query-then-put
        // idempotency was only race-safe for *sequential* retries (an
        // index-staleness scenario the M3 WaitForNonStaleResults handled).
        // Two concurrent POSTs with the same (slug, type, agentId) could
        // both miss the query and both store a fresh channels/{widgetId}
        // — different widgetIds, identical binding tuple, duplicate
        // channels routing to the same agent. The fix uses an atomic
        // guard on a deterministic channel-bindings/{slug}/{type}/{agentId}
        // doc id (Shape B from the planning doc): the cluster-wide tx
        // serializes concurrent writers through Raft, the loser catches
        // ClusterTransactionConcurrencyException, reads the winner's
        // binding, and returns the same widgetId.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);
        var body = new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } };

        var tasks = Enumerable.Range(0, 10)
            .Select(_ => client.PostAsJsonAsync("/api/apps/my-app/setup/channel", body))
            .ToArray();
        var responses = await Task.WhenAll(tasks);

        var widgetIds = new HashSet<string>();
        var freshCreates = 0;
        foreach (var resp in responses)
        {
            Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
            var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
            widgetIds.Add(json.GetProperty("widgetId").GetString()!);
            if (json.GetProperty("existing").GetBoolean() == false)
                freshCreates++;
        }

        Assert.Single(widgetIds);
        // Exactly one request actually created the channel; the rest surfaced
        // existing=true (fast path or race-loser).
        Assert.Equal(1, freshCreates);

        // Query<> counts are index-backed and can read stale right after the
        // concurrent writes — wait for indexing so the asserts can't flake.
        Indexes.WaitForIndexing(store, perAppDb);

        using var session = store.OpenAsyncSession(perAppDb);
        Assert.Equal(1, await session.Query<Channel>().CountAsync());
        Assert.Equal(1, await session.Query<ChannelBinding>().CountAsync());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_reprovision_with_different_payload_returns_existing_and_keeps_stored_values()
    {
        // M2 (review 2026-06-04): provision is create-only. Re-running it for an
        // existing (slug, type, agent) with different origins/displayName returns
        // the existing widgetId, surfaces existing=true, and applies nothing —
        // edits go through PUT /channels/{id}.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var first = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" }, displayName = "Original" });
        Assert.True(first.IsSuccessStatusCode, await first.Content.ReadAsStringAsync());
        var firstJson = await first.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(firstJson.GetProperty("existing").GetBoolean());
        var widgetId = firstJson.GetProperty("widgetId").GetString();

        var second = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "https://changed.example.com" }, displayName = "Changed" });
        Assert.True(second.IsSuccessStatusCode, await second.Content.ReadAsStringAsync());
        var secondJson = await second.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(widgetId, secondJson.GetProperty("widgetId").GetString());
        Assert.True(secondJson.GetProperty("existing").GetBoolean());

        // The differing payload was discarded, not applied.
        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.LoadAsync<Channel>($"channels/{widgetId}");
        Assert.NotNull(channel);
        Assert.Equal("Original", channel!.DisplayName);
        Assert.Equal(new[] { "http://localhost" }, channel.AllowedOrigins);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_same_widgetId_for_repeated_calls()
    {
        // M3: idempotency. Two POSTs with the same body must return the same
        // widgetId — operator double-click / client retry should not create
        // orphan channel docs.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var body = new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } };

        var resp1 = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel", body);
        Assert.True(resp1.IsSuccessStatusCode, await resp1.Content.ReadAsStringAsync());
        var widgetId1 = (await resp1.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("widgetId").GetString();

        // Let the in-process auto-index catch up so the idempotency query on
        // the server side sees the freshly-stored channel. Without this the
        // second POST may read stale and create a second doc.
        Indexes.WaitForIndexing(store, perAppDb);

        var resp2 = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel", body);
        Assert.True(resp2.IsSuccessStatusCode, await resp2.Content.ReadAsStringAsync());
        var widgetId2 = (await resp2.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("widgetId").GetString();

        Assert.Equal(widgetId1, widgetId2);

        // And only one channel doc in the per-app DB.
        using var session = store.OpenAsyncSession(perAppDb);
        var count = await session.Query<Channel>().CountAsync();
        Assert.Equal(1, count);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("https://example.com/",  "https://example.com")]   // C2 (review 4365219160): trailing slash normalized away
    [InlineData("http://example.com:8080/", "http://example.com:8080")] // explicit port retained, slash dropped
    [InlineData("https://example.com",   "https://example.com")]   // already canonical → unchanged
    public async Task Channel_endpoint_normalizes_origin_on_persist(string supplied, string expected)
    {
        // C2 (Copilot review 4365219160): an origin like `https://example.com/`
        // passes the earlier path-rejection check (AbsolutePath = "/" is the
        // canonical form for origin-only URLs) but the BROWSER `Origin` header
        // is scheme+host[:port] only — no trailing slash. If we persisted the
        // raw `supplied` value, runtime CORS-style matching on the future
        // /embed/{widgetId} page would silently fail. Normalize on intake.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { supplied } });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        Indexes.WaitForIndexing(store, perAppDb);

        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Single(channel.AllowedOrigins);
        Assert.Equal(expected, channel.AllowedOrigins[0]);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("http://example.com/app")]             // C3: path
    [InlineData("https://example.com/foo/bar")]        // C3: nested path
    [InlineData("https://example.com/?q=1")]           // C3: query
    [InlineData("https://example.com/#frag")]          // C3: fragment
    public async Task Channel_endpoint_rejects_origin_with_path_query_or_fragment(string badOrigin)
    {
        // C3 from Copilot review #4362803113: allowedOrigins entries must be
        // origins (scheme + host + optional port) — what the browser sends in
        // the Origin header. Paths/queries/fragments slip through Uri.TryCreate
        // + scheme check but don't match the runtime header value, causing
        // silent CORS misconfig at the future /embed/{widgetId} page.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { badOrigin } });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("*")]                                  // M2: wildcard widens trust
    [InlineData("example.com")]                        // M2: scheme-less
    [InlineData("ftp://example.com")]                  // M2: non-http(s) scheme
    [InlineData("")]                                   // M2: empty entry
    [InlineData("https://user:pass@example.com")]      // M2: userinfo isn't part of an Origin
    public async Task Channel_endpoint_rejects_invalid_allowed_origin(string badOrigin)
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { badOrigin } });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_too_many_allowed_origins()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        // M2: 33 entries exceeds the 32 cap.
        var tooMany = Enumerable.Range(0, 33)
            .Select(i => $"http://example{i}.com")
            .ToArray();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = tooMany });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("a\u0007b")]                          // M4: BEL control char (escape-sequence so source is all printable)
    [InlineData("name\twith\ttabs")]                    // M4: tab is also a control char
    public async Task Channel_endpoint_rejects_invalid_display_name(string badName)
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new
            {
                type = "iframe",
                agentId = "demo-agent",
                allowedOrigins = new[] { "http://localhost" },
                displayName = badName,
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_too_long_display_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new
            {
                type = "iframe",
                agentId = "demo-agent",
                allowedOrigins = new[] { "http://localhost" },
                displayName = new string('x', 201),  // M4: 200-char cap
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_carries_its_binding_id()
    {
        // The reverse direction (channel -> binding) is not derivable from
        // in-doc fields: rebuilding the binding id needs the app slug, which
        // lives on the App doc in the config DB, not in the per-app DB. Store
        // BindingId on the Channel so the future delete-channel / Channels &
        // Adapters tab can navigate back without a cross-DB lookup.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        Indexes.WaitForIndexing(store, perAppDb);

        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Equal("channel-bindings/my-app/IFrame/demo-agent", channel.BindingId);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_doc_lands_in_Channels_collection()
    {
        // C4 from Copilot review #4362803113: the design §3.4 spec says the
        // collection is "Channels", but RavenDB derives the collection name
        // from the CLR type — so a class named ChannelInstance lands in
        // "ChannelInstances". Renaming the class to Channel makes the
        // persisted collection match the spec without overriding conventions.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        Indexes.WaitForIndexing(store, perAppDb);

        using var session = store.OpenAsyncSession(perAppDb);
        var ch = await session.Query<Channel>().FirstAsync();
        var collection = session.Advanced.GetMetadataFor(ch)["@collection"]!.ToString();
        Assert.Equal("Channels", collection);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_persists_canonical_agent_id_casing()
    {
        // L3: agent resolution is case-insensitive, but the persisted
        // Channel.AgentId must adopt the agent's canonical (lowercase)
        // identifier — otherwise later case-sensitive queries (e.g. M3's
        // idempotency lookup) break across re-runs that mix casings.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        await SeedMockAgentAsync(client);

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "Demo-Agent", allowedOrigins = new[] { "http://localhost" } });

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        Indexes.WaitForIndexing(store, perAppDb);

        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Equal("demo-agent", channel.AgentId);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_binds_to_an_operator_defined_non_demo_agent()
    {
        // The ticket's core acceptance: an operator-provisioned agent with a
        // custom identifier + system prompt + RQL query is runnable — a channel
        // binds to it (resolved from the per-app DB), not just a placeholder.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var csResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var agentResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new
            {
                identifier = "order-support",
                name = "Order Support",
                systemPrompt = "You are the Northwind order-support agent.",
                connectionStringName = "demo-llm",
                queries = new[]
                {
                    new
                    {
                        name = "findOrdersByCustomer",
                        description = "Find orders for a given customer id.",
                        query = "from Orders where Customer = $customerId",
                        parametersSampleObject = """{"customerId":"ALFKI"}""",
                    },
                },
            });
        Assert.True(agentResp.IsSuccessStatusCode,
            $"agent returned {agentResp.StatusCode}: {await agentResp.Content.ReadAsStringAsync()}");

        var channelResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "order-support", allowedOrigins = new[] { "http://localhost" } });
        Assert.True(channelResp.IsSuccessStatusCode,
            $"channel returned {channelResp.StatusCode}: {await channelResp.Content.ReadAsStringAsync()}");

        Indexes.WaitForIndexing(store, perAppDb);

        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.Query<Channel>().FirstAsync();
        Assert.Equal("order-support", channel.AgentId);

        // The bound agent carries ITS prompt + query, not a placeholder.
        var agents = await store.Maintenance.ForDatabase(perAppDb).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal("order-support", agent.Identifier);
        Assert.Equal("You are the Northwind order-support agent.", agent.SystemPrompt);
        Assert.Equal("findOrdersByCustomer", Assert.Single(agent.Queries).Name);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_400_for_unknown_agent()
    {
        // Provision rejects an agentId that doesn't exist in the per-app DB:
        // resolution goes through the database, not a compile-time registry.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "ghost-agent", allowedOrigins = new[] { "http://localhost" } });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    // ---- helpers ----

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts =>
            {
                // Pin the appliance's "config DB" to the test store's own
                // (auto-named, unique-per-test) database — so parallel /
                // serial tests against the shared RavenDB server don't
                // step on each other's App docs.
                opts.ConfigDatabase = store.Database;
            });

    /// <summary>
    /// Creates a uniquely-named per-app database on the test store and
    /// returns its name plus a cleanup handle. Tests <c>using</c> the
    /// handle so the database drops at test end — otherwise these DBs
    /// accumulate on the (shared) test server across runs and slow the
    /// dev loop down (Copilot review #4361946757 C6).
    /// </summary>
    private async Task<(string Name, IDisposable Cleanup)> CreatePerAppDatabaseAsync(IDocumentStore store)
    {
        var name = "per-app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(name)));
        return (name, Databases.EnsureDatabaseDeletion(name, store));
    }

    private static async Task SeedAppAsync(IDocumentStore store, string slug, string database)
    {
        // The "config DB" is store.Database (PostConfigure'd above); seeding
        // the App doc there means the wizard endpoints find it.
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new App
        {
            Slug = slug,
            AppName = slug,
            Database = database,
            CdcTaskName = $"{slug}-cdc",
            CreatedAt = DateTime.UtcNow,
        }, id: $"apps/{slug}");
        await session.SaveChangesAsync();
    }

    /// <summary>
    /// Seeds a mock connection string + agent in the app's per-app DB so the
    /// channel endpoints (which resolve the agent from the database, not a
    /// compile-time registry) have a real agent to bind to. The Ollama CS is
    /// stored config only — it is never dialed.
    /// </summary>
    private static async Task SeedMockAgentAsync(HttpClient client, string slug = "my-app", string agentId = "demo-agent")
    {
        var csResp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode,
            $"seed connection-string returned {csResp.StatusCode}: {await csResp.Content.ReadAsStringAsync()}");

        var agentResp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/setup/agent",
            new
            {
                identifier = agentId,
                name = "Demo Agent",
                systemPrompt = "You are a placeholder demo agent.",
                connectionStringName = "demo-llm",
            });
        Assert.True(agentResp.IsSuccessStatusCode,
            $"seed agent returned {agentResp.StatusCode}: {await agentResp.Content.ReadAsStringAsync()}");
    }
}
