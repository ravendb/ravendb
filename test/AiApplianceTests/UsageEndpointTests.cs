using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.AiAppliance.Auth;
using Raven.AiAppliance.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for the global <c>GET /api/usage</c> endpoint — the backend behind the
/// prototype's <c>api.getUsage()</c>. Contract: <c>UsagePoint[]</c> of
/// <c>{ timestamp, conversations, messages, tokens }</c>, one contiguous point per
/// bucket over the window (<c>time</c> = <c>Last24h</c> hourly / <c>Last7d</c> /
/// <c>Last30d</c> daily), summed across every app DB (or scoped via <c>app</c>).
/// <c>messages</c> = user turns in the bucket; <c>tokens</c> = summed token usage.
/// Aggregated from the per-app <see cref="ConversationMetricsIndex"/>; no live LLM.
/// </summary>
public class UsageEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_returns_24_hourly_points_summing_invocations_and_tokens()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", now.AddHours(-1), messages: 3, tokens: 100);
        await SeedConversationAsync(store, perAppDb, "chats/b", "demo", now.AddHours(-1), messages: 5, tokens: 250);
        // Outside the 24h window — must not appear in the series.
        await SeedConversationAsync(store, perAppDb, "chats/old", "demo", now.AddDays(-20), messages: 9, tokens: 999);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/usage");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var points = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, points.ValueKind);
        // Contiguous hourly series over the last 24h.
        Assert.Equal(24, points.GetArrayLength());

        long totalInvocations = 0, totalTokens = 0;
        foreach (var p in points.EnumerateArray())
        {
            Assert.True(p.TryGetProperty("timestamp", out _), "point is missing 'timestamp'");
            totalInvocations += p.GetProperty("messages").GetInt64();
            totalTokens += p.GetProperty("tokens").GetInt64();
        }

        // Only the two last-hour conversations fall in the window.
        Assert.Equal(8, totalInvocations);  // 3 + 5 messages
        Assert.Equal(350, totalTokens);     // 100 + 250 tokens
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_sums_across_app_databases()
    {
        var store = GetDocumentStore();
        var (appDb1, cleanup1) = await CreatePerAppDatabaseAsync(store);
        using var _db1 = cleanup1;
        var (appDb2, cleanup2) = await CreatePerAppDatabaseAsync(store);
        using var _db2 = cleanup2;
        await SeedAppAsync(store, slug: "app-one", database: appDb1);
        await SeedAppAsync(store, slug: "app-two", database: appDb2);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: appDb1);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: appDb2);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, appDb1, "chats/a", "demo", now.AddHours(-1), messages: 2, tokens: 50);
        await SeedConversationAsync(store, appDb2, "chats/b", "demo", now.AddHours(-1), messages: 4, tokens: 70);
        await Indexes.WaitForIndexingAsync(store, appDb1);
        await Indexes.WaitForIndexingAsync(store, appDb2);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/usage");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var points = await resp.Content.ReadFromJsonAsync<JsonElement>();
        long totalInvocations = 0, totalTokens = 0;
        foreach (var p in points.EnumerateArray())
        {
            totalInvocations += p.GetProperty("messages").GetInt64();
            totalTokens += p.GetProperty("tokens").GetInt64();
        }

        // Both apps' last-hour conversations are summed into the one global series.
        Assert.Equal(6, totalInvocations);  // 2 + 4 messages
        Assert.Equal(120, totalTokens);     // 50 + 70 tokens
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_invocations_count_user_messages_only()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        // Real-shaped doc: one user message + system/assistant/tool scaffolding.
        await SeedRealisticConversationAsync(store, perAppDb, "chats/r", "demo", DateTime.UtcNow.AddHours(-1));
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var points = await client.GetFromJsonAsync<JsonElement>("/api/usage");
        long invocations = 0;
        foreach (var p in points.EnumerateArray())
            invocations += p.GetProperty("messages").GetInt64();
        Assert.Equal(1, invocations);  // only the user message counts (not system/assistant/tool)
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_degrades_to_empty_when_index_not_deployed()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        // The metrics index is intentionally NOT deployed on this app DB.

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/usage");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());  // not 500
        var points = await resp.Content.ReadFromJsonAsync<JsonElement>();
        long invocations = 0;
        foreach (var p in points.EnumerateArray())
            invocations += p.GetProperty("messages").GetInt64();
        Assert.Equal(0, invocations);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_requires_authentication()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync("/api/usage");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_skips_unhealthy_app_db_and_returns_partial()
    {
        var store = GetDocumentStore();
        var (healthyDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "healthy", database: healthyDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: healthyDb);
        // A registered app whose database was never created — the fan-out must skip it, not 500.
        await SeedAppAsync(store, slug: "broken", database: "missing-" + Guid.NewGuid().ToString("N"));

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, healthyDb, "chats/a", "support", now.AddHours(-1), messages: 3, tokens: 120);
        await Indexes.WaitForIndexingAsync(store, healthyDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // I2: one bad tenant DB must not 500 the whole endpoint — healthy data still returns.
        var usageResp = await client.GetAsync("/api/usage");
        Assert.Equal(HttpStatusCode.OK, usageResp.StatusCode);
        var points = await usageResp.Content.ReadFromJsonAsync<JsonElement>();
        long invocations = 0, tokens = 0;
        foreach (var p in points.EnumerateArray())
        {
            invocations += p.GetProperty("messages").GetInt64();
            tokens += p.GetProperty("tokens").GetInt64();
        }
        Assert.Equal(3, invocations);
        Assert.Equal(120, tokens);

        // The global dashboard fan-out is resilient too.
        var dashResp = await client.GetAsync("/api/dashboard");
        Assert.Equal(HttpStatusCode.OK, dashResp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_window_controls_granularity_and_point_count()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now.AddDays(-2), messages: 2, tokens: 100);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Last24h → 24 hourly, Last7d → 7 daily, Last30d → 30 daily (contiguous, zero-filled).
        Assert.Equal(24, (await client.GetFromJsonAsync<JsonElement>("/api/usage?time=Last24h")).GetArrayLength());
        Assert.Equal(7, (await client.GetFromJsonAsync<JsonElement>("/api/usage?time=Last7d")).GetArrayLength());
        var d30 = await client.GetFromJsonAsync<JsonElement>("/api/usage?time=Last30d");
        Assert.Equal(30, d30.GetArrayLength());

        // The 2-days-ago conversation lands in the 30d window with all three metrics.
        long conv = 0, msg = 0, tok = 0;
        foreach (var p in d30.EnumerateArray())
        {
            conv += p.GetProperty("conversations").GetInt64();
            msg += p.GetProperty("messages").GetInt64();
            tok += p.GetProperty("tokens").GetInt64();
        }
        Assert.Equal(1, conv);
        Assert.Equal(2, msg);
        Assert.Equal(100, tok);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_app_param_scopes_to_a_single_app()
    {
        var store = GetDocumentStore();
        var (db1, c1) = await CreatePerAppDatabaseAsync(store);
        using var _1 = c1;
        var (db2, c2) = await CreatePerAppDatabaseAsync(store);
        using var _2 = c2;
        await SeedAppAsync(store, slug: "app-one", database: db1);
        await SeedAppAsync(store, slug: "app-two", database: db2);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: db1);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: db2);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, db1, "chats/a", "x", now.AddHours(-1), messages: 2, tokens: 50);
        await SeedConversationAsync(store, db2, "chats/b", "y", now.AddHours(-1), messages: 4, tokens: 70);
        await Indexes.WaitForIndexingAsync(store, db1);
        await Indexes.WaitForIndexingAsync(store, db2);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        static long Sum(JsonElement points, string field)
        {
            long s = 0;
            foreach (var p in points.EnumerateArray()) s += p.GetProperty(field).GetInt64();
            return s;
        }

        var appOne = await client.GetFromJsonAsync<JsonElement>("/api/usage?time=Last24h&app=app-one");
        Assert.Equal(2, Sum(appOne, "messages"));   // only app-one's data
        Assert.Equal(50, Sum(appOne, "tokens"));

        var all = await client.GetFromJsonAsync<JsonElement>("/api/usage?time=Last24h");
        Assert.Equal(6, Sum(all, "messages"));       // both apps summed

        var appTwo = await client.GetFromJsonAsync<JsonElement>("/api/usage?time=Last24h&app=app-two");
        // Writes are a deterministic per-app mock (RavenDB-26780): the global series must
        // equal the sum of the per-app series, and be populated (> 0).
        Assert.Equal(Sum(appOne, "writes") + Sum(appTwo, "writes"), Sum(all, "writes"));
        Assert.True(Sum(all, "writes") > 0);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_tolerates_conversation_doc_missing_usage_and_messages()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now.AddHours(-1), messages: 2, tokens: 100);
        // A @conversations doc with neither TotalUsage nor Messages — the index reads those via
        // dynamic member access (DynamicNullObject, not NRE), so it must contribute 0, not error.
        await PutConversationDocAsync(store, perAppDb, "chats/min",
            new { Agent = "support", CreatedAt = now.AddHours(-1), LastMessageAt = now.AddHours(-1) });
        await Indexes.WaitForIndexingAsync(store, perAppDb);   // throws if the index errored on the partial doc

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var points = await client.GetFromJsonAsync<JsonElement>("/api/usage");
        long conv = 0, msg = 0, tok = 0;
        foreach (var p in points.EnumerateArray())
        {
            conv += p.GetProperty("conversations").GetInt64();
            msg += p.GetProperty("messages").GetInt64();
            tok += p.GetProperty("tokens").GetInt64();
        }
        Assert.Equal(2, conv);    // both docs count as conversations
        Assert.Equal(2, msg);     // only the well-formed doc's user messages
        Assert.Equal(100, tok);   // only the well-formed doc's tokens
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Usage_rejects_invalid_time()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/usage?time=5d");
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }
}
