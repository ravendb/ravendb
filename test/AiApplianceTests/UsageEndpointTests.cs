using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.AiAppliance.Auth;
using Raven.AiAppliance.Metrics;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for the global <c>GET /api/usage</c> endpoint — the backend behind the
/// prototype's <c>api.getUsage()</c>. Contract (mock-api.ts): <c>UsagePoint[]</c> of
/// <c>{ timestamp, invocations, tokens }</c>, one contiguous point per hour over the
/// last 24h, summed across every app DB. <c>invocations</c> = agent turns (messages)
/// in the hour; <c>tokens</c> = summed token usage. Aggregated from the per-app
/// <see cref="ConversationMetricsIndex"/>; no live LLM.
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
            totalInvocations += p.GetProperty("invocations").GetInt64();
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
            totalInvocations += p.GetProperty("invocations").GetInt64();
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
            invocations += p.GetProperty("invocations").GetInt64();
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
            invocations += p.GetProperty("invocations").GetInt64();
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
            invocations += p.GetProperty("invocations").GetInt64();
            tokens += p.GetProperty("tokens").GetInt64();
        }
        Assert.Equal(3, invocations);
        Assert.Equal(120, tokens);

        // The global dashboard fan-out is resilient too.
        var dashResp = await client.GetAsync("/api/dashboard");
        Assert.Equal(HttpStatusCode.OK, dashResp.StatusCode);
    }
}
