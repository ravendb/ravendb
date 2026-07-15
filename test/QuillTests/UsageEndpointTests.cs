using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Quill.Auth;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the global <c>GET /api/usage</c> endpoint — the backend behind the
/// prototype's <c>api.getUsage()</c>. Contract: <c>UsagePoint[]</c> of
/// <c>{ timestamp, conversations, messages, tokens }</c>, one contiguous point per calendar
/// bucket. The granularity follows which query fields are set: <c>year</c> → 12 months of that
/// year; <c>year</c>+<c>month</c> → every day of that month; <c>year</c>+<c>month</c>+<c>day</c>
/// → the 24 hours of that day. Summed across every app DB (or scoped via <c>app</c>).
/// Aggregated from the per-app <see cref="ConversationMetricsIndex"/>; no live LLM.
/// </summary>
public class UsageEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    // ?year={y}[&month={m}][&day={d}][&app=]. Granularity = how many of month/day are set.
    private static string Q(int year, int? month = null, int? day = null, string? app = null)
    {
        var q = $"year={year}";
        if (month is not null) q += $"&month={month}";
        if (day is not null) q += $"&day={day}";
        if (app is not null) q += $"&app={app}";
        return q;
    }

    // A timestamp earlier today (00:00 + up to 1h) so seeds land in today's hourly buckets
    // regardless of the hour the test runs.
    private static DateTime EarlierToday(DateTime now) =>
        new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, DateTimeKind.Utc).AddHours(Math.Min(now.Hour, 1));

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_returns_24_hourly_points_summing_invocations_and_tokens()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", earlierToday, messages: 3, tokens: 100);
        await SeedConversationAsync(store, perAppDb, "chats/b", "demo", earlierToday, messages: 5, tokens: 250);
        // Outside today's window — must not appear in the series.
        await SeedConversationAsync(store, perAppDb, "chats/old", "demo", now.AddDays(-20), messages: 9, tokens: 999);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var points = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, points.ValueKind);
        // Contiguous hourly series over the selected day.
        Assert.Equal(24, points.GetArrayLength());

        long totalInvocations = 0, totalTokens = 0;
        foreach (var p in points.EnumerateArray())
        {
            Assert.True(p.TryGetProperty("timestamp", out _), "point is missing 'timestamp'");
            totalInvocations += p.GetProperty("messages").GetInt64();
            totalTokens += p.GetProperty("tokens").GetInt64();
        }

        // Only the two conversations from earlier today fall in the window.
        Assert.Equal(8, totalInvocations);  // 3 + 5 messages
        Assert.Equal(350, totalTokens);     // 100 + 250 tokens
    }

    [RavenFact(RavenTestCategory.Quill)]
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
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(store, appDb1, "chats/a", "demo", earlierToday, messages: 2, tokens: 50);
        await SeedConversationAsync(store, appDb2, "chats/b", "demo", earlierToday, messages: 4, tokens: 70);
        await Indexes.WaitForIndexingAsync(store, appDb1);
        await Indexes.WaitForIndexingAsync(store, appDb2);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var points = await resp.Content.ReadFromJsonAsync<JsonElement>();
        long totalInvocations = 0, totalTokens = 0;
        foreach (var p in points.EnumerateArray())
        {
            totalInvocations += p.GetProperty("messages").GetInt64();
            totalTokens += p.GetProperty("tokens").GetInt64();
        }

        // Both apps' conversations are summed into the one global series.
        Assert.Equal(6, totalInvocations);  // 2 + 4 messages
        Assert.Equal(120, totalTokens);     // 50 + 70 tokens
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_invocations_count_user_messages_only()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        // Real-shaped doc: one user message + system/assistant/tool scaffolding.
        var now = DateTime.UtcNow;
        await SeedRealisticConversationAsync(store, perAppDb, "chats/r", "demo", EarlierToday(now));
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var points = await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
        long invocations = 0;
        foreach (var p in points.EnumerateArray())
            invocations += p.GetProperty("messages").GetInt64();
        Assert.Equal(1, invocations);  // only the user message counts (not system/assistant/tool)
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_degrades_to_empty_when_index_not_deployed()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        // The metrics index is intentionally NOT deployed on this app DB.

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var now = DateTime.UtcNow;
        var resp = await client.GetAsync($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());  // not 500
        var points = await resp.Content.ReadFromJsonAsync<JsonElement>();
        long invocations = 0;
        foreach (var p in points.EnumerateArray())
            invocations += p.GetProperty("messages").GetInt64();
        Assert.Equal(0, invocations);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_requires_authentication()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        // Authorization runs before parameter binding, so a missing key is 401 regardless of query.
        var now = DateTime.UtcNow;
        var resp = await client.GetAsync($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
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
        await SeedConversationAsync(store, healthyDb, "chats/a", "support", EarlierToday(now), messages: 3, tokens: 120);
        await Indexes.WaitForIndexingAsync(store, healthyDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // I2: one bad tenant DB must not 500 the whole endpoint — healthy data still returns.
        var usageResp = await client.GetAsync($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
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
        var dashResp = await client.GetAsync($"/api/dashboard?{Q(now.Year, now.Month, now.Day)}");
        Assert.Equal(HttpStatusCode.OK, dashResp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_fields_control_bucket_layout_and_point_count()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        // Seed earlier this month so it lands in both the month (year+month) and year (year) windows.
        var now = DateTime.UtcNow;
        var thisMonth = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(Math.Min(now.Day - 1, 1));
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", thisMonth, messages: 2, tokens: 100);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // year+month+day → 24 hours, year+month → the month's days, year → 12 months.
        Assert.Equal(24, (await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year, now.Month, now.Day)}")).GetArrayLength());
        Assert.Equal(DateTime.DaysInMonth(now.Year, now.Month),
            (await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year, now.Month)}")).GetArrayLength());
        var byMonth = await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year)}");
        Assert.Equal(12, byMonth.GetArrayLength());

        // The conversation seeded this month lands in the year window with all three metrics.
        long conv = 0, msg = 0, tok = 0;
        foreach (var p in byMonth.EnumerateArray())
        {
            conv += p.GetProperty("conversations").GetInt64();
            msg += p.GetProperty("messages").GetInt64();
            tok += p.GetProperty("tokens").GetInt64();
        }
        Assert.Equal(1, conv);
        Assert.Equal(2, msg);
        Assert.Equal(100, tok);
    }

    [RavenFact(RavenTestCategory.Quill)]
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
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(store, db1, "chats/a", "x", earlierToday, messages: 2, tokens: 50);
        await SeedConversationAsync(store, db2, "chats/b", "y", earlierToday, messages: 4, tokens: 70);
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

        var appOne = await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year, now.Month, now.Day, "app-one")}");
        Assert.Equal(2, Sum(appOne, "messages"));   // only app-one's data
        Assert.Equal(50, Sum(appOne, "tokens"));

        var all = await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
        Assert.Equal(6, Sum(all, "messages"));       // both apps summed
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_tolerates_conversation_doc_missing_usage_and_messages()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", earlierToday, messages: 2, tokens: 100);
        // A @conversations doc with neither TotalUsage nor Messages — the index reads those via
        // dynamic member access (DynamicNullObject, not NRE), so it must contribute 0, not error.
        await PutConversationDocAsync(store, perAppDb, "chats/min",
            new { Agent = "support", CreatedAt = earlierToday, LastMessageAt = earlierToday });
        await Indexes.WaitForIndexingAsync(store, perAppDb);   // throws if the index errored on the partial doc

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var points = await client.GetFromJsonAsync<JsonElement>($"/api/usage?{Q(now.Year, now.Month, now.Day)}");
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

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_requires_year()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // year is the one required field; without it the int binding fails → 400.
        var resp = await client.GetAsync("/api/usage?month=5");
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }
}
