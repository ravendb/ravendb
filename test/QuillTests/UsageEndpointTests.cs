using System.Net;
using Raven.Client.Documents.Operations.Indexes;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Auth;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

[Collection(QuillFanOutCollection.Name)]
public class UsageEndpointTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    // earlier today so seeds land in today's hourly buckets regardless of run hour
    private static DateTime EarlierToday(DateTime now) =>
        new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, DateTimeKind.Utc).AddHours(Math.Min(now.Hour, 1));

    private static long Sum(IReadOnlyList<UsagePoint> points, Func<UsagePoint, long> field)
    {
        long total = 0;
        foreach (var p in points)
            total += field(p);
        return total;
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_returns_24_hourly_points_summing_invocations_and_tokens()
    {
        await using var app = await NewAppAsync();
        // past day → full unclamped 24 buckets (current day clamps to now)
        var now = DateTime.UtcNow;
        var day = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, DateTimeKind.Utc).AddDays(-1);
        var duringDay = day.AddHours(1);
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "demo", duringDay, messages: 3, tokens: 100);
        await SeedConversationAsync(app.Store, app.Slug, "chats/b", "demo", duringDay, messages: 5, tokens: 250);
        await SeedConversationAsync(app.Store, app.Slug, "chats/old", "demo", day.AddDays(-20), messages: 9, tokens: 999);

        var points = (await Host.GetUsageAsync(day.Year, day.Month, day.Day)).Points;
        Assert.Equal(24, points.Count);

        Assert.Equal(8, Sum(points, p => p.Messages));
        Assert.Equal(350, Sum(points, p => p.Tokens));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_sums_across_app_databases()
    {
        await using var appOne = await NewAppAsync();
        await using var appTwo = await NewAppAsync();

        var now = DateTime.UtcNow;
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(appOne.Store, appOne.Slug, "chats/a", "demo", earlierToday, messages: 2, tokens: 50);
        await SeedConversationAsync(appTwo.Store, appTwo.Slug, "chats/b", "demo", earlierToday, messages: 4, tokens: 70);

        var points = (await Host.GetUsageAsync(now.Year, now.Month, now.Day)).Points;

        Assert.Equal(6, Sum(points, p => p.Messages));
        Assert.Equal(120, Sum(points, p => p.Tokens));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_invocations_count_user_messages_only()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        await SeedRealisticConversationAsync(app.Store, app.Slug, "chats/r", "demo", EarlierToday(now));

        var points = (await Host.GetUsageAsync(now.Year, now.Month, now.Day)).Points;
        Assert.Equal(1, Sum(points, p => p.Messages));  // only the user message counts, not system/assistant/tool
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_invocations_skip_the_injected_parameters_message()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/p", "demo", EarlierToday(now), richMessages:
        [
            ("system", "You are a helpful assistant."),
            ("user", "AI Agent Parameters:\ncompany = companies/1-A\r\n"),
            ("user", "what do you sell?"),
            ("assistant", "coffee"),
        ]);

        var points = (await Host.GetUsageAsync(now.Year, now.Month, now.Day)).Points;
        Assert.Equal(1, Sum(points, p => p.Messages));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_degrades_to_empty_when_index_not_deployed()
    {
        await using var app = await NewAppAsync();
        // delete the auto-deployed index → exercise not-deployed path (must degrade to empty, not 500)
        await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(
            new DeleteIndexOperation(new ConversationMetricsIndex().IndexName));

        var now = DateTime.UtcNow;
        var points = (await Host.GetUsageAsync(now.Year, now.Month, now.Day)).Points;
        Assert.Equal(0, Sum(points, p => p.Messages));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_requires_authentication()
    {
        // throwaway client off the shared factory so dropping the key doesn't unauthenticate the shared client
        using var client = Host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        // auth runs before parameter binding → missing key is 401 regardless of query
        var now = DateTime.UtcNow;
        var resp = await client.GetAsync($"{QuillRoutes.Usage}?year={now.Year}&month={now.Month}&day={now.Day}");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_skips_unhealthy_app_db_and_returns_partial()
    {
        // own host: this test seeds a raw apps/{slug} doc the registry can't clean, which would pollute a
        // shared config store's fan-out; an own host is disposed (with its config DB) at test end
        await using var host = await NewHostAsync();
        await using var healthy = await NewAppAsync(host);
        // no EP creates a dangling app → seed the App doc directly with a non-existent Database
        using (var session = host.Config.OpenAsyncSession())
        {
            var brokenSlug = "broken-" + Guid.NewGuid().ToString("N");
            await session.StoreAsync(new App
            {
                Slug = brokenSlug,
                AppName = brokenSlug,
                Database = "missing-" + Guid.NewGuid().ToString("N"),
                CdcTaskName = $"{brokenSlug}-cdc",
                CreatedAt = DateTime.UtcNow,
            }, $"apps/{brokenSlug}");
            await session.SaveChangesAsync();
        }

        var now = DateTime.UtcNow;
        await SeedConversationAsync(healthy.Store, healthy.Slug, "chats/a", "support", EarlierToday(now), messages: 3, tokens: 120);

        var points = (await host.GetUsageAsync(now.Year, now.Month, now.Day)).Points;
        Assert.Equal(3, Sum(points, p => p.Messages));
        Assert.Equal(120, Sum(points, p => p.Tokens));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_fields_control_bucket_layout_and_point_count()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        var thisMonth = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(Math.Min(now.Day - 1, 1));
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "support", thisMonth, messages: 2, tokens: 100);

        // past year → full unclamped layout (current period clamps to now)
        var pastYear = now.Year - 1;
        Assert.Equal(24, (await Host.GetUsageAsync(pastYear, 1, 15)).Points.Count);
        Assert.Equal(DateTime.DaysInMonth(pastYear, 1), (await Host.GetUsageAsync(pastYear, 1)).Points.Count);
        Assert.Equal(12, (await Host.GetUsageAsync(pastYear)).Points.Count);

        var byMonth = (await Host.GetUsageAsync(now.Year)).Points;
        Assert.Equal(1, Sum(byMonth, p => p.Conversations));
        Assert.Equal(2, Sum(byMonth, p => p.Messages));
        Assert.Equal(100, Sum(byMonth, p => p.Tokens));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_app_param_scopes_to_a_single_app()
    {
        await using var app1 = await NewAppAsync();
        await using var app2 = await NewAppAsync();

        var now = DateTime.UtcNow;
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(app1.Store, app1.Slug, "chats/a", "x", earlierToday, messages: 2, tokens: 50);
        await SeedConversationAsync(app2.Store, app2.Slug, "chats/b", "y", earlierToday, messages: 4, tokens: 70);

        var appOne = await Host.GetUsageAsync(now.Year, now.Month, now.Day, app1.Slug);
        Assert.Equal(2, Sum(appOne.Points, p => p.Messages));
        Assert.Equal(50, Sum(appOne.Points, p => p.Tokens));
        Assert.Equal(app1.Slug, Assert.Single(appOne.WritesByApp).Slug);

        var all = await Host.GetUsageAsync(now.Year, now.Month, now.Day);
        Assert.Equal(6, Sum(all.Points, p => p.Messages));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_reports_writes_for_every_app()
    {
        await using var app1 = await NewAppAsync();
        await using var app2 = await NewAppAsync();

        var now = DateTime.UtcNow;
        var usage = await Host.GetUsageAsync(now.Year, now.Month);

        // license write metering reports nothing in tests; each app still gets a row (0 writes)
        // so the dashboard can key the WRU column by slug
        Assert.Equal(0, Assert.Single(usage.WritesByApp, w => w.Slug == app1.Slug).Writes);
        Assert.Equal(0, Assert.Single(usage.WritesByApp, w => w.Slug == app2.Slug).Writes);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_tolerates_conversation_doc_missing_usage_and_messages()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        var earlierToday = EarlierToday(now);
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "support", earlierToday, messages: 2, tokens: 100);
        // doc missing TotalUsage/Messages → index reads via DynamicNullObject (contributes 0, not NRE)
        await PutConversationDocAsync(app.Store, app.Slug, "chats/min",
            new { Agent = "support", CreatedAt = earlierToday, LastMessageAt = earlierToday });

        var points = (await Host.GetUsageAsync(now.Year, now.Month, now.Day)).Points;
        Assert.Equal(2, Sum(points, p => p.Conversations));
        Assert.Equal(2, Sum(points, p => p.Messages));
        Assert.Equal(100, Sum(points, p => p.Tokens));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_requires_year()
    {
        var resp = await Host.Client.GetAsync($"{QuillRoutes.Usage}?month=5");
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }
}
