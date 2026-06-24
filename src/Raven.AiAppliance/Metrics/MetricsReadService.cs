using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Session;

namespace Raven.AiAppliance.Metrics;

/// <summary>
/// Read-side aggregation for the dashboard stats endpoints. Queries the
/// per-app metric indexes and folds the hour buckets into the rolling windows
/// the views ask for (24h / 7d / 30d). Reads accept eventual consistency — no
/// <c>WaitForNonStaleResults</c> — so a dashboard refresh never blocks on
/// indexing; tests wait for indexing explicitly.
/// </summary>
internal static class MetricsReadService
{
    // Channels per app are few (a handful); read them by id prefix (immediately
    // consistent, no index) with a generous cap rather than a staleness-prone query.
    private const int ChannelPageSize = 1024;

    // Apps (tenants) are also modest in number; read the registry by id prefix.
    private const string AppIdPrefix = "apps/";
    private const int AppPageSize = 1024;

    // The global usage series is a contiguous hourly sparkline over the last day.
    private const int UsageWindowHours = 24;

    /// <summary>
    /// Global usage series behind the prototype's <c>getUsage()</c>: one
    /// <see cref="UsagePoint"/> per hour over the last 24h (contiguous, zero-filled),
    /// summed across every app DB. <c>invocations</c> = agent turns (messages),
    /// <c>tokens</c> = token usage, both per hour.
    /// </summary>
    public static async Task<List<UsagePoint>> GetUsageAsync(
        IDocumentStore store, DateTime nowUtc, CancellationToken ct)
    {
        var nowHour = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Utc);
        var since = nowHour.AddHours(-(UsageWindowHours - 1));

        var invocations = new long[UsageWindowHours];
        var tokens = new long[UsageWindowHours];

        foreach (var app in await LoadAllAppsAsync(store, ct))
        {
            using var session = store.OpenAsyncSession(app.Database);
            var rows = await QueryMetricRowsAsync(session, since, ct);
            foreach (var row in rows)
            {
                var bucket = (int)Math.Round((row.Bucket - since).TotalHours);
                if (bucket < 0 || bucket >= UsageWindowHours)
                    continue;
                invocations[bucket] += row.Messages;
                tokens[bucket] += row.Tokens;
            }
        }

        var points = new List<UsagePoint>(UsageWindowHours);
        for (var i = 0; i < UsageWindowHours; i++)
            points.Add(new UsagePoint(since.AddHours(i), invocations[i], tokens[i]));
        return points;
    }

    /// <summary>
    /// Per-app token totals behind the prototype's <c>getTokensByApp()</c>: all-time
    /// token usage summed from each app's <c>@conversations</c> (fan-out), sorted by
    /// tokens descending. <c>refreshedMinutesAgo</c> is 0 — computed live per request.
    /// </summary>
    public static async Task<TokensByAppResponse> GetTokensByAppAsync(IDocumentStore store, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);
        var rows = new List<AppTokens>(apps.Count);
        foreach (var app in apps)
        {
            using var session = store.OpenAsyncSession(app.Database);
            var metricRows = await QueryAllMetricRowsAsync(session, ct);
            rows.Add(new AppTokens(app.Slug, metricRows.Sum(r => r.Tokens)));
        }

        var sorted = rows
            .OrderByDescending(a => a.Tokens)
            .ThenBy(a => a.Slug, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return new TokensByAppResponse(sorted, RefreshedMinutesAgo: 0);
    }

    // Same per-token cost factor the prototype uses for the cost tile.
    private const double CostPerToken = 0.000015;

    // Stable-ish palette so each capability series gets a colour without config.
    private static readonly string[] SeriesPalette =
        ["#3b82f6", "#8b5cf6", "#10b981", "#f59e0b", "#ef4444", "#22d3ee", "#a855f7", "#84cc16"];

    /// <summary>
    /// Per-app usage analytics behind <c>getAppUsage({appId,start,end})</c>. Phase-1
    /// subset from <see cref="ConversationMetricsIndex"/>: conversations/tokens/cost
    /// KPI cards (value + delta-vs-previous-window + per-bucket sparkline),
    /// <c>tokensByCapability</c>, and <c>topCapabilities</c>. cdcWrites/topTables
    /// (RavenDB-26780), tokensByModel (no model recorded) and conversationsByChannel
    /// (no channel link) return empty skeletons — see the impl handoff.
    /// </summary>
    public static async Task<AppUsageResponse> GetAppUsageAsync(
        IDocumentStore store, string database, DateTime startUtc, DateTime endUtc, CancellationToken ct)
    {
        var granularity = (endUtc - startUtc).TotalDays <= 2 ? UsageGranularity.Hour : UsageGranularity.Day;

        using var session = store.OpenAsyncSession(database);
        var rows = await QueryMetricRowsInRangeAsync(session, startUtc, endUtc, ct);

        // Previous equal-length window drives the percent delta on each card.
        var windowLength = endUtc - startUtc;
        var prevRows = await QueryMetricRowsInRangeAsync(session, startUtc - windowLength, startUtc, ct);

        long convNow = rows.Sum(r => r.Conversations), tokNow = rows.Sum(r => r.Tokens);
        long convPrev = prevRows.Sum(r => r.Conversations), tokPrev = prevRows.Sum(r => r.Tokens);

        var buckets = BuildBuckets(startUtc, endUtc, granularity);
        var convByBucket = new long[buckets.Count];
        var tokByBucket = new long[buckets.Count];
        foreach (var row in rows)
        {
            var i = BucketIndex(buckets, row.Bucket, granularity);
            if (i < 0) continue;
            convByBucket[i] += row.Conversations;
            tokByBucket[i] += row.Tokens;
        }

        var metrics = new AppUsageMetrics(
            Conversations: new MetricCard(convNow, Delta(convNow, convPrev), ToDoubles(convByBucket)),
            Tokens: new MetricCard(tokNow, Delta(tokNow, tokPrev), ToDoubles(tokByBucket)),
            Cost: new MetricCard(Math.Round(tokNow * CostPerToken, 2), Delta(tokNow, tokPrev),
                tokByBucket.Select(t => t * CostPerToken).ToArray()),
            CdcWrites: new MetricCard(0, 0, [])); // CDC blocked on RavenDB-26780.

        // tokensByCapability: per-bucket tokens per agent (capability ≈ agent).
        var agents = rows.Select(r => r.Agent ?? "").Distinct()
            .OrderBy(a => a, StringComparer.OrdinalIgnoreCase).ToArray();
        var capKeys = agents
            .Select((a, idx) => new SeriesKey(a, a, SeriesPalette[idx % SeriesPalette.Length])).ToArray();
        var capPoints = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
        {
            var point = new Dictionary<string, object> { ["t"] = BucketLabel(buckets[b], granularity) };
            foreach (var a in agents) point[a] = 0L;
            capPoints[b] = point;
        }
        foreach (var row in rows)
        {
            var i = BucketIndex(buckets, row.Bucket, granularity);
            if (i < 0) continue;
            var key = row.Agent ?? "";
            capPoints[i][key] = (long)capPoints[i][key] + row.Tokens;
        }

        var topCapabilities = rows
            .GroupBy(r => r.Agent ?? "")
            .Select(g =>
            {
                var invocations = g.Sum(r => r.Conversations);
                var total = g.Sum(r => r.Tokens);
                return new TopCapability(g.Key, invocations, invocations == 0 ? 0 : total / invocations,
                    total, Math.Round(total * CostPerToken, 2));
            })
            .OrderByDescending(c => c.TotalTokens)
            .ThenBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var empty = new SeriesData([], []);
        return new AppUsageResponse(
            granularity, metrics,
            TokensByCapability: new SeriesData(capPoints, capKeys),
            TokensByModel: empty,            // no model recorded on @conversations
            ConversationsByChannel: empty,   // no channel link on @conversations
            CdcWrites: [],                   // RavenDB-26780
            TopTables: [],                   // RavenDB-26780
            TopCapabilities: topCapabilities);
    }

    private static double Delta(long now, long prev) =>
        prev == 0 ? 0 : Math.Round((now - prev) / (double)prev * 100, 1);

    private static double[] ToDoubles(long[] values)
    {
        var result = new double[values.Length];
        for (var i = 0; i < values.Length; i++)
            result[i] = values[i];
        return result;
    }

    private static TimeSpan Step(UsageGranularity granularity) =>
        granularity == UsageGranularity.Hour ? TimeSpan.FromHours(1) : TimeSpan.FromDays(1);

    private static List<DateTime> BuildBuckets(DateTime start, DateTime end, UsageGranularity granularity)
    {
        var step = Step(granularity);
        var cur = granularity == UsageGranularity.Hour
            ? new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0, DateTimeKind.Utc)
            : new DateTime(start.Year, start.Month, start.Day, 0, 0, 0, DateTimeKind.Utc);
        var buckets = new List<DateTime>();
        while (cur <= end) { buckets.Add(cur); cur = cur.Add(step); }
        if (buckets.Count == 0) buckets.Add(cur);
        return buckets;
    }

    private static int BucketIndex(List<DateTime> buckets, DateTime bucketUtc, UsageGranularity granularity)
    {
        var idx = (int)Math.Floor((bucketUtc - buckets[0]) / Step(granularity));
        return idx >= 0 && idx < buckets.Count ? idx : -1;
    }

    private static string BucketLabel(DateTime bucketUtc, UsageGranularity granularity) =>
        granularity == UsageGranularity.Hour ? bucketUtc.ToString("yyyy-MM-ddTHH:00") : bucketUtc.ToString("yyyy-MM-dd");

    public static async Task<DashboardResponse> GetDashboardStatsAsync(
        IDocumentStore store, DateTime nowUtc, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);

        // Read-time fan-out: fold every app's rows into one shared set of windows.
        var last24h = new WindowAccumulator();
        var last7d = new WindowAccumulator();
        var last30d = new WindowAccumulator();

        foreach (var app in apps)
        {
            using var session = store.OpenAsyncSession(app.Database);
            var rows = await QueryMetricRowsAsync(session, nowUtc.AddDays(-30), ct);
            FoldInto(rows, nowUtc, ref last24h, ref last7d, ref last30d);
        }

        return new DashboardResponse(apps.Count, last24h.ToWindow(), last7d.ToWindow(), last30d.ToWindow());
    }

    public static async Task<AppOverviewResponse> GetAppOverviewAsync(
        IDocumentStore store, string slug, string database, CancellationToken ct)
    {
        var maintenance = store.Maintenance.ForDatabase(database);
        var stats = await maintenance.SendAsync(new GetStatisticsOperation(), ct);
        var agents = await maintenance.SendAsync(new GetAiAgentsOperation(), ct);
        var channels = await GetChannelStatsAsync(store, database, ct);

        return new AppOverviewResponse(
            slug,
            stats.CountOfDocuments,
            agents.AiAgents?.Count ?? 0,
            channels.Total,
            channels.Active);
    }

    public static async Task<ChannelStatsResponse> GetChannelStatsAsync(
        IDocumentStore store, string database, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);

        var total = 0;
        var active = 0;
        var offset = 0;
        while (true)
        {
            var page = (await session.Advanced.LoadStartingWithAsync<Channel>(
                Channel.IdPrefix, start: offset, pageSize: ChannelPageSize, token: ct)).ToList();
            foreach (var channel in page)
            {
                total++;
                if (channel.Enabled) active++;
            }
            // partial page = no more results
            if (page.Count < ChannelPageSize) break;
            offset += ChannelPageSize;
        }

        return new ChannelStatsResponse(total, active);
    }

    public static async Task<ConversationStatsResponse> GetConversationStatsAsync(
        IDocumentStore store, string database, DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var rows = await QueryMetricRowsAsync(session, nowUtc.AddDays(-30), ct);

        var (last24h, last7d, last30d) = FoldWindows(rows, nowUtc);
        return new ConversationStatsResponse(last24h, last7d, last30d);
    }

    public static async Task<AgentStatsResponse> GetAgentStatsAsync(
        IDocumentStore store, string database, DateTime nowUtc, CancellationToken ct)
    {
        var agents = await store.Maintenance.ForDatabase(database).SendAsync(new GetAiAgentsOperation(), ct);
        var configuredAgents = agents.AiAgents?.Count ?? 0;

        using var session = store.OpenAsyncSession(database);
        var rows = await QueryMetricRowsAsync(session, nowUtc.AddDays(-30), ct);

        var (last24h, last7d, last30d) = FoldWindows(rows, nowUtc);

        var perAgent = rows
            .GroupBy(row => row.Agent ?? "")
            .Select(group => new AgentUsageSummary(
                group.Key,
                group.Sum(row => row.Conversations),
                group.Sum(row => row.Messages),
                group.Sum(row => row.Tokens)))
            .OrderBy(agent => agent.AgentId, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return new AgentStatsResponse(configuredAgents, last24h, last7d, last30d, perAgent);
    }

    /// <summary>
    /// Mirrored collections behind <c>listCollections(appId)</c>: each business
    /// collection with its current document count. System (<c>@</c>-prefixed)
    /// collections are excluded; <c>fields</c> is empty for now (schemaless).
    /// </summary>
    public static async Task<List<DataCollectionDto>> GetCollectionsAsync(
        IDocumentStore store, string slug, string database, CancellationToken ct)
    {
        var stats = await store.Maintenance.ForDatabase(database)
            .SendAsync(new GetCollectionStatisticsOperation(), ct);

        return stats.Collections
            .Where(c => c.Key.StartsWith('@') == false)
            .OrderBy(c => c.Key, StringComparer.OrdinalIgnoreCase)
            .Select(c => new DataCollectionDto(slug, c.Key, c.Value, []))
            .ToList();
    }

    /// <summary>Loads every registered app from the config DB by id prefix, paging
    /// so installations with more than one page are fully returned.</summary>
    private static async Task<List<App>> LoadAllAppsAsync(IDocumentStore store, CancellationToken ct)
    {
        var apps = new List<App>();
        using var configSession = store.OpenAsyncSession();
        var offset = 0;
        while (true)
        {
            var page = (await configSession.Advanced.LoadStartingWithAsync<App>(
                AppIdPrefix, start: offset, pageSize: AppPageSize, token: ct)).ToList();
            apps.AddRange(page);
            if (page.Count < AppPageSize) break;
            offset += AppPageSize;
        }
        return apps;
    }

    /// <summary>Fetches the hour-bucket rows from the widest window (server-side
    /// filter keeps the row count bounded) for client-side folding/grouping.</summary>
    private static Task<List<ConversationMetricsIndex.Result>> QueryMetricRowsAsync(
        IAsyncDocumentSession session, DateTime since, CancellationToken ct)
    {
        return session.Advanced
            .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
            .WhereGreaterThanOrEqual(row => row.Bucket, since)
            .ToListAsync(ct);
    }

    /// <summary>Fetches every hour-bucket row (no time filter) for all-time totals.</summary>
    private static Task<List<ConversationMetricsIndex.Result>> QueryAllMetricRowsAsync(
        IAsyncDocumentSession session, CancellationToken ct)
    {
        return session.Advanced
            .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
            .ToListAsync(ct);
    }

    /// <summary>Fetches hour-bucket rows whose bucket falls within [start, end].</summary>
    private static Task<List<ConversationMetricsIndex.Result>> QueryMetricRowsInRangeAsync(
        IAsyncDocumentSession session, DateTime start, DateTime end, CancellationToken ct)
    {
        return session.Advanced
            .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
            .WhereGreaterThanOrEqual(r => r.Bucket, start)
            .AndAlso()
            .WhereLessThanOrEqual(r => r.Bucket, end)
            .ToListAsync(ct);
    }

    private static (ConversationWindow Last24h, ConversationWindow Last7d, ConversationWindow Last30d) FoldWindows(
        IReadOnlyList<ConversationMetricsIndex.Result> rows, DateTime nowUtc)
    {
        var last24h = new WindowAccumulator();
        var last7d = new WindowAccumulator();
        var last30d = new WindowAccumulator();

        FoldInto(rows, nowUtc, ref last24h, ref last7d, ref last30d);

        return (last24h.ToWindow(), last7d.ToWindow(), last30d.ToWindow());
    }

    /// <summary>Fans each hour row into the nested windows it belongs to in a
    /// single pass (24h ⊂ 7d ⊂ 30d; rows are already within 30d). The
    /// accumulators are shared so the dashboard fan-out can fold many apps into
    /// one set of totals.</summary>
    private static void FoldInto(
        IReadOnlyList<ConversationMetricsIndex.Result> rows, DateTime nowUtc,
        ref WindowAccumulator last24h, ref WindowAccumulator last7d, ref WindowAccumulator last30d)
    {
        var since7 = nowUtc.AddDays(-7);
        var since24h = nowUtc.AddHours(-24);

        foreach (var row in rows)
        {
            last30d.Add(row);
            if (row.Bucket >= since7)
                last7d.Add(row);
            if (row.Bucket >= since24h)
                last24h.Add(row);
        }
    }

    private struct WindowAccumulator
    {
        private long _conversations;
        private long _messages;
        private long _tokens;

        public void Add(ConversationMetricsIndex.Result row)
        {
            _conversations += row.Conversations;
            _messages += row.Messages;
            _tokens += row.Tokens;
        }

        public readonly ConversationWindow ToWindow() => new(_conversations, _messages, _tokens);
    }
}
