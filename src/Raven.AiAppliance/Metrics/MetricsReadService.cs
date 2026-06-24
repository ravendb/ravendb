using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
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

    // Conversation docs (@conversations collection) are id-prefixed "chats/".
    private const string ConversationIdPrefix = "chats/";

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

    // Series key for tokens whose agent has no resolvable connection-string model.
    private const string UnknownModel = "unknown";

    // Cap on the "top tables" (collections) list returned for the App Usage page.
    private const int TopTablesLimit = 10;

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
        var maintenance = store.Maintenance.ForDatabase(database);

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

        // Per-bucket token series: by capability (agent) and by model (resolved via
        // each agent's connection string). Same shape, different key.
        var modelByAgent = await ResolveAgentModelsAsync(maintenance, ct);
        var tokensByCapability = BuildTokenSeries(rows, buckets, granularity, agent => agent);
        var tokensByModel = BuildTokenSeries(rows, buckets, granularity,
            agent => modelByAgent.GetValueOrDefault(agent, UnknownModel));

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

        var conversationsByChannel = await BuildConversationsByChannelAsync(
            session, buckets, startUtc, endUtc, granularity, ct);
        var topTables = await BuildTopTablesAsync(maintenance, ct);

        return new AppUsageResponse(
            granularity, metrics,
            TokensByCapability: tokensByCapability,
            TokensByModel: tokensByModel,
            ConversationsByChannel: conversationsByChannel,
            CdcWrites: [],                   // RavenDB-26780 (CDC perf stats)
            TopTables: topTables,
            TopCapabilities: topCapabilities);
    }

    /// <summary>Builds a multi-series token chart from the hour-bucket rows: one
    /// series per distinct <paramref name="keyOf"/>(agent), per-bucket token sums.
    /// Used for both tokensByCapability (key = agent) and tokensByModel (key = the
    /// agent's resolved model).</summary>
    private static SeriesData BuildTokenSeries(
        IReadOnlyList<ConversationMetricsIndex.Result> rows, List<DateTime> buckets,
        UsageGranularity granularity, Func<string, string> keyOf)
    {
        var keys = rows.Select(r => keyOf(r.Agent ?? "")).Distinct()
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys
            .Select((k, idx) => new SeriesKey(k, k, SeriesPalette[idx % SeriesPalette.Length])).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
        {
            var point = new Dictionary<string, object> { ["t"] = BucketLabel(buckets[b], granularity) };
            foreach (var k in keys) point[k] = 0L;
            points[b] = point;
        }
        foreach (var row in rows)
        {
            var i = BucketIndex(buckets, row.Bucket, granularity);
            if (i < 0) continue;
            var key = keyOf(row.Agent ?? "");
            points[i][key] = (long)points[i][key] + row.Tokens;
        }
        return new SeriesData(points, seriesKeys);
    }

    /// <summary>agentId → model, joining the AI agents (each carries a
    /// <c>ConnectionStringName</c>) with the database's AI connection strings
    /// (each carries the provider model). Agents with no resolvable model are
    /// omitted (callers fall back to <see cref="UnknownModel"/>).</summary>
    private static async Task<Dictionary<string, string>> ResolveAgentModelsAsync(
        MaintenanceOperationExecutor maintenance, CancellationToken ct)
    {
        var agents = await maintenance.SendAsync(new GetAiAgentsOperation(), ct);
        var connectionStrings = await maintenance.SendAsync(new GetConnectionStringsOperation(), ct);
        var modelByConnectionString = (connectionStrings.AiConnectionStrings ?? new Dictionary<string, AiConnectionString>())
            .ToDictionary(p => p.Key, p => AiConnectionStringModel.Resolve(p.Value), StringComparer.OrdinalIgnoreCase);

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var agent in agents.AiAgents ?? [])
        {
            if (agent.ConnectionStringName is { } name
                && modelByConnectionString.TryGetValue(name, out var model)
                && string.IsNullOrWhiteSpace(model) == false)
            {
                map[agent.Identifier] = model!;
            }
        }
        return map;
    }

    /// <summary>conversations-per-channel over time, iframe only: each
    /// <see cref="EmbedLink"/> (one per conversation) bucketed by its
    /// <c>CreatedAt</c> and attributed to its channel via <c>WidgetId</c>.
    /// Telegram/WhatsApp aren't implemented, so they don't appear.</summary>
    private static async Task<SeriesData> BuildConversationsByChannelAsync(
        IAsyncDocumentSession session, List<DateTime> buckets, DateTime startUtc, DateTime endUtc,
        UsageGranularity granularity, CancellationToken ct)
    {
        var channels = await session.Advanced.LoadStartingWithAsync<Channel>(
            Channel.IdPrefix, pageSize: 1024, token: ct);
        var nameByWidget = channels
            .Where(c => c.Id is not null)
            .ToDictionary(c => c.Id![Channel.IdPrefix.Length..],
                c => string.IsNullOrWhiteSpace(c.DisplayName) ? c.Id![Channel.IdPrefix.Length..] : c.DisplayName,
                StringComparer.OrdinalIgnoreCase);
        if (nameByWidget.Count == 0)
            return new SeriesData([], []);

        var keys = nameByWidget.Values.Distinct().OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys
            .Select((k, idx) => new SeriesKey(k, k, SeriesPalette[idx % SeriesPalette.Length])).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
        {
            var point = new Dictionary<string, object> { ["t"] = BucketLabel(buckets[b], granularity) };
            foreach (var k in keys) point[k] = 0L;
            points[b] = point;
        }

        var links = await session.Advanced.LoadStartingWithAsync<EmbedLink>(
            EmbedLink.IdPrefix, pageSize: 1024, token: ct);
        foreach (var link in links)
        {
            if (link.CreatedAt < startUtc || link.CreatedAt > endUtc) continue;
            if (link.WidgetId is null || nameByWidget.TryGetValue(link.WidgetId, out var channelName) == false) continue;
            var i = BucketIndex(buckets, link.CreatedAt, granularity);
            if (i < 0) continue;
            points[i][channelName] = (long)points[i][channelName] + 1L;
        }
        return new SeriesData(points, seriesKeys);
    }

    /// <summary>"Top tables" = the app's business collections by document count
    /// (egor: collection stats stand in for CDC source-table stats until
    /// RavenDB-26780). <c>lagSeconds</c>/<c>lastWriteAt</c> are CDC-perf data, left
    /// empty for now.</summary>
    private static async Task<TopTable[]> BuildTopTablesAsync(
        MaintenanceOperationExecutor maintenance, CancellationToken ct)
    {
        var stats = await maintenance.SendAsync(new GetCollectionStatisticsOperation(), ct);
        return stats.Collections
            .Where(c => c.Key.StartsWith('@') == false)
            .OrderByDescending(c => c.Value)
            .ThenBy(c => c.Key, StringComparer.OrdinalIgnoreCase)
            .Take(TopTablesLimit)
            .Select(c => new TopTable(c.Key, c.Value, LagSeconds: 0, LastWriteAt: ""))
            .ToArray();
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

    /// <summary>Conversations for the Conversations list — shaped from
    /// <c>@conversations</c> docs (id-prefixed <c>chats/</c>), newest activity first,
    /// last-exchange preview only (no full transcript).</summary>
    public static async Task<List<ConversationDto>> GetConversationsAsync(
        IDocumentStore store, string database, DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var docs = await session.Advanced.LoadStartingWithAsync<ConversationDoc>(
            ConversationIdPrefix, pageSize: 1024, token: ct);
        return docs
            .Select(d => ShapeConversation(d, nowUtc, includeTranscript: false))
            .OrderByDescending(c => c.LastActivityAt)
            .ToList();
    }

    /// <summary>One conversation with its full chronological transcript, or null.</summary>
    public static async Task<ConversationDto?> GetConversationAsync(
        IDocumentStore store, string database, string conversationId, DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var doc = await session.LoadAsync<ConversationDoc>(conversationId, ct);
        return doc is null ? null : ShapeConversation(doc, nowUtc, includeTranscript: true);
    }

    private static ConversationDto ShapeConversation(ConversationDoc doc, DateTime nowUtc, bool includeTranscript)
    {
        var agentName = doc.Agent ?? "";
        var chrono = (doc.Messages ?? [])
            .Select(m => new ConversationTurn(m.role ?? "", TextOf(m.content), m.date))
            .OrderBy(t => t.At ?? doc.CreatedAt)
            .ToArray();
        var lastExchange = chrono.TakeLast(2).Reverse().ToArray();  // newest first, at most 2
        var prms = (doc.Parameters ?? new Dictionary<string, object>())
            .Select(kv => new ConversationParam(kv.Key, kv.Value?.ToString() ?? ""))
            .ToArray();

        var age = nowUtc - doc.LastMessageAt;
        var state = age < TimeSpan.FromHours(1) ? "active" : age < TimeSpan.FromHours(24) ? "idle" : "closed";

        return new ConversationDto(
            doc.Id ?? "", ChannelName: "", agentName,
            AgentInitials(agentName), AgentColor(agentName),
            prms, lastExchange, includeTranscript ? chrono : null,
            state, doc.LastMessageAt, doc.CreatedAt, MaxDuration: null);
    }

    // Content is string | array-of-parts | object on the server doc; use the string
    // form directly and JSON-stringify the rest (best-effort) — see impl handoff.
    private static string TextOf(object? content) => content switch
    {
        null => "",
        string s => s,
        _ => content.ToString() ?? "",
    };

    private static string AgentInitials(string name)
    {
        var parts = name.Split([' ', '-', '_'], StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return "?";
        if (parts.Length == 1) return parts[0][..Math.Min(2, parts[0].Length)].ToUpperInvariant();
        return $"{parts[0][0]}{parts[1][0]}".ToUpperInvariant();
    }

    private static string AgentColor(string name)
    {
        var hash = 0;
        foreach (var c in name) hash = unchecked(hash * 31 + c);
        return SeriesPalette[Math.Abs(hash % SeriesPalette.Length)];
    }

    private sealed class ConversationDoc
    {
        public string? Id { get; set; }
        public string? Agent { get; set; }
        public Dictionary<string, object>? Parameters { get; set; }
        public List<ConversationMessageDoc>? Messages { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime LastMessageAt { get; set; }
    }

    private sealed class ConversationMessageDoc
    {
        public string? role { get; set; }
        public object? content { get; set; }
        public DateTime date { get; set; }
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
