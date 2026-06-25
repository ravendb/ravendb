using System.Text.Json;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide.Operations;

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

    // Per-app fan-out (dashboard/usage) runs bounded-parallel and isolates failures.
    private const int MaxFanoutConcurrency = 8;

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
    /// Global usage series behind the prototype's <c>getUsage()</c>: one
    /// <see cref="UsagePoint"/> per hour over the last 24h (contiguous, zero-filled),
    /// summed across every app DB. <c>invocations</c> = agent turns (messages),
    /// <c>tokens</c> = token usage, both per hour.
    /// </summary>
    public static async Task<List<UsagePoint>> GetUsageAsync(
        IDocumentStore store, DateTime nowUtc, ILogger? log, CancellationToken ct)
    {
        var nowHour = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Utc);
        var since = nowHour.AddHours(-(UsageWindowHours - 1));

        var invocations = new long[UsageWindowHours];
        var tokens = new long[UsageWindowHours];

        var apps = await LoadAllAppsAsync(store, ct);
        var perApp = await ForEachAppAsync(apps, log, async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            return await QueryMetricRowsAsync(session, since, ct);
        }, ct);

        foreach (var rows in perApp)
            foreach (var row in rows)
            {
                var bucket = (int)Math.Round((row.Bucket - since).TotalHours);
                if (bucket < 0 || bucket >= UsageWindowHours)
                    continue;
                invocations[bucket] += row.Messages;
                tokens[bucket] += row.Tokens;
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
    public static async Task<TokensByAppResponse> GetTokensByAppAsync(
        IDocumentStore store, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);
        var rows = await ForEachAppAsync(apps, log, async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            var metricRows = await QueryAllMetricRowsAsync(session, ct);
            return new AppTokens(app.Slug, metricRows.Sum(r => r.Tokens));
        }, ct);

        var sorted = rows
            .OrderByDescending(a => a.Tokens)
            .ThenBy(a => a.Slug, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return new TokensByAppResponse(sorted, RefreshedMinutesAgo: 0);
    }

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

        // Previous equal-length window drives the percent delta on each card. Its upper
        // bound is exclusive so a row whose bucket == startUtc isn't counted in both
        // windows (review M4).
        var windowLength = endUtc - startUtc;
        var prevRows = await QueryMetricRowsInRangeAsync(
            session, startUtc - windowLength, startUtc, ct, endInclusive: false);

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

        // Resolve the agents once: connection-string model for tokensByModel, and the
        // display name for human-facing series labels / topCapabilities (review M2).
        var agentDefs = await maintenance.SendAsync(new GetAiAgentsOperation(), ct);
        var connectionStrings = await maintenance.SendAsync(new GetConnectionStringsOperation(), ct);
        var modelByConnectionString = (connectionStrings.AiConnectionStrings ?? new Dictionary<string, AiConnectionString>())
            .ToDictionary(p => p.Key, p => AiConnectionStringModel.Resolve(p.Value), StringComparer.OrdinalIgnoreCase);
        var modelByAgent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var nameByAgent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var agent in agentDefs.AiAgents ?? [])
        {
            nameByAgent[agent.Identifier] = string.IsNullOrWhiteSpace(agent.Name) ? agent.Identifier : agent.Name;
            if (agent.ConnectionStringName is { } name
                && modelByConnectionString.TryGetValue(name, out var model)
                && string.IsNullOrWhiteSpace(model) == false)
            {
                modelByAgent[agent.Identifier] = model!;
            }
        }
        string NameOf(string agentId) => nameByAgent.GetValueOrDefault(agentId, agentId);

        // Per-bucket token series: by capability (agent) and by model. Same shape,
        // different key; the capability series labels with the agent display name.
        var tokensByCapability = BuildTokenSeries(rows, buckets, granularity, agent => agent, NameOf);
        var tokensByModel = BuildTokenSeries(rows, buckets, granularity,
            agent => modelByAgent.GetValueOrDefault(agent, UnknownModel));

        var topCapabilities = rows
            .GroupBy(r => r.Agent ?? "")
            .Select(g =>
            {
                var invocations = g.Sum(r => r.Conversations);
                var total = g.Sum(r => r.Tokens);
                return new TopCapability(NameOf(g.Key), invocations, invocations == 0 ? 0 : total / invocations,
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
        UsageGranularity granularity, Func<string, string> keyOf, Func<string, string>? labelOf = null)
    {
        // The series key is the stable data key (used in each bucket's points dict); the
        // label is the human-facing name. Default the label to the key (review M2).
        var label = labelOf ?? (k => k);
        var keys = rows.Select(r => keyOf(r.Agent ?? "")).Distinct()
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys
            .Select((k, idx) => new SeriesKey(k, label(k), SeriesPalette[idx % SeriesPalette.Length])).ToArray();

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

        // Key by the stable WidgetId (dictionary keys are already distinct); the display
        // name is only the label, so channels sharing a display name don't merge (review C2).
        var keys = nameByWidget.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys
            .Select((k, idx) => new SeriesKey(k, nameByWidget[k], SeriesPalette[idx % SeriesPalette.Length])).ToArray();

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
            if (link.WidgetId is null || nameByWidget.ContainsKey(link.WidgetId) == false) continue;
            var i = BucketIndex(buckets, link.CreatedAt, granularity);
            if (i < 0) continue;
            points[i][link.WidgetId] = (long)points[i][link.WidgetId] + 1L;
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
        // name + doc count is what the prototype's "Top source tables" renders;
        // lagSeconds/lastWriteAt are CDC-perf data (RavenDB-26780) — left empty.
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

    /// <summary>
    /// Enriched apps list behind the prototype's <c>listApps()</c> (Dashboard table):
    /// per-app counts (documents/agents/channels/tables), CDC <c>source.type</c>, and
    /// a derived <c>status</c>, via fan-out. <c>writesPerMonth</c> is null (no counter).
    /// </summary>
    public static async Task<List<ApplianceAppResponse>> GetDashboardAppsAsync(
        IDocumentStore store, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);
        return await ForEachAppAsync(apps, log, app => EnrichAppAsync(store, app, ct), ct);
    }

    /// <summary>Single enriched app (mock-api <c>getApp(id)</c>), or null if not found.</summary>
    public static async Task<ApplianceAppResponse?> GetDashboardAppAsync(
        IDocumentStore store, string slug, CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        return app is null ? null : await EnrichAppAsync(store, app, ct);
    }

    private static async Task<ApplianceAppResponse> EnrichAppAsync(IDocumentStore store, App app, CancellationToken ct)
    {
        var maintenance = store.Maintenance.ForDatabase(app.Database);
        var stats = await maintenance.SendAsync(new GetStatisticsOperation(), ct);
        var agents = await maintenance.SendAsync(new GetAiAgentsOperation(), ct);
        var agentsCount = agents.AiAgents?.Count ?? 0;

        List<Channel> channels;
        using (var session = store.OpenAsyncSession(app.Database))
            channels = (await session.Advanced.LoadStartingWithAsync<Channel>(
                Channel.IdPrefix, pageSize: 1024, token: ct)).ToList();
        var enabledChannels = channels.Count(c => c.Enabled);
        var channelsLabel = channels.Count == 0
            ? null
            : string.Join(", ", channels.Select(c => ChannelTypeLabel(c.Type)).Distinct());

        // CDC config → tablesCount + source.type (CDC conn string → SQL FactoryName).
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(app.Database), ct);
        var cdc = record?.CdcSinks?.FirstOrDefault();
        var tablesCount = cdc?.Tables?.Count ?? 0;
        var sourceType = "";
        if (cdc?.ConnectionStringName is { } csName)
        {
            var conn = await maintenance.SendAsync(new GetConnectionStringsOperation(), ct);
            if (conn.SqlConnectionStrings is not null && conn.SqlConnectionStrings.TryGetValue(csName, out var sql))
                sourceType = MapSourceType(sql.FactoryName);
        }

        var (status, subtitle) = DeriveAppStatus(agentsCount, channels.Count, enabledChannels, cdc?.Disabled ?? false);

        return new ApplianceAppResponse(
            Id: app.Slug,  // the prototype routes by app.id; slug is the routing key (id==slug)
            Name: app.AppName,
            Slug: app.Slug,
            Status: status,
            Source: new AppSource(sourceType, ConnectionString: ""),
            TablesCount: tablesCount,
            DocumentsCount: stats.CountOfDocuments,
            CapabilitiesCount: agentsCount,
            ChannelsCount: channels.Count,
            AdaptersCount: 0,
            AgentsCount: agentsCount,
            WritesPerMonth: null,
            ChannelsLabel: channelsLabel,
            StatusSubtitle: subtitle,
            CreatedAt: Utc(app.CreatedAt),
            UpdatedAt: Utc(app.CreatedAt));
    }

    private static string MapSourceType(string? factory)
    {
        var f = (factory ?? "").ToLowerInvariant();
        if (f.Contains("npgsql")) return "PostgreSQL";
        if (f.Contains("sqlclient")) return "SQL Server";
        if (f.Contains("mysql")) return "MySQL";
        if (f.Contains("oracle")) return "Oracle";
        return "";
    }

    private static string ChannelTypeLabel(ChannelType type) => type switch
    {
        ChannelType.IFrame => "Web widget",
        _ => type.ToString(),
    };

    private static (string Status, string? Subtitle) DeriveAppStatus(
        int agentsCount, int channelsCount, int enabledChannels, bool cdcDisabled)
    {
        if (agentsCount == 0) return ("setup", "No AI agent yet");
        if (cdcDisabled) return ("warning", "Data sync paused");
        if (channelsCount > 0 && enabledChannels == 0) return ("warning", "All channels disabled");
        return ("running", null);
    }

    public static async Task<DashboardResponse> GetDashboardStatsAsync(
        IDocumentStore store, DateTime nowUtc, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);

        // Read-time fan-out: each app contributes its rows; fold them after. A single
        // bad tenant DB is skipped, not fatal for the whole dashboard (review I2).
        var perApp = await ForEachAppAsync(apps, log, async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            return await QueryMetricRowsAsync(session, nowUtc.AddDays(-30), ct);
        }, ct);

        var last24h = new WindowAccumulator();
        var last7d = new WindowAccumulator();
        var last30d = new WindowAccumulator();
        foreach (var rows in perApp)
            FoldInto(rows, nowUtc, ref last24h, ref last7d, ref last30d);

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

    /// <summary>Per-agent activity from the conversation index: invocations
    /// (conversation count) and last-invoked (latest hour bucket). Returns empty
    /// when the index isn't deployed yet, so callers degrade to zeroes.</summary>
    public static async Task<Dictionary<string, (long Invocations, DateTime? LastInvokedAt)>> GetAgentActivityAsync(
        IDocumentStore store, string database, CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession(database);
            var rows = await QueryAllMetricRowsAsync(session, ct);
            return rows
                .GroupBy(r => r.Agent ?? "")
                .ToDictionary(
                    g => g.Key,
                    // The index Bucket is built with DateTimeKind.Unspecified; mark it UTC
                    // so it serializes with the Z designator (review I1).
                    g => (g.Sum(r => r.Conversations), (DateTime?)Utc(g.Max(r => r.Bucket))),
                    StringComparer.OrdinalIgnoreCase);
        }
        catch (IndexDoesNotExistException)
        {
            return new Dictionary<string, (long, DateTime?)>(StringComparer.OrdinalIgnoreCase);
        }
    }

    /// <summary>Conversations for the Conversations list — shaped from
    /// <c>@conversations</c> docs (id-prefixed <c>chats/</c>), newest activity first,
    /// last-exchange preview only (no full transcript).</summary>
    public static async Task<List<ConversationDto>> GetConversationsAsync(
        IDocumentStore store, string slug, string database, DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var channelByConversation = await BuildConversationChannelMapAsync(session, ct);
        // NOTE (review SF5): single 1024-doc page ordered by id (not LastMessageAt) — for
        // apps with >1024 conversations this truncates and may miss the newest. Index-backed
        // pagination is a deferred follow-up.
        var docs = await session.Advanced.LoadStartingWithAsync<ConversationDoc>(
            ConversationIdPrefix, pageSize: 1024, token: ct);
        return docs
            .Select(d => ShapeConversation(d, slug, nowUtc, includeTranscript: false, channelByConversation))
            .OrderByDescending(c => c.LastActivityAt)
            .ToList();
    }

    /// <summary>One conversation with its full chronological transcript, or null.</summary>
    public static async Task<ConversationDto?> GetConversationAsync(
        IDocumentStore store, string slug, string database, string conversationId, DateTime nowUtc, CancellationToken ct)
    {
        // Only conversation docs are addressable here; reject other ids (e.g. the
        // catch-all capturing "channels/x") so we don't shape a non-conversation
        // doc and return nonsense instead of 404 (review M5).
        if (conversationId.StartsWith(ConversationIdPrefix, StringComparison.Ordinal) == false)
            return null;

        using var session = store.OpenAsyncSession(database);
        var doc = await session.LoadAsync<ConversationDoc>(conversationId, ct);
        if (doc is null) return null;
        var channelByConversation = await BuildConversationChannelMapAsync(session, ct);
        return ShapeConversation(doc, slug, nowUtc, includeTranscript: true, channelByConversation);
    }

    /// <summary>conversationId → channel display-name, for iframe conversations only,
    /// reverse-mapped via <c>EmbedLink.ConversationId</c> → <c>WidgetId</c> →
    /// <c>Channel.DisplayName</c>. Telegram/WhatsApp aren't implemented, so their
    /// conversations have no mapping (channelName stays empty).</summary>
    private static async Task<Dictionary<string, string>> BuildConversationChannelMapAsync(
        IAsyncDocumentSession session, CancellationToken ct)
    {
        var channels = await session.Advanced.LoadStartingWithAsync<Channel>(
            Channel.IdPrefix, pageSize: 1024, token: ct);
        var nameByWidget = channels
            .Where(c => c.Id is not null)
            .ToDictionary(c => c.Id![Channel.IdPrefix.Length..],
                c => string.IsNullOrWhiteSpace(c.DisplayName) ? c.Id![Channel.IdPrefix.Length..] : c.DisplayName,
                StringComparer.OrdinalIgnoreCase);

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (nameByWidget.Count == 0)
            return map;

        var links = await session.Advanced.LoadStartingWithAsync<EmbedLink>(
            EmbedLink.IdPrefix, pageSize: 1024, token: ct);
        foreach (var link in links)
            if (link.ConversationId is { } cid && link.WidgetId is { } wid
                && nameByWidget.TryGetValue(wid, out var name))
                map[cid] = name;
        return map;
    }

    private static ConversationDto ShapeConversation(
        ConversationDoc doc, string slug, DateTime nowUtc, bool includeTranscript, Dictionary<string, string> channelByConversation)
    {
        var agentName = doc.Agent ?? "";
        var channelName = doc.Id is { } id && channelByConversation.TryGetValue(id, out var cn) ? cn : "";
        // Only user/assistant turns are end-user-facing; drop the system prompt and
        // tool/internal scaffolding, and normalize the assistant role to "agent".
        var chrono = (doc.Messages ?? [])
            .Where(m => m.role is "user" or "assistant")
            .Select(m => new ConversationTurn(m.role == "assistant" ? "agent" : "user", TextOf(m.content), Utc(m.date)))
            .OrderBy(t => t.At ?? doc.CreatedAt)
            .ToArray();
        var lastExchange = chrono.TakeLast(2).Reverse().ToArray();  // newest first, at most 2
        var prms = (doc.Parameters ?? new Dictionary<string, object>())
            .Select(kv => new ConversationParam(kv.Key, kv.Value?.ToString() ?? ""))
            .ToArray();

        var age = nowUtc - doc.LastMessageAt;
        var state = age < TimeSpan.FromHours(1) ? "active" : age < TimeSpan.FromHours(24) ? "idle" : "closed";

        return new ConversationDto(
            doc.Id ?? "", slug, channelName, agentName,
            AgentInitials(agentName), AgentColor(agentName),
            prms, lastExchange, includeTranscript ? chrono : null,
            state, Utc(doc.LastMessageAt), Utc(doc.CreatedAt), MaxDuration: null);
    }

    // Message content is a string, an array-of-parts ([{type:"text",text}]), or an
    // object. RavenDB deserializes a JSON array/object into CLR object[] /
    // dictionaries, so handle those directly; a JSON round-trip is the fallback.
    private static string TextOf(object? content)
    {
        switch (content)
        {
            case null: return "";
            case string s: return s;
            case IEnumerable<object> parts: return string.Concat(parts.Select(TextOf));
            case IDictionary<string, object> dict:
                return dict.TryGetValue("text", out var t) ? t?.ToString() ?? "" : "";
        }
        try
        {
            using var json = JsonDocument.Parse(JsonSerializer.Serialize(content));
            return ExtractText(json.RootElement);
        }
        catch (JsonException) { return content.ToString() ?? ""; }
    }

    private static string ExtractText(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString() ?? "",
        JsonValueKind.Array => string.Concat(el.EnumerateArray().Select(ExtractText)),
        JsonValueKind.Object => el.TryGetProperty("text", out var t) && t.ValueKind == JsonValueKind.String
            ? t.GetString() ?? ""
            : "",
        _ => "",
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

    // Normalize an outbound timestamp to UTC so System.Text.Json emits the Z designator
    // (an Unspecified DateTime serializes without it → the browser parses it as local).
    private static DateTime Utc(DateTime d) => d.Kind switch
    {
        DateTimeKind.Utc => d,
        DateTimeKind.Local => d.ToUniversalTime(),
        _ => DateTime.SpecifyKind(d, DateTimeKind.Utc),
    };

    /// <summary>Runs <paramref name="body"/> for each app with bounded concurrency,
    /// isolating per-app failures: a tenant whose DB is missing/offline/compacting is
    /// logged and skipped so one bad tenant can't 500 a global fan-out endpoint
    /// (review I2/I3). Returns only the successful results.</summary>
    private static async Task<List<T>> ForEachAppAsync<T>(
        IReadOnlyList<App> apps, ILogger? log, Func<App, Task<T>> body, CancellationToken ct)
    {
        using var gate = new SemaphoreSlim(MaxFanoutConcurrency);
        var tasks = apps.Select(async app =>
        {
            await gate.WaitAsync(ct);
            try
            {
                return (Ok: true, Value: await body(app));
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                log?.LogWarning(e, "Dashboard fan-out: skipping app {Slug} ({Database})", app.Slug, app.Database);
                return (Ok: false, Value: default(T)!);
            }
            finally
            {
                gate.Release();
            }
        });
        var results = await Task.WhenAll(tasks);
        return results.Where(r => r.Ok).Select(r => r.Value).ToList();
    }

    // All three index reads degrade to empty when the index isn't deployed on an app
    // DB yet (provisioned before this feature, or a brief post-create window) — so the
    // stats endpoints return empty windows instead of HTTP 500 (review SF3).

    /// <summary>Fetches the hour-bucket rows from the widest window (server-side
    /// filter keeps the row count bounded) for client-side folding/grouping.</summary>
    private static async Task<List<ConversationMetricsIndex.Result>> QueryMetricRowsAsync(
        IAsyncDocumentSession session, DateTime since, CancellationToken ct)
    {
        try
        {
            return await session.Advanced
                .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
                .WhereGreaterThanOrEqual(row => row.Bucket, since)
                .ToListAsync(ct);
        }
        catch (IndexDoesNotExistException) { return []; }
    }

    /// <summary>Fetches every hour-bucket row (no time filter) for all-time totals.
    /// Intentionally unbounded: conversations carry an Expires TTL, so the reduced row
    /// count stays small. No <c>.Take()</c> cap — a silent cap would under-count the
    /// all-time aggregates rather than fail loudly (review M8).</summary>
    private static async Task<List<ConversationMetricsIndex.Result>> QueryAllMetricRowsAsync(
        IAsyncDocumentSession session, CancellationToken ct)
    {
        try
        {
            return await session.Advanced
                .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
                .ToListAsync(ct);
        }
        catch (IndexDoesNotExistException) { return []; }
    }

    /// <summary>Fetches hour-bucket rows whose bucket falls within the window:
    /// [start, end] when <paramref name="endInclusive"/> (default), else [start, end).</summary>
    private static async Task<List<ConversationMetricsIndex.Result>> QueryMetricRowsInRangeAsync(
        IAsyncDocumentSession session, DateTime start, DateTime end, CancellationToken ct, bool endInclusive = true)
    {
        try
        {
            var query = session.Advanced
                .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
                .WhereGreaterThanOrEqual(r => r.Bucket, start)
                .AndAlso();
            query = endInclusive
                ? query.WhereLessThanOrEqual(r => r.Bucket, end)
                : query.WhereLessThan(r => r.Bucket, end);
            return await query.ToListAsync(ct);
        }
        catch (IndexDoesNotExistException) { return []; }
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
