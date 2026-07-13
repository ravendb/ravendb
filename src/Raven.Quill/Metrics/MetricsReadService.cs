using System.Text.Json;
using Microsoft.Extensions.Logging;
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
using Raven.Quill.Cdc;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Wizard;

namespace Raven.Quill.Metrics;

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
    private const int ConversationPageSize = 1024;

    // The conversations-list preview needs only the last exchange, so read a small most-recent tail
    // per row instead of the whole history. Backward paging returns the newest N (oldest-first), and
    // DetailLevel.Simple already excludes system/tool turns, so 10 comfortably covers the 2 the preview
    // keeps even after MapTranscript drops any blank/attachment-only turn.
    private const int LastExchangePageSize = 10;

    // Embed links (iframe channel attribution) are id-prefixed; read by prefix.
    private const int EmbedLinkPageSize = 1024;

    // Per-app fan-out (dashboard/usage) runs bounded-parallel and isolates failures.
    private const int MaxFanoutConcurrency = 8;

    // Series key for tokens whose agent has no resolvable connection-string model.
    private const string UnknownModel = "unknown";

    // Cap on the "top tables" (collections) list returned for the App Usage page.
    private const int TopTablesLimit = 10;

    /// <summary>
    /// Usage series behind <c>GET /api/usage?time=&amp;app=</c>: one <see cref="UsagePoint"/>
    /// per bucket over the window (24h → 24 hourly, 7d → 7 daily, 30d → 30 daily),
    /// contiguous + zero-filled, carrying conversations / messages / tokens. With
    /// <paramref name="appSlug"/> it scopes to that app; otherwise it sums across all apps.
    /// </summary>
    public static async Task<List<UsagePoint>> GetUsageAsync(
        IDocumentStore store, UsageWindow window, string? appSlug, DateTime nowUtc, ILogger? log, CancellationToken ct)
    {
        var (granularity, start, end) = WindowRange(window, nowUtc);
        var buckets = BuildBuckets(start, end, granularity);
        var conversations = new long[buckets.Count];
        var messages = new long[buckets.Count];
        var tokens = new long[buckets.Count];
        var writes = new long[buckets.Count];

        var apps = await AppsToQueryAsync(store, appSlug, ct);
        var perApp = await ForEachAppAsync(apps, log, async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            return await QueryMetricRowsAsync(session, start, ct);
        }, ct);

        foreach (var rows in perApp)
            foreach (var row in rows)
            {
                var i = BucketIndex(buckets, row.Bucket, granularity);
                if (i < 0) continue;
                conversations[i] += row.Conversations;
                messages[i] += row.Messages;
                tokens[i] += row.Tokens;
            }

        // Writes have no real per-DB counter yet (RavenDB-26780): fill the series with a
        // deterministic per-app mock so the dashboards render plausible writes. Each app
        // contributes independently, so a global series equals the sum of its apps. Swap
        // this loop for the real counter when it lands.
        foreach (var app in apps)
            for (var i = 0; i < buckets.Count; i++)
                writes[i] += MockWritesForBucket(app, buckets[i], granularity);

        var points = new List<UsagePoint>(buckets.Count);
        for (var i = 0; i < buckets.Count; i++)
            points.Add(new UsagePoint(buckets[i], conversations[i], messages[i], tokens[i], writes[i]));
        return points;
    }

    private static (UsageGranularity Granularity, DateTime Start, DateTime End) WindowRange(UsageWindow window, DateTime nowUtc)
    {
        var hour = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Utc);
        var day = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, 0, 0, 0, DateTimeKind.Utc);
        return window switch
        {
            UsageWindow.Last7d => (UsageGranularity.Day, day.AddDays(-6), day),
            UsageWindow.Last30d => (UsageGranularity.Day, day.AddDays(-29), day),
            _ => (UsageGranularity.Hour, hour.AddHours(-23), hour),
        };
    }

    private static async Task<List<App>> AppsToQueryAsync(IDocumentStore store, string? appSlug, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(appSlug))
            return await LoadAllAppsAsync(store, ct);
        var app = await AppLookup.LoadAppAsync(store, appSlug, ct);
        return app is null ? [] : [app];
    }

    /// <summary>
    /// Deterministic stand-in for a per-app, per-bucket write count until a real per-DB
    /// write counter lands (RavenDB-26780). Pure in (app, bucket, granularity) so a global
    /// series equals the sum of its apps and values stay stable across requests — hence
    /// FNV-1a over the database name, not the per-process-randomized string hash.
    /// </summary>
    private static long MockWritesForBucket(App app, DateTime bucket, UsageGranularity granularity)
    {
        var perHour = 400 + StableHash(app.Database) % 800;            // ~[400, 1200) writes/hour, stable per app
        var phase = granularity == UsageGranularity.Hour ? bucket.Hour : bucket.Day;
        var wave = 0.55 + 0.45 * Math.Sin(phase / 24.0 * 2 * Math.PI); // shape so the sparkline isn't flat
        var hoursInBucket = granularity == UsageGranularity.Hour ? 1 : 24;
        return (long)Math.Round(perHour * hoursInBucket * wave);
    }

    private static uint StableHash(string value)
    {
        // FNV-1a — deterministic across processes (string.GetHashCode is randomized).
        var hash = 2166136261u;
        foreach (var c in value)
            hash = (hash ^ c) * 16777619u;
        return hash;
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
    /// subset from <see cref="ConversationMetricsIndex"/>: conversations/tokens
    /// KPI cards (value + delta-vs-previous-window + per-bucket sparkline),
    /// <c>tokensByCapability</c>, and <c>topCapabilities</c>. <c>cdcWrites</c> and the CDC
    /// KPI card are bucketed from RavenDB's rolling per-batch perf window (recent activity,
    /// not full history — see the loop below); <c>topTables</c> stands in with collection
    /// stats (RavenDB-26780).
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

        // CDC writes: fold RavenDB's rolling per-batch perf window into the usage buckets
        // (see BuildCdcWrites). Recent snapshot, not history — buckets before the window stay
        // zero and the KPI card carries no delta (no reliable previous-window baseline).
        var cdcRaw = await CdcPerformanceReader.ReadAsync(maintenance, ct);
        var cdcWrites = BuildCdcWrites(cdcRaw, buckets, granularity);

        var metrics = new AppUsageMetrics(
            Conversations: new MetricCard(convNow, Delta(convNow, convPrev), ToDoubles(convByBucket)),
            Tokens: new MetricCard(tokNow, Delta(tokNow, tokPrev), ToDoubles(tokByBucket)),
            // Delta 0: the rolling window can't reach the previous window for a real baseline.
            CdcWrites: new MetricCard(cdcWrites.Sum(p => p.Writes), 0,
                cdcWrites.Select(p => (double)p.Writes).ToArray()));

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
                    total);
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
            CdcWrites: cdcWrites,
            TopTables: topTables,
            TopCapabilities: topCapabilities);
    }

    // Reserved time-axis key in each series point ({ "t": label, <seriesKey>: number }).
    // A series key colliding with it can't be represented twice in one dict, so such keys
    // are dropped from the series (see the BuildTokenSeries / BuildConversationsByChannel filters).
    private const string TimeAxisKey = "t";

    // Builds a zero-filled bucket point. Series zeros first, then the time label last so it
    // can't be clobbered; callers must have already filtered TimeAxisKey out of seriesKeys.
    private static Dictionary<string, object> NewBucketPoint(IReadOnlyList<string> seriesKeys, string label)
    {
        var point = new Dictionary<string, object>(seriesKeys.Count + 1);
        foreach (var k in seriesKeys) point[k] = 0L;
        point[TimeAxisKey] = label;
        return point;
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
            .Where(k => k != TimeAxisKey)   // a key colliding with the reserved time axis can't be represented — drop it
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys.Select(k => new SeriesKey(k, label(k))).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
            points[b] = NewBucketPoint(keys, BucketLabel(buckets[b], granularity));
        foreach (var row in rows)
        {
            var i = BucketIndex(buckets, row.Bucket, granularity);
            if (i < 0) continue;
            var key = keyOf(row.Agent ?? "");
            if (key == TimeAxisKey) continue;   // dropped from keys above
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
        var channels = await LoadAllByPrefixAsync<Channel>(session, Channel.IdPrefix, ChannelPageSize, ct);
        var nameByWidget = channels
            .Where(c => c.Id is not null)
            .ToDictionary(c => c.Id![Channel.IdPrefix.Length..],
                c => string.IsNullOrWhiteSpace(c.DisplayName) ? c.Id![Channel.IdPrefix.Length..] : c.DisplayName,
                StringComparer.OrdinalIgnoreCase);
        if (nameByWidget.Count == 0)
            return new SeriesData([], []);

        // Key by the stable WidgetId (dictionary keys are already distinct); the display
        // name is only the label, so channels sharing a display name don't merge (review C2).
        var keys = nameByWidget.Keys
            .Where(k => k != TimeAxisKey)   // a WidgetId colliding with the reserved time axis can't be represented — drop it
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys.Select(k => new SeriesKey(k, nameByWidget[k])).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
            points[b] = NewBucketPoint(keys, BucketLabel(buckets[b], granularity));

        var links = await LoadAllByPrefixAsync<EmbedLink>(session, EmbedLink.IdPrefix, EmbedLinkPageSize, ct);
        foreach (var link in links)
        {
            if (link.CreatedAt < startUtc || link.CreatedAt > endUtc) continue;
            if (link.WidgetId is null || nameByWidget.ContainsKey(link.WidgetId) == false) continue;
            if (link.WidgetId == TimeAxisKey) continue;   // dropped from keys above
            var i = BucketIndex(buckets, link.CreatedAt, granularity);
            if (i < 0) continue;
            points[i][link.WidgetId] = (long)points[i][link.WidgetId] + 1L;
        }
        return new SeriesData(points, seriesKeys);
    }

    /// <summary>"Top tables" = the app's business collections by document count
    /// (collection stats stand in for CDC source-table stats until RavenDB-26780).
    /// TODO RavenDB-26992: <c>lagSeconds</c>/<c>lastWriteAt</c> ship as placeholders
    /// (0 / "") until real replication-lag / last-write per table is implemented.</summary>
    private static async Task<TopTable[]> BuildTopTablesAsync(
        MaintenanceOperationExecutor maintenance, CancellationToken ct)
    {
        var stats = await maintenance.SendAsync(new GetCollectionStatisticsOperation(), ct);
        // name + doc count is what the "Top source tables" view renders.
        return stats.Collections
            .Where(c => c.Key.StartsWith('@') == false)
            .OrderByDescending(c => c.Value)
            .ThenBy(c => c.Key, StringComparer.OrdinalIgnoreCase)
            .Take(TopTablesLimit)
            // TODO RavenDB-26992: LagSeconds/LastWriteAt are placeholders pending real per-table CDC metrics.
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

    /// <summary>Folds the CDC sink's rolling per-batch perf window into a per-bucket writes
    /// series: each batch's <c>NumberOfProcessedMessages</c> (docs written into RavenDB) is
    /// attributed to the bucket of its completion (or start, if still running). Pure so it can
    /// be unit-tested without a live CDC source. Batches outside <paramref name="buckets"/>
    /// (older than the rolling window, or a future clock skew) are ignored, so pre-window
    /// buckets stay zero — this is recent activity, not a historical series.</summary>
    internal static CdcWritePoint[] BuildCdcWrites(
        CdcSinkPerformanceRaw raw, List<DateTime> buckets, UsageGranularity granularity)
    {
        var byBucket = new long[buckets.Count];
        foreach (var batch in CdcPerformanceShaper.Batches(raw))
        {
            var i = BucketIndex(buckets, Utc(batch.Completed ?? batch.Started), granularity);
            if (i < 0) continue;
            byBucket[i] += batch.NumberOfProcessedMessages;
        }
        var points = new CdcWritePoint[buckets.Count];
        for (var i = 0; i < buckets.Count; i++)
            points[i] = new CdcWritePoint(BucketLabel(buckets[i], granularity), byBucket[i]);
        return points;
    }

    private static TimeSpan Step(UsageGranularity granularity) =>
        granularity == UsageGranularity.Hour ? TimeSpan.FromHours(1) : TimeSpan.FromDays(1);

    internal static List<DateTime> BuildBuckets(DateTime start, DateTime end, UsageGranularity granularity)
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
            channels = await LoadAllByPrefixAsync<Channel>(session, Channel.IdPrefix, ChannelPageSize, ct);
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
            // App carries no update timestamp yet, so updatedAt mirrors createdAt (review B4).
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

    /// <summary>Per-agent rollup from the conversation index: conversations, user
    /// messages, tokens, and last-invoked (latest hour bucket). Returns empty when the
    /// index isn't deployed yet, so callers degrade to zeroes.</summary>
    public static async Task<Dictionary<string, AgentActivity>> GetAgentActivityAsync(
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
                    // The index Bucket is DateTimeKind.Utc, but a round-trip through the index
                    // may not preserve Kind, so normalize to UTC to keep the Z designator (review I1).
                    g => new AgentActivity(
                        g.Sum(r => r.Conversations), g.Sum(r => r.Messages), g.Sum(r => r.Tokens),
                        Utc(g.Max(r => r.Bucket))),
                    StringComparer.OrdinalIgnoreCase);
        }
        catch (IndexDoesNotExistException)
        {
            return new Dictionary<string, AgentActivity>(StringComparer.OrdinalIgnoreCase);
        }
    }

    /// <summary>Conversations for the Conversations list (agent, channel, state, timestamps,
    /// params, and a last-exchange preview), newest activity first. The last exchange is read
    /// per row through the AI messages endpoint (bounded fan-out); the full transcript stays
    /// detail-only.</summary>
    public static async Task<List<ConversationDto>> GetConversationsAsync(
        IDocumentStore store, string slug, string database, DateTime nowUtc, CancellationToken ct)
    {
        Dictionary<string, string> channelByConversation;
        List<ConversationDoc> docs;
        using (var session = store.OpenAsyncSession(database))
        {
            channelByConversation = await BuildConversationChannelMapAsync(session, ct);
            // Loads every conversation doc (paged) and sorts by last activity in memory — no
            // truncation. Index-backed *server-side* pagination is a deferred perf follow-up
            // for very large apps (review SF5).
            docs = await LoadAllByPrefixAsync<ConversationDoc>(session, ConversationIdPrefix, ConversationPageSize, ct);
        }

        // Each row carries its last exchange (newest two turns), read through the AI runtime —
        // one read per conversation, bounded-parallel. Server-side pagination would cap this
        // fan-out; until then it mirrors the whole (already in-memory) conversation set.
        using var gate = new SemaphoreSlim(MaxFanoutConcurrency);
        var items = await Task.WhenAll(docs.Select(async doc =>
        {
            var lastExchange = doc.Id is { } id
                ? await LoadLastExchangeAsync(store, database, id, gate, ct)
                : [];
            return ShapeListItem(doc, slug, nowUtc, channelByConversation, lastExchange);
        }));

        return items.OrderByDescending(c => c.LastActivityAt).ToList();
    }

    /// <summary>Reads the newest two turns of a conversation through the AI runtime, bounded by
    /// <paramref name="gate"/>. A single unreadable conversation degrades to an empty exchange
    /// rather than failing the whole list.</summary>
    private static async Task<ConversationTurn[]> LoadLastExchangeAsync(
        IDocumentStore store, string database, string conversationId, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        try
        {
            var options = new GetConversationMessagesOptions { ConversationId = conversationId, PageSize = LastExchangePageSize };
            var result = await store.AI.ForDatabase(database).GetConversationMessagesAsync(options, ct);
            return result is null ? [] : MapTranscript(result.Messages).TakeLast(2).Reverse().ToArray();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return [];
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>One conversation with its full chronological transcript, or null. The AI
    /// runtime owns the on-disk message format, so we read it through its own
    /// <c>GetConversationMessages</c> operation (joins multi-part text, and at
    /// <see cref="AiConversationDetailLevel.Simple"/> returns only user + assistant-with-content)
    /// instead of re-parsing the raw doc.</summary>
    public static async Task<ConversationDto?> GetConversationAsync(
        IDocumentStore store, string slug, string database, string conversationId, DateTime nowUtc, CancellationToken ct)
    {
        // Only conversation docs are addressable here; reject other ids (e.g. the
        // catch-all capturing "channels/x") so we don't read a non-conversation
        // doc and return nonsense instead of 404 (review M5).
        if (conversationId.StartsWith(ConversationIdPrefix, StringComparison.Ordinal) == false)
            return null;

        var result = await store.AI.ForDatabase(database).GetConversationMessagesAsync(conversationId, ct);
        if (result is null)
            return null;  // 404 — no such conversation

        // Messages arrive oldest-first; MapTranscript drops blank/attachment-only turns and
        // normalizes role + reply text without re-ordering.
        var transcript = MapTranscript(result.Messages);

        using var session = store.OpenAsyncSession(database);
        var channelName = (await BuildConversationChannelMapAsync(session, ct))
            .GetValueOrDefault(conversationId, "");

        var agentName = result.Agent ?? "";
        var prms = (result.Parameters ?? new Dictionary<string, object>())
            .Select(kv => new ConversationParam(kv.Key, kv.Value?.ToString() ?? ""))
            .ToArray();
        var lastExchange = transcript.TakeLast(2).Reverse().ToArray();  // newest first, at most 2
        var startedAt = result.Messages.Count > 0 ? Utc(result.Messages[0].Timestamp) : Utc(result.LastMessageAt);

        return new ConversationDto(
            conversationId, slug, channelName, agentName, AgentInitials(agentName),
            prms, lastExchange, transcript, State(nowUtc - result.LastMessageAt),
            Utc(result.LastMessageAt), startedAt, MaxDuration: null);
    }

    /// <summary>conversationId → channel display-name, for iframe conversations only,
    /// reverse-mapped via <c>EmbedLink.ConversationId</c> → <c>WidgetId</c> →
    /// <c>Channel.DisplayName</c>. Telegram/WhatsApp aren't implemented, so their
    /// conversations have no mapping (channelName stays empty).</summary>
    private static async Task<Dictionary<string, string>> BuildConversationChannelMapAsync(
        IAsyncDocumentSession session, CancellationToken ct)
    {
        var channels = await LoadAllByPrefixAsync<Channel>(session, Channel.IdPrefix, ChannelPageSize, ct);
        var nameByWidget = channels
            .Where(c => c.Id is not null)
            .ToDictionary(c => c.Id![Channel.IdPrefix.Length..],
                c => string.IsNullOrWhiteSpace(c.DisplayName) ? c.Id![Channel.IdPrefix.Length..] : c.DisplayName,
                StringComparer.OrdinalIgnoreCase);

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (nameByWidget.Count == 0)
            return map;

        var links = await LoadAllByPrefixAsync<EmbedLink>(session, EmbedLink.IdPrefix, EmbedLinkPageSize, ct);
        foreach (var link in links)
            if (link.ConversationId is { } cid && link.WidgetId is { } wid
                && nameByWidget.TryGetValue(wid, out var name))
                map[cid] = name;
        return map;
    }

    private static ConversationDto ShapeListItem(
        ConversationDoc doc, string slug, DateTime nowUtc, Dictionary<string, string> channelByConversation,
        ConversationTurn[] lastExchange)
    {
        var agentName = doc.Agent ?? "";
        var channelName = doc.Id is { } id && channelByConversation.TryGetValue(id, out var cn) ? cn : "";
        var prms = (doc.Parameters ?? new Dictionary<string, object>())
            .Select(kv => new ConversationParam(kv.Key, kv.Value?.ToString() ?? ""))
            .ToArray();

        // The full transcript stays detail-only; the list carries just the last-exchange preview.
        return new ConversationDto(
            doc.Id ?? "", slug, channelName, agentName, AgentInitials(agentName),
            prms, lastExchange, Transcript: null,
            State(nowUtc - doc.LastMessageAt), Utc(doc.LastMessageAt), Utc(doc.CreatedAt), MaxDuration: null);
    }

    private static string State(TimeSpan age) =>
        age < TimeSpan.FromHours(1) ? "active" : age < TimeSpan.FromHours(24) ? "idle" : "closed";

    // The UI labels the assistant "agent"; these are the FE wire-contract values, not the enum
    // names (nameof would yield "Assistant"/"User" and break the contract + the FE).
    private static string RoleLabel(AiMessageRole role) => role == AiMessageRole.Assistant ? "agent" : "user";

    // Shared transcript projection: drop attachment-only/blank turns, normalize the role, and
    // extract the reply text; DetailLevel.Simple already excludes system/tool + assistant-without-
    // content, so chronological order is preserved. Shared by the conversation detail, the list's
    // last-exchange preview, and the embed page's history.
    internal static ConversationTurn[] MapTranscript(IEnumerable<AiConversationMessage> messages) =>
        messages
            .Where(m => string.IsNullOrWhiteSpace(m.Content) == false)
            .Select(m => new ConversationTurn(RoleLabel(m.Role), ReplyText(m.Content), Utc(m.Timestamp)))
            .ToArray();

    // AiConversationMessage.Content is already a string: plain text for user / plain-assistant
    // turns, or the JSON of a structured reply ({"reply":…}) for schema-output agents. For the
    // latter, surface the reply text; plain prose and scalars pass through unchanged.
    private static string ReplyText(string content)
    {
        try
        {
            using var json = JsonDocument.Parse(content);
            if (json.RootElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
            {
                var text = ExtractText(json.RootElement);
                return string.IsNullOrEmpty(text) ? content : text;
            }
        }
        catch (JsonException) { /* not JSON — plain text */ }
        return content;
    }

    private static string ExtractText(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString() ?? "",
        JsonValueKind.Array => string.Concat(el.EnumerateArray().Select(ExtractText)),
        JsonValueKind.Object => ObjectText(el),
        _ => "",
    };

    // A content object is either an array-of-parts element ({type,text}) or the agent's
    // structured reply ({reply:…} or a custom single-field reply). Prefer "text", then
    // "reply", then the first non-empty string property so any reply shape surfaces.
    private static string ObjectText(JsonElement o)
    {
        if (o.TryGetProperty("text", out var t) && t.ValueKind == JsonValueKind.String)
            return t.GetString() ?? "";
        if (o.TryGetProperty("reply", out var r) && r.ValueKind == JsonValueKind.String)
            return r.GetString() ?? "";
        foreach (var p in o.EnumerateObject())
            if (p.Value.ValueKind == JsonValueKind.String && p.Value.GetString() is { Length: > 0 } s)
                return s;
        return "";
    }

    private static string AgentInitials(string name)
    {
        var parts = name.Split([' ', '-', '_'], StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return "?";
        if (parts.Length == 1) return parts[0][..Math.Min(2, parts[0].Length)].ToUpperInvariant();
        return $"{parts[0][0]}{parts[1][0]}".ToUpperInvariant();
    }

    // List-row projection of an @conversations doc — metadata only (no Messages; the
    // transcript is read on detail via the AI GetConversationMessages operation).
    private sealed class ConversationDoc
    {
        public string? Id { get; set; }
        public string? Agent { get; set; }
        public Dictionary<string, object>? Parameters { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime LastMessageAt { get; set; }
    }

    /// <summary>Loads every registered app from the config DB by id prefix, paging
    /// so installations with more than one page are fully returned.</summary>
    private static async Task<List<App>> LoadAllAppsAsync(IDocumentStore store, CancellationToken ct)
    {
        using var configSession = store.OpenAsyncSession();
        return await LoadAllByPrefixAsync<App>(configSession, AppIdPrefix, AppPageSize, ct);
    }

    /// <summary>Loads every document whose id starts with <paramref name="prefix"/>, paging
    /// until a partial page so result sets larger than one page are fully returned. A single
    /// <c>LoadStartingWithAsync</c> call silently truncates at <paramref name="pageSize"/> —
    /// every prefix-load in this service goes through here so that never happens.</summary>
    private static async Task<List<T>> LoadAllByPrefixAsync<T>(
        IAsyncDocumentSession session, string prefix, int pageSize, CancellationToken ct) where T : class
    {
        var all = new List<T>();
        var offset = 0;
        while (true)
        {
            var page = (await session.Advanced.LoadStartingWithAsync<T>(
                prefix, start: offset, pageSize: pageSize, token: ct)).ToList();
            all.AddRange(page);
            if (page.Count < pageSize) break;
            offset += pageSize;
        }
        return all;
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

/// <summary>The usage-series time window — maps to bucket granularity + count.</summary>
public enum UsageWindow { Last24h, Last7d, Last30d }

/// <summary>Per-agent rollup from the conversation index for the merged Agents table.</summary>
internal sealed record AgentActivity(long Conversations, long Messages, long Tokens, DateTime? LastInvokedAt);
