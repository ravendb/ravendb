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
using Raven.Quill.Licensing;
using Raven.Quill.Wizard;

namespace Raven.Quill.Metrics;

internal static class MetricsReadService
{
    private const int ChannelPageSize = 1024;

    private const string AppIdPrefix = "apps/";
    private const int AppPageSize = 1024;

    private const string ConversationIdPrefix = "chats/";
    private const int ConversationPageSize = 1024;

    private const int LastExchangePageSize = 10;

    private const int EmbedLinkPageSize = 1024;

    private const int MaxFanoutConcurrency = 8;

    private const string UnknownModel = "unknown";

    private const int TopTablesLimit = 10;

    public static async Task<List<UsagePoint>> GetUsageAsync(
        ILicenseStatsProvider provider,
        IDocumentStore store, int year, int? month, int? day, string? appSlug, ILogger? log, CancellationToken ct)
    {
        var period = new UsagePeriod(year, month, day);
        var buckets = period.Buckets();
        var conversations = new long[buckets.Count];
        var messages = new long[buckets.Count];
        var tokens = new long[buckets.Count];
        var writes = new long[buckets.Count];

        var apps = await AppsToQueryAsync(store, appSlug, ct);
        var perApp = await ForEachAppAsync(apps, log, async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            return await QueryMetricRowsInRangeAsync(session, period.Start, period.End, ct);
        }, ct);

        foreach (var rows in perApp)
            foreach (var row in rows)
            {
                var i = period.IndexOf(row.Bucket);
                if (i < 0) continue;
                conversations[i] += row.Conversations;
                messages[i] += row.Messages;
                tokens[i] += row.Tokens;
            }

        var stats = await provider.GetUsageAsync(year, month, day, ct);
        foreach (var p in stats?.ByPeriod ?? [])
        {
            var i = period.IndexOf(Utc(p.From));
            if (i < 0) continue;
            writes[i] += p.Usage;
        }

        var points = new List<UsagePoint>(buckets.Count);
        for (var i = 0; i < buckets.Count; i++)
            points.Add(new UsagePoint(buckets[i], conversations[i], messages[i], tokens[i], writes[i]));
        return points;
    }

    private static async Task<List<App>> AppsToQueryAsync(IDocumentStore store, string? appSlug, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(appSlug))
            return await LoadAllAppsAsync(store, ct);
        var app = await AppLookup.LoadAppAsync(store, appSlug, ct);
        return app is null ? [] : [app];
    }


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

    public static async Task<AppUsageResponse> GetAppUsageAsync(
        IDocumentStore store, string database, int year, int? month, int? day, CancellationToken ct)
    {
        var maintenance = store.Maintenance.ForDatabase(database);

        // Calendar period selected by year/month/day: [Start, End) is what the buckets cover
        // (a day by hour / a month by day / a year by month); the preceding equal period
        // [PreviousStart, Start) is the baseline for each card's delta.
        var period = new UsagePeriod(year, month, day);

        using var session = store.OpenAsyncSession(database);
        var rows = await QueryMetricRowsInRangeAsync(session, period.Start, period.End, ct);


        var prevRows = await QueryMetricRowsInRangeAsync(session, period.PreviousStart, period.Start, ct);

        long convNow = rows.Sum(r => r.Conversations), tokNow = rows.Sum(r => r.Tokens);
        long convPrev = prevRows.Sum(r => r.Conversations), tokPrev = prevRows.Sum(r => r.Tokens);

        var buckets = period.Buckets();
        var convByBucket = new long[buckets.Count];
        var tokByBucket = new long[buckets.Count];
        foreach (var row in rows)
        {
            var i = period.IndexOf(row.Bucket);
            if (i < 0) continue;
            convByBucket[i] += row.Conversations;
            tokByBucket[i] += row.Tokens;
        }

        var cdcRaw = await CdcPerformanceReader.ReadAsync(maintenance, ct);
        var cdcWrites = BuildCdcWrites(cdcRaw, buckets, period);

        var metrics = new AppUsageMetrics(
            Conversations: new MetricCard(convNow, Delta(convNow, convPrev), ToDoubles(convByBucket)),
            Tokens: new MetricCard(tokNow, Delta(tokNow, tokPrev), ToDoubles(tokByBucket)),
            CdcWrites: new MetricCard(cdcWrites.Sum(p => p.Writes), 0,
                cdcWrites.Select(p => (double)p.Writes).ToArray()));

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

        var tokensByCapability = BuildTokenSeries(rows, buckets, period, agent => agent, NameOf);
        var tokensByModel = BuildTokenSeries(rows, buckets, period,
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
            session, buckets, period, ct);
        var topTables = await BuildTopTablesAsync(maintenance, ct);

        return new AppUsageResponse(
            metrics,
            TokensByCapability: tokensByCapability,
            TokensByModel: tokensByModel,
            ConversationsByChannel: conversationsByChannel,
            CdcWrites: cdcWrites,
            TopTables: topTables,
            TopCapabilities: topCapabilities);
    }

    private const string TimeAxisKey = "t";

    private static Dictionary<string, object> NewBucketPoint(IReadOnlyList<string> seriesKeys, string label)
    {
        var point = new Dictionary<string, object>(seriesKeys.Count + 1);
        foreach (var k in seriesKeys) point[k] = 0L;
        point[TimeAxisKey] = label;
        return point;
    }

    private static SeriesData BuildTokenSeries(
        IReadOnlyList<ConversationMetricsIndex.Result> rows, List<DateTime> buckets,
        UsagePeriod period, Func<string, string> keyOf, Func<string, string>? labelOf = null)
    {
        var label = labelOf ?? (k => k);
        var keys = rows.Select(r => keyOf(r.Agent ?? "")).Distinct()
            .Where(k => k != TimeAxisKey)   // a key colliding with the reserved time axis can't be represented — drop it
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys.Select(k => new SeriesKey(k, label(k))).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
            points[b] = NewBucketPoint(keys, period.Label(buckets[b]));
        foreach (var row in rows)
        {
            var i = period.IndexOf(row.Bucket);
            if (i < 0) continue;
            var key = keyOf(row.Agent ?? "");
            if (key == TimeAxisKey) continue;   // dropped from keys above
            points[i][key] = (long)points[i][key] + row.Tokens;
        }

        return new SeriesData(points, seriesKeys);
    }

    private static async Task<SeriesData> BuildConversationsByChannelAsync(
        IAsyncDocumentSession session, List<DateTime> buckets, UsagePeriod period, CancellationToken ct)
    {
        var channels = await LoadAllByPrefixAsync<Channel>(session, Channel.IdPrefix, ChannelPageSize, ct);
        var nameByWidget = channels
            .Where(c => c.Id is not null)
            .ToDictionary(c => c.Id![Channel.IdPrefix.Length..],
                c => string.IsNullOrWhiteSpace(c.DisplayName) ? c.Id![Channel.IdPrefix.Length..] : c.DisplayName,
                StringComparer.OrdinalIgnoreCase);
        if (nameByWidget.Count == 0)
            return new SeriesData([], []);

        var keys = nameByWidget.Keys
            .Where(k => k != TimeAxisKey)   // a WidgetId colliding with the reserved time axis can't be represented — drop it
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys.Select(k => new SeriesKey(k, nameByWidget[k])).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
            points[b] = NewBucketPoint(keys, period.Label(buckets[b]));

        var links = await LoadAllByPrefixAsync<EmbedLink>(session, EmbedLink.IdPrefix, EmbedLinkPageSize, ct);
        foreach (var link in links)
        {
            if (link.CreatedAt < period.Start || link.CreatedAt >= period.End) continue;
            if (link.WidgetId is null || nameByWidget.ContainsKey(link.WidgetId) == false) continue;
            if (link.WidgetId == TimeAxisKey) continue;   // dropped from keys above
            var i = period.IndexOf(link.CreatedAt);
            if (i < 0) continue;
            points[i][link.WidgetId] = (long)points[i][link.WidgetId] + 1L;
        }

        return new SeriesData(points, seriesKeys);
    }

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

    internal static CdcWritePoint[] BuildCdcWrites(
        CdcSinkPerformanceRaw raw, List<DateTime> buckets, UsagePeriod period)
    {
        var byBucket = new long[buckets.Count];
        foreach (var batch in CdcPerformanceShaper.Batches(raw))
        {
            var i = period.IndexOf(Utc(batch.Completed ?? batch.Started));
            if (i < 0) continue;
            byBucket[i] += batch.NumberOfProcessedMessages;
        }

        var points = new CdcWritePoint[buckets.Count];
        for (var i = 0; i < buckets.Count; i++)
            points[i] = new CdcWritePoint(period.Label(buckets[i]), byBucket[i]);
        return points;
    }

    public static async Task<List<ApplianceAppResponse>> GetDashboardAppsAsync(
        IDocumentStore store, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);
        return await ForEachAppAsync(apps, log, app => EnrichAppAsync(store, app, ct), ct);
    }

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
        IDocumentStore store, int year, int? month, int? day, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);


        var period = new UsagePeriod(year, month, day);


        var perApp = await ForEachAppAsync(apps, log, async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            return await QueryMetricRowsInRangeAsync(session, period.Start, period.End, ct);
        }, ct);

        var rows = perApp.SelectMany(appRows => appRows).ToList();
        return new DashboardResponse(
            apps.Count,
            rows.Sum(r => r.Conversations),
            rows.Sum(r => r.Messages),
            rows.Sum(r => r.Tokens));
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

            if (page.Count < ChannelPageSize) break;
            offset += ChannelPageSize;
        }

        return new ChannelStatsResponse(total, active);
    }

    public static async Task<ConversationStatsResponse> GetConversationStatsAsync(
        IDocumentStore store, string database, int year, int? month, int? day, CancellationToken ct)
    {
        // Calendar period selected by year/month/day (a year / a month / a day); [Start, End)
        // is the span the totals cover, mirroring the usage endpoints.
        var period = new UsagePeriod(year, month, day);

        using var session = store.OpenAsyncSession(database);
        var rows = await QueryMetricRowsInRangeAsync(session, period.Start, period.End, ct);

        return new ConversationStatsResponse(
            rows.Sum(r => r.Conversations),
            rows.Sum(r => r.Messages),
            rows.Sum(r => r.Tokens));
    }

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

    public static async Task<List<ConversationDto>> GetConversationsAsync(
        IDocumentStore store, string slug, string database, DateTime nowUtc, CancellationToken ct)
    {
        Dictionary<string, string> channelByConversation;
        List<ConversationDoc> docs;
        using (var session = store.OpenAsyncSession(database))
        {
            channelByConversation = await BuildConversationChannelMapAsync(session, ct);
            docs = await LoadAllByPrefixAsync<ConversationDoc>(session, ConversationIdPrefix, ConversationPageSize, ct);
        }

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

    public static async Task<ConversationDto?> GetConversationAsync(
        IDocumentStore store, string slug, string database, string conversationId, DateTime nowUtc, CancellationToken ct)
    {
        if (conversationId.StartsWith(ConversationIdPrefix, StringComparison.Ordinal) == false)
            return null;

        var result = await store.AI.ForDatabase(database).GetConversationMessagesAsync(conversationId, ct);
        if (result is null)
            return null;  // 404 — no such conversation

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

        return new ConversationDto(
            doc.Id ?? "", slug, channelName, agentName, AgentInitials(agentName),
            prms, lastExchange, Transcript: null,
            State(nowUtc - doc.LastMessageAt), Utc(doc.LastMessageAt), Utc(doc.CreatedAt), MaxDuration: null);
    }

    private static string State(TimeSpan age) =>
        age < TimeSpan.FromHours(1) ? "active" : age < TimeSpan.FromHours(24) ? "idle" : "closed";

    // FE wire-contract values, not the enum names (nameof would break the contract)
    private static string RoleLabel(AiMessageRole role) => role == AiMessageRole.Assistant ? "agent" : "user";

    internal static ConversationTurn[] MapTranscript(IEnumerable<AiConversationMessage> messages) =>
        messages
            .Where(m => string.IsNullOrWhiteSpace(m.Content) == false)
            .Select(m => new ConversationTurn(RoleLabel(m.Role), ReplyText(m.Content), Utc(m.Timestamp)))
            .ToArray();

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
        catch (JsonException)
        {
            /* not JSON — plain text */
        }

        return content;
    }

    private static string ExtractText(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString() ?? "",
        JsonValueKind.Array => string.Concat(el.EnumerateArray().Select(ExtractText)),
        JsonValueKind.Object => ObjectText(el),
        _ => "",
    };

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

    private sealed class ConversationDoc
    {
        public string? Id { get; set; }
        public string? Agent { get; set; }
        public Dictionary<string, object>? Parameters { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime LastMessageAt { get; set; }
    }

    private static async Task<List<App>> LoadAllAppsAsync(IDocumentStore store, CancellationToken ct)
    {
        using var configSession = store.OpenAsyncSession();
        return await LoadAllByPrefixAsync<App>(configSession, AppIdPrefix, AppPageSize, ct);
    }

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

    private static DateTime Utc(DateTime d) => d.Kind switch
    {
        DateTimeKind.Utc => d,
        DateTimeKind.Local => d.ToUniversalTime(),
        _ => DateTime.SpecifyKind(d, DateTimeKind.Utc),
    };

    // isolate per-app failures: one bad tenant DB can't 500 a global fan-out
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

    private static async Task<List<ConversationMetricsIndex.Result>> QueryMetricRowsInRangeAsync(
        IAsyncDocumentSession session, DateTime start, DateTime end, CancellationToken ct)
    {
        try
        {
            return await session.Advanced
                .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
                .WhereGreaterThanOrEqual(r => r.Bucket, start)
                .AndAlso()
                .WhereLessThan(r => r.Bucket, end)
                .ToListAsync(ct);
        }
        catch (IndexDoesNotExistException) { return []; }
    }

}


internal sealed record AgentActivity(long Conversations, long Messages, long Tokens, DateTime? LastInvokedAt);
