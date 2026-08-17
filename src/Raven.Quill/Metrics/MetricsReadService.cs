using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Licensing;
using Raven.Quill.Raven;
using Raven.Quill.Wizard;

namespace Raven.Quill.Metrics;

internal static class MetricsReadService
{
    private const int ChannelPageSize = 1024;

    private const string AppIdPrefix = "apps/";

    private const string ConversationIdPrefix = "chats/";

    private const int EmbedLinkPageSize = 1024;

    private const string UnknownModel = "unknown";

    public static async Task<UsageResponse> GetUsageAsync(
        ILicenseStatsProvider provider,
        IDocumentStore store, List<App> apps, int year, int? month, int? day, ILogger? log, CancellationToken ct)
    {
        var period = new UsagePeriod(year, month, day);

        var results = await Task.WhenAll(apps.Select(async app =>
        {
            var usage = await GetAppUsageAsync(store, app, period, ct);
            return (Usage: usage, App: app);
        }));

        var buckets = period.Buckets();
        var conversations = new long[buckets.Count];
        var messages = new long[buckets.Count];
        var tokens = new long[buckets.Count];
        var writes = new long[buckets.Count];

        var stats = await provider.GetUsageAsync(year, month, day, ct);
        var statsPerApp = stats.PerApplication.GroupBy(p => p.TopologyId)
            .ToDictionary(g => g.Key, g => g.ToList());

        var writesByApp = new List<AppWrites>(results.Length);
        foreach (var result in results)
        {
            var usage = result.Usage;
            for (int i = 0; i < buckets.Count; i++)
            {
                conversations[i] += usage.Conversations[i];
                messages[i] += usage.Messages[i];
                tokens[i] += usage.Tokens[i];
            }

            long appWrites = 0;
            if (statsPerApp.TryGetValue(result.App.TopologyId, out var appWriteUsage))
            {
                foreach (var p in appWriteUsage)
                {
                    var i = period.IndexOf(Utc(p.From));
                    if (i < 0)
                        continue;
                    writes[i] += p.Usage;
                    appWrites += p.Usage;
                }
            }

            writesByApp.Add(new AppWrites(result.App.Slug, appWrites));
        }

        var points = new List<UsagePoint>(buckets.Count);
        for (var i = 0; i < buckets.Count; i++)
            points.Add(new UsagePoint(buckets[i], conversations[i], messages[i], tokens[i], writes[i]));
        return new UsageResponse(points, writesByApp);
    }

    private static async Task<(long[] Conversations, long[] Messages, long[] Tokens)> GetAppUsageAsync(IDocumentStore store, App app, UsagePeriod period, CancellationToken ct)
    {
        var buckets = period.Buckets();
        var conversations = new long[buckets.Count];
        var messages = new long[buckets.Count];
        var tokens = new long[buckets.Count];

        ct.ThrowIfCancellationRequested();

        try
        {
            using var session = store.OpenAsyncSession(app.Database);
            var rows = await QueryMetricRowsInRangeAsync(session, period.Start, period.End, ct);
            foreach (var row in rows)
            {
                var i = period.IndexOf(row.Bucket);
                if (i < 0)
                    continue;
                conversations[i] += row.Conversations;
                messages[i] += row.Messages;
                tokens[i] += row.Tokens;
            }
        }
        catch
        {
            // do nothing
        }

        return (conversations, messages, tokens);
    }

    public static async Task<TokensByAppResponse> GetTokensByAppAsync(
        IDocumentStore store, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);
        var results = await Task.WhenAll(apps.Select(async app =>
        {
            using var session = store.OpenAsyncSession(app.Database);
            var metricRows = await QueryAllMetricRowsAsync(session, ct);
            return new AppTokens(app.Slug, metricRows.Sum(r => r.Tokens));
        }));
        
        var sorted = results
            .OrderByDescending(a => a.Tokens)
            .ThenBy(a => a.Slug, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return new TokensByAppResponse(sorted, RefreshedMinutesAgo: 0);
    }

    public static async Task<AppUsageResponse> GetAppUsageAsync(
        IDocumentStore store, string database, int year, int? month, int? day, CancellationToken ct)
    {
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

        var metrics = new AppUsageMetrics(
            Conversations: new MetricCard(convNow, Delta(convNow, convPrev), ToDoubles(convByBucket)),
            Tokens: new MetricCard(tokNow, Delta(tokNow, tokPrev), ToDoubles(tokByBucket)));

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database), ct);
        
        var modelByConnectionString = await ModelByConnectionStringAsync(store, database, ct);

        var modelByAgent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var nameByAgent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var agent in record.AiAgents ?? [])
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

        return new AppUsageResponse(
            metrics,
            TokensByCapability: tokensByCapability,
            TokensByModel: tokensByModel,
            ConversationsByChannel: conversationsByChannel,
            TopCapabilities: topCapabilities);
    }

    public static async Task<Dictionary<string, string>> ModelByConnectionStringAsync(IDocumentStore store, string slug, CancellationToken ct)
    {
        var connectionStrings = await store.Maintenance.ForDatabase(slug).SendAsync(new GetConnectionStringsOperation(), ct);
        return connectionStrings.AiConnectionStrings.ToDictionary(cs => cs.Key, cs => AiConnectionStringModel.Resolve(cs.Value), StringComparer.OrdinalIgnoreCase)!;
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
        // one round-trip: channels + embed-links batched as lazy loads
        var lazyChannels = session.Advanced.Lazily.LoadStartingWithAsync<Channel>(
            Channel.IdPrefix, pageSize: ChannelPageSize, token: ct);
        var lazyLinks = session.Advanced.Lazily.LoadStartingWithAsync<EmbedLink>(
            EmbedLink.IdPrefix, pageSize: EmbedLinkPageSize, token: ct);
        await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync(ct);

        var channels = (await lazyChannels.Value).Values;
        var nameByChannel = channels
            .Where(c => c.Id is not null)
            .ToDictionary(c => c.Id![Channel.IdPrefix.Length..],
                c => string.IsNullOrWhiteSpace(c.DisplayName) ? c.Id![Channel.IdPrefix.Length..] : c.DisplayName,
                StringComparer.OrdinalIgnoreCase);
        if (nameByChannel.Count == 0)
            return new SeriesData([], []);

        var keys = nameByChannel.Keys
            .Where(k => k != TimeAxisKey)   // a channel id colliding with the reserved time axis can't be represented — drop it
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase).ToArray();
        var seriesKeys = keys.Select(k => new SeriesKey(k, nameByChannel[k])).ToArray();

        var points = new Dictionary<string, object>[buckets.Count];
        for (var b = 0; b < buckets.Count; b++)
            points[b] = NewBucketPoint(keys, period.Label(buckets[b]));

        foreach (var link in (await lazyLinks.Value).Values)
        {
            if (link.CreatedAt < period.Start || link.CreatedAt >= period.End) continue;
            if (link.ChannelId is null || nameByChannel.ContainsKey(link.ChannelId) == false) continue;
            if (link.ChannelId == TimeAxisKey) continue;   // dropped from keys above
            var i = period.IndexOf(link.CreatedAt);
            if (i < 0) continue;
            points[i][link.ChannelId] = (long)points[i][link.ChannelId] + 1L;
        }

        return new SeriesData(points, seriesKeys);
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

    public static async Task<List<ApplianceAppResponse>> GetDashboardAppsAsync(
        IDocumentStore store, ILogger? log, CancellationToken ct)
    {
        var apps = await LoadAllAppsAsync(store, ct);

        return (await Task.WhenAll(apps.Select(async app =>
        {
            try
            {
                return await EnrichAppAsync(store, app, ct);
            }
            catch
            {
                return new(
                    Id: app.Slug,
                    Name: app.AppName,
                    Slug: app.Slug,
                    Status: "unavailable",
                    Source: new AppSource(Type: "", ConnectionString: ""),
                    TablesCount: 0,
                    DocumentsCount: 0,
                    CapabilitiesCount: 0,
                    ChannelsCount: 0,
                    AdaptersCount: 0,
                    AgentsCount: 0,
                    ChannelsLabel: null,
                    StatusSubtitle: "Database unavailable",
                    CreatedAt: Utc(app.CreatedAt),
                    UpdatedAt: Utc(app.CreatedAt));
            }
        }))).ToList();
    }

    public static async Task<ApplianceAppResponse?> GetDashboardAppAsync(
        IDocumentStore store, string slug, CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        return app is null ? null : await EnrichAppAsync(store, app, ct);
    }

    private static async Task<ApplianceAppResponse> EnrichAppAsync(IDocumentStore store, App app, CancellationToken ct)
    {
        var stats = await store.Maintenance.ForDatabase(app.Database).SendAsync(new GetStatisticsOperation(), ct);

        List<Channel> channels;
        using (var session = store.OpenAsyncSession(app.Database))
            channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);
        var enabledChannels = channels.Count(c => c.Enabled);
        var channelsLabel = channels.Count == 0
            ? null
            : string.Join(", ", channels.Select(c => ChannelTypeLabel(c.Type)).Distinct());

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(app.Database), ct);
        var cdc = record.CdcSinks?.FirstOrDefault();
        var tablesCount = cdc?.Tables?.Count ?? 0;
        var sourceType = "";
        if (cdc?.ConnectionStringName is { } csName)
        {
            if (record.SqlConnectionStrings is not null && record.SqlConnectionStrings.TryGetValue(csName, out var sql))
                sourceType = MapSourceType(sql.FactoryName);
        }
        var agentsCount = record.AiAgents?.Count ?? 0;
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

        var channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);
        return new ChannelStatsResponse(channels.Count, channels.Count(c => c.Enabled));
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
            using var session = store.OpenAsyncSession(database);
            var rows = await QueryAllMetricRowsAsync(session, ct);
            return rows
                .GroupBy(r => r.Agent)
                .ToDictionary(
                    g => g.Key,
                    g => new AgentActivity(
                        g.Sum(r => r.Conversations), g.Sum(r => r.Messages), g.Sum(r => r.Tokens),
                        Utc(g.Max(r => r.Bucket))),
                    StringComparer.OrdinalIgnoreCase);
    }

    public static async Task<ConversationListResult> GetConversationsAsync(IDocumentStore store, string slug, string database, UsagePeriod period, int start, int pageSize, DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        return await GetConversationsAsync(session, slug, period, start, pageSize, nowUtc, ct);
    }

    internal static async Task<ConversationListResult> GetConversationsAsync(
        IAsyncDocumentSession session, string slug, UsagePeriod period, int start, int pageSize, DateTime nowUtc, CancellationToken ct)
    {
        var previews = await session.Query<ConversationPreview, ConversationPreviewIndex>()
            .Where(x => x.LastMessageAt >= period.Start && x.LastMessageAt < period.End)
            .Statistics(out var stats)
                .Include(i => i.IncludeDocuments<Channel>(p => p.ChannelId))
                .OrderByDescending(x => x.LastMessageAt)
                .Skip(start).Take(pageSize)
                .ToListAsync(ct);

        var items = new List<ConversationDto>(previews.Count);
        foreach (var p in previews)
            items.Add(BuildPreviewDto(p, slug, await ChannelNameAsync(session, p.ChannelId, ct), nowUtc));

        return new ConversationListResult(items, stats.TotalResults);
    }

    public static async Task<ConversationDto?> GetConversationAsync(
        IDocumentStore store, string slug, string database, string conversationId, DateTime nowUtc, CancellationToken ct)
    {
        if (conversationId.StartsWith(ConversationIdPrefix, StringComparison.Ordinal) == false)
            return null;

        var result = await store.AI.ForDatabase(database).GetConversationMessagesAsync(new GetConversationMessagesOptions
        {
            ConversationId = conversationId,
            DetailLevel = AiConversationDetailLevel.Detailed,
            PageSize = 25
        }, ct);

        if (result is null)
            return null;

        using var session = store.OpenAsyncSession(database);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor(conversationId),
            include => include.IncludeDocuments<Channel>(p => p.ChannelId), ct)
            ?? new ConversationPreview { ConversationId = conversationId, CreatedAt = result.CreatedAt, LastMessageAt = result.LastMessageAt };
        var channelName = await ChannelNameAsync(session, preview.ChannelId, ct);

        var config = await AgentLookup.FindAsync(store, database, result.Agent, ct);
        var replyField = AgentOutputShape.ResolveReplyField(config);

        return BuildDto(result, slug, preview, channelName, replyField, nowUtc);
    }

    private static async Task<string> ChannelNameAsync(IAsyncDocumentSession session, string channelId, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(channelId))
            return "";
        var channel = await session.LoadAsync<Channel>(channelId, ct);
        if (channel is null)
            return "";
        // fall back to the bare widget id as a label when the channel has no display name
        return string.IsNullOrWhiteSpace(channel.DisplayName)
            ? channelId[Channel.IdPrefix.Length..]
            : channel.DisplayName;
    }

    private static ConversationDto BuildPreviewDto(ConversationPreview p, string slug, string channelName, DateTime nowUtc)
    {
        var prms = p.Parameters.Select(kv => new ConversationParam(kv.Key, kv.Value)).ToArray();
        var at = Utc(p.LastMessageAt);
        AiConversationMessage[] lastExchange =
        [
            new()
            {
                Role = AiMessageRole.Assistant,
                Content = p.LastAgentReply,
                Timestamp = at
            },
            new()
            {
                Role = AiMessageRole.User,
                Content = p.LastUserPrompt,
                Timestamp = at
            }
        ];
        return MakeDto(p.ConversationId, slug, channelName, p.Agent, prms, lastExchange, transcript: null,
            p.LastMessageAt, p.CreatedAt, nowUtc);
    }

    private static ConversationDto BuildDto(AiConversationMessagesResult result, string slug, ConversationPreview preview, string channelName, string replyField, DateTime nowUtc)
    {
        var previewDto = BuildPreviewDto(preview, slug, channelName, nowUtc);
        var transcript = MapTranscript(result.Messages, replyField);
        return MakeDto(result.ConversationId, slug, channelName, result.Agent, previewDto.Params, previewDto.LastExchange, transcript,
            result.LastMessageAt, result.CreatedAt, nowUtc);
    }

    private static ConversationDto MakeDto(
        string id, string slug, string channelName, string agent,
        ConversationParam[] parameters, AiConversationMessage[] lastExchange, AiConversationMessage[] transcript,
        DateTime lastMessageAt, DateTime createdAt, DateTime nowUtc)
    {
        var age = nowUtc - lastMessageAt;
        var state = age < TimeSpan.FromHours(1) ? "active" : age < TimeSpan.FromHours(24) ? "idle" : "closed";

        return new ConversationDto(
            id, slug, channelName, agent, GetAgentInitials(agent),
            parameters, lastExchange, transcript,
            state, Utc(lastMessageAt), Utc(createdAt), MaxDuration: null);
    }

    private static string GetAgentInitials(string name)
    {
        var parts = name.Split([' ', '-', '_'], StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
            return "?";
        if (parts.Length == 1)
            return parts[0][..Math.Min(2, parts[0].Length)].ToUpperInvariant();
        return $"{parts[0][0]}{parts[1][0]}".ToUpperInvariant();
    }

    internal static AiConversationMessage[] MapTranscript(IEnumerable<AiConversationMessage> messages, string replyField)
    {
        var turns = new List<AiConversationMessage>();
        foreach (var m in messages)
        {
            switch (m.Role)
            {
                case AiMessageRole.System:
                case AiMessageRole.Summary:
                case AiMessageRole.Internal:
                    continue;
                case AiMessageRole.User:
                    break;
                case AiMessageRole.Assistant:
                    if (string.IsNullOrEmpty(m.Content))
                        break;

                    if (string.IsNullOrEmpty(replyField) == false)
                    {
                        try
                        {
                            var answer = JsonSerializer.Deserialize<Dictionary<string, object>>(m.Content);
                            m.Content = AgentOutputShape.ExtractReplyText(answer, replyField);
                        }
                        catch
                        {
                            // ignore
                        }
                    }

                    break;
                default:
                    continue;
            }
            turns.Add(m);
        }
        return turns.ToArray();
    }

    internal static async Task<List<App>> LoadAllAppsAsync(IDocumentStore store, CancellationToken ct)
    {
        using var configSession = store.OpenAsyncSession();
        return await configSession.LoadAllStartingWithAsync<App>(AppIdPrefix, ct);
    }

    private static DateTime Utc(DateTime d) => d.Kind switch
    {
        DateTimeKind.Utc => d,
        DateTimeKind.Local => d.ToUniversalTime(),
        _ => DateTime.SpecifyKind(d, DateTimeKind.Utc),
    };

    // isolate per-app failures: one bad tenant DB can't 500 a global fan-out


    private static async Task<List<ConversationMetricsIndex.Result>> QueryAllMetricRowsAsync(
        IAsyncDocumentSession session, CancellationToken ct)
    {
        try
        {
            return await session.Advanced
                .AsyncDocumentQuery<ConversationMetricsIndex.Result, ConversationMetricsIndex>()
                .ToListAsync(ct);
        }
        catch (IndexDoesNotExistException) { return []; }  // fresh app DB / index not built yet ⇒ no activity
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
