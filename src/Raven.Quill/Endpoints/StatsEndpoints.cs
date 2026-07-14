using System.Globalization;
using Raven.Client.Documents;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Licensing;
using Raven.Quill.Metrics;

namespace Raven.Quill.Endpoints;

/// <summary>
/// Read-side dashboard statistics for the per-app views. Each route aggregates
/// existing per-app data (conversations, channels, agents, db stats) into the
/// windows the UI renders — no write path, no live LLM. Live operational
/// telemetry (CDC ingestion) is served separately via the WebSocket feed proxy.
/// </summary>
public static class StatsEndpoints
{
    public static void Map(WebApplication app)
    {
        // Global roll-up across all apps (not per-slug).
        app.MapGet("/api/dashboard", GetDashboardAsync)
            .WithTags("stats")
            .WithName("stats.dashboard")
            .RequireAuthorization()
            .Produces<DashboardResponse>();

        // Usage series (conversations / messages / tokens) over a calendar period — global or per-app.
        // `GET /api/usage?year={y}[&month={m}][&day={d}][&app={slug}]` (app omitted → summed across all
        // apps). The granularity follows which fields are set: year → 12 months, +month → the month's
        // days, +month+day → the day's 24 hours. Out-of-range values are clamped, not rejected.
        app.MapGet("/api/usage", GetUsageAsync)
            .WithTags("stats")
            .WithName("stats.usage")
            .RequireAuthorization()
            .Produces<UsagePoint[]>();

        // Per-app token-usage breakdown.
        app.MapGet("/api/usage/by-app", GetTokensByAppAsync)
            .WithTags("stats")
            .WithName("stats.tokensByApp")
            .RequireAuthorization()
            .Produces<TokensByAppResponse>();

        // Enriched apps list for the Dashboard table.
        app.MapGet("/api/dashboard/apps", GetDashboardAppsAsync)
            .WithTags("stats")
            .WithName("stats.dashboardApps")
            .RequireAuthorization()
            .Produces<ApplianceAppResponse[]>();

        // Single enriched app.
        app.MapGet("/api/dashboard/apps/{slug}", GetDashboardAppAsync)
            .WithTags("stats")
            .WithName("stats.dashboardApp")
            .RequireAuthorization()
            .Produces<ApplianceAppResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        var group = app.MapGroup("/api/apps/{slug}").WithTags("stats").RequireAuthorization();

        group.MapGet("/overview", GetAppOverviewAsync)
            .WithName("stats.overview")
            .Produces<AppOverviewResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // Per-app usage analytics.
        group.MapGet("/usage", GetAppUsageAsync)
            .WithName("stats.appUsage")
            .Produces<AppUsageResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // Mirrored data collections with document counts.
        group.MapGet("/collections", GetCollectionsAsync)
            .WithName("stats.collections")
            .Produces<DataCollectionDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // Conversations list + detail.
        // The {*conversationId} catch-all carries the "chats/..." id (it contains a slash);
        // the literal "/conversations/stats" route still wins by routing precedence.
        group.MapGet("/conversations", GetConversationsListAsync)
            .WithName("stats.conversations")
            .Produces<ConversationDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/conversations/{*conversationId}", GetConversationByIdAsync)
            .WithName("stats.conversation")
            .Produces<ConversationDto>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // CDC "Events" tab. Deferred — no event log yet, so
        // this returns an empty feed (a real audit log is a separate ticket).
        group.MapGet("/activity", GetActivityAsync)
            .WithName("stats.activity")
            .Produces<ActivityEventDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/conversations/stats", GetConversationStatsAsync)
            .WithName("stats.conversationStats")
            .Produces<ConversationStatsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/channels/stats", GetChannelStatsAsync)
            .WithName("stats.channels")
            .Produces<ChannelStatsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetDashboardAsync(
        IDocumentStore store,
        ILoggerFactory loggerFactory,
        CancellationToken ct)
    {
        var dashboard = await MetricsReadService.GetDashboardStatsAsync(
            store, DateTime.UtcNow, loggerFactory.CreateLogger(nameof(MetricsReadService)), ct);
        return Results.Ok(dashboard);
    }

    private static async Task<IResult> GetUsageAsync(
        ILicenseStatsProvider provider,
        IDocumentStore store,
        ILoggerFactory loggerFactory,
        int year,
        int? month,
        int? day,
        string? app,
        CancellationToken ct)
    {
        var usage = await MetricsReadService.GetUsageAsync(provider,
            store, year, month, day, app, loggerFactory.CreateLogger(nameof(MetricsReadService)), ct);
        return Results.Ok(usage);
    }

    private static async Task<IResult> GetTokensByAppAsync(
        IDocumentStore store,
        ILoggerFactory loggerFactory,
        CancellationToken ct)
    {
        var byApp = await MetricsReadService.GetTokensByAppAsync(
            store, loggerFactory.CreateLogger(nameof(MetricsReadService)), ct);
        return Results.Ok(byApp);
    }

    private static async Task<IResult> GetDashboardAppsAsync(
        IDocumentStore store,
        ILoggerFactory loggerFactory,
        CancellationToken ct)
    {
        var apps = await MetricsReadService.GetDashboardAppsAsync(
            store, loggerFactory.CreateLogger(nameof(MetricsReadService)), ct);
        return Results.Ok(apps);
    }

    private static async Task<IResult> GetDashboardAppAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await MetricsReadService.GetDashboardAppAsync(store, slug, ct);
        return app is null
            ? Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"))
            : Results.Ok(app);
    }

    private static async Task<IResult> GetAppUsageAsync(
        string slug,
        IDocumentStore store,
        int year,
        int? month,
        int? day,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var usage = await MetricsReadService.GetAppUsageAsync(store, app.Database, year, month, day, ct);
        return Results.Ok(usage);
    }

    private static async Task<IResult> GetCollectionsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var collections = await MetricsReadService.GetCollectionsAsync(store, slug, app.Database, ct);
        return Results.Ok(collections);
    }

    private static async Task<IResult> GetConversationsListAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var items = await MetricsReadService.GetConversationsAsync(store, slug, app.Database, DateTime.UtcNow, ct);
        return Results.Ok(items);
    }

    private static async Task<IResult> GetConversationByIdAsync(
        string slug,
        string conversationId,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        // The id carries a '/', e.g. "chats/recent". The browser client percent-encodes it to
        // "chats%2Frecent" and Kestrel keeps %2F encoded in the path (unlike TestServer), so the
        // catch-all hands us the still-encoded value — decode it back to the real document id.
        var decodedId = Uri.UnescapeDataString(conversationId);

        var conversation = await MetricsReadService.GetConversationAsync(store, slug, app.Database, decodedId, DateTime.UtcNow, ct);
        return conversation is null
            ? Results.NotFound(new ApiErrorResponse($"no conversation '{decodedId}'"))
            : Results.Ok(conversation);
    }

    private static async Task<IResult> GetActivityAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        // Deferred: no event-log source yet (see impl handoff) — empty feed.
        return Results.Ok(Array.Empty<ActivityEventDto>());
    }

    private static async Task<IResult> GetAppOverviewAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var overview = await MetricsReadService.GetAppOverviewAsync(store, slug, app.Database, ct);
        return Results.Ok(overview);
    }

    private static async Task<IResult> GetConversationStatsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var stats = await MetricsReadService.GetConversationStatsAsync(store, app.Database, DateTime.UtcNow, ct);
        return Results.Ok(stats);
    }

    private static async Task<IResult> GetChannelStatsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var stats = await MetricsReadService.GetChannelStatsAsync(store, app.Database, ct);
        return Results.Ok(stats);
    }
}
