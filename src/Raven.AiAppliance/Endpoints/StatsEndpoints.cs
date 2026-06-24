using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Metrics;
using Raven.Client.Documents;

namespace Raven.AiAppliance.Endpoints;

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

        // Global hourly usage series (mock-api `getUsage()`) — last 24h, all apps.
        app.MapGet("/api/usage", GetUsageAsync)
            .WithTags("stats")
            .WithName("stats.usage")
            .RequireAuthorization()
            .Produces<UsagePoint[]>();

        // Per-app token-usage breakdown (mock-api `getTokensByApp()`).
        app.MapGet("/api/usage/by-app", GetTokensByAppAsync)
            .WithTags("stats")
            .WithName("stats.usage.byApp")
            .RequireAuthorization()
            .Produces<TokensByAppResponse>();

        // Enriched apps list for the Dashboard table (mock-api `listApps()`).
        app.MapGet("/api/dashboard/apps", GetDashboardAppsAsync)
            .WithTags("stats")
            .WithName("stats.dashboard.apps")
            .RequireAuthorization()
            .Produces<AppliancAppResponse[]>();

        var group = app.MapGroup("/api/apps/{slug}").WithTags("stats").RequireAuthorization();

        group.MapGet("/overview", GetAppOverviewAsync)
            .WithName("stats.overview")
            .Produces<AppOverviewResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // Per-app usage analytics (mock-api `getAppUsage({appId,start,end})`).
        group.MapGet("/usage", GetAppUsageAsync)
            .WithName("stats.appUsage")
            .Produces<AppUsageResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // Mirrored data collections with document counts (mock-api `listCollections(appId)`).
        group.MapGet("/collections", GetCollectionsAsync)
            .WithName("stats.collections")
            .Produces<DataCollectionDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // Conversations list + detail (mock-api `listConversations` / `getConversation`).
        // The {*conversationId} catch-all carries the "chats/..." id (it contains a slash);
        // the literal "/conversations/stats" route still wins by routing precedence.
        group.MapGet("/conversations", GetConversationsListAsync)
            .WithName("stats.conversations.list")
            .Produces<ConversationDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/conversations/{*conversationId}", GetConversationByIdAsync)
            .WithName("stats.conversations.get")
            .Produces<ConversationDto>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        // CDC "Events" tab (mock-api `listActivity`). Deferred — no event log yet, so
        // this returns an empty feed (a real audit log is a separate ticket).
        group.MapGet("/activity", GetActivityAsync)
            .WithName("stats.activity")
            .Produces<ActivityEventDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/conversations/stats", GetConversationStatsAsync)
            .WithName("stats.conversations")
            .Produces<ConversationStatsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/agents/stats", GetAgentStatsAsync)
            .WithName("stats.agents")
            .Produces<AgentStatsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/channels/stats", GetChannelStatsAsync)
            .WithName("stats.channels")
            .Produces<ChannelStatsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetDashboardAsync(
        IDocumentStore store,
        CancellationToken ct)
    {
        var dashboard = await MetricsReadService.GetDashboardStatsAsync(store, DateTime.UtcNow, ct);
        return Results.Ok(dashboard);
    }

    private static async Task<IResult> GetUsageAsync(
        IDocumentStore store,
        CancellationToken ct)
    {
        var usage = await MetricsReadService.GetUsageAsync(store, DateTime.UtcNow, ct);
        return Results.Ok(usage);
    }

    private static async Task<IResult> GetTokensByAppAsync(
        IDocumentStore store,
        CancellationToken ct)
    {
        var byApp = await MetricsReadService.GetTokensByAppAsync(store, ct);
        return Results.Ok(byApp);
    }

    private static async Task<IResult> GetDashboardAppsAsync(
        IDocumentStore store,
        CancellationToken ct)
    {
        var apps = await MetricsReadService.GetDashboardAppsAsync(store, ct);
        return Results.Ok(apps);
    }

    private static async Task<IResult> GetAppUsageAsync(
        string slug,
        DateTime? start,
        DateTime? end,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var endUtc = (end ?? DateTime.UtcNow).ToUniversalTime();
        var startUtc = (start ?? endUtc.AddDays(-7)).ToUniversalTime();

        var usage = await MetricsReadService.GetAppUsageAsync(store, app.Database, startUtc, endUtc, ct);
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

        var items = await MetricsReadService.GetConversationsAsync(store, app.Database, DateTime.UtcNow, ct);
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

        var conversation = await MetricsReadService.GetConversationAsync(store, app.Database, conversationId, DateTime.UtcNow, ct);
        return conversation is null
            ? Results.NotFound(new ApiErrorResponse($"no conversation '{conversationId}'"))
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

    private static async Task<IResult> GetAgentStatsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var stats = await MetricsReadService.GetAgentStatsAsync(store, app.Database, DateTime.UtcNow, ct);
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
