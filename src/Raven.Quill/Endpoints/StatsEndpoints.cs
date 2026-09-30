using System.Globalization;
using Raven.Client.Documents;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Licensing;
using Raven.Quill.Metrics;
using Raven.Quill.Wizard;

namespace Raven.Quill.Endpoints;

/// <summary>
/// Read-side dashboard statistics for the per-app views. Each route aggregates
/// existing per-app data (conversations, channels, agents, db stats) into the
/// windows the UI renders � no write path, no live LLM. Live operational
/// telemetry (CDC ingestion) is served separately via the WebSocket feed proxy.
/// </summary>
public static class StatsEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/api/usage", GetUsageAsync)
            .WithTags("stats")
            .WithName("stats.usage")
            .RequireAuthorization()
            .Produces<UsageResponse>();

        app.MapGet("/api/usage/by-app", GetTokensByAppAsync)
            .WithTags("stats")
            .WithName("stats.tokensByApp")
            .RequireAuthorization()
            .Produces<TokensByAppResponse>();

        app.MapGet("/api/dashboard/apps", GetDashboardAppsAsync)
            .WithTags("stats")
            .WithName("stats.dashboardApps")
            .RequireAuthorization()
            .Produces<ApplianceAppResponse[]>();

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

        group.MapGet("/usage", GetAppUsageAsync)
            .WithName("stats.appUsage")
            .Produces<AppUsageResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/collections", GetCollectionsAsync)
            .WithName("stats.collections")
            .Produces<DataCollectionDto[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/conversations", GetConversationsListAsync)
            .WithName("stats.conversations")
            .Produces<ConversationListResult>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/conversations/{*conversationId}", GetConversationByIdAsync)
            .WithName("stats.conversation")
            .Produces<ConversationDto>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

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

    private static async Task<IResult> GetUsageAsync(
        ILicenseStatsProvider provider,
        IDocumentStore store,
        string? app,
        int year,
        int? month,
        int? day,
        CancellationToken ct)
    {
        List<App> apps = [];
        if (app is not null)
        {
            var loadedApp = await AppLookup.LoadAppAsync(store, app, ct);
            if (loadedApp is null)
                return Results.NotFound(new ApiErrorResponse($"no app with slug '{app}'"));

            apps.Add(loadedApp);
        }
        else
        {
            apps = await MetricsReadService.LoadAllAppsAsync(store, ct);
        }

        var usage = await MetricsReadService.GetUsageAsync(provider, store, apps, year, month, day, ct);
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
        int year,
        int? month,
        int? day,
        int? start,
        int? pageSize,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var now = DateTime.UtcNow;
        var period = new UsagePeriod(year, month, day, now);

        var items = await MetricsReadService.GetConversationsAsync(store, slug, app.Database, period, start ?? 0, pageSize ?? int.MaxValue, now, ct);
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

        // Kestrel keeps %2F encoded; decode the chats/... id back
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

        // deferred: no event-log source yet — empty feed
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
        int year,
        int? month,
        int? day,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var stats = await MetricsReadService.GetConversationStatsAsync(store, app.Database, year, month, day, ct);
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
