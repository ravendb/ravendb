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

        var group = app.MapGroup("/api/apps/{slug}").WithTags("stats").RequireAuthorization();

        group.MapGet("/overview", GetAppOverviewAsync)
            .WithName("stats.overview")
            .Produces<AppOverviewResponse>()
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
