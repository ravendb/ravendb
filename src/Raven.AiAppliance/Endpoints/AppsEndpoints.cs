using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;

namespace Raven.AiAppliance.Endpoints;

public static class AppsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps");
        group.MapGet("/", ListAsync);
        group.MapGet("/{slug}", GetAsync);
    }

    private static async Task<IResult> ListAsync(
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        CancellationToken ct)
    {
        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        using var session = store.OpenAsyncSession(opts.ConfigDatabase);
        var apps = await session.Query<App>()
            .OrderByDescending(x => x.CreatedAt)
            .ToListAsync(ct);

        return Results.Ok(apps.Select(AppDto.From));
    }

    private static async Task<IResult> GetAsync(
        string slug,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        CancellationToken ct)
    {
        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        using var session = store.OpenAsyncSession(opts.ConfigDatabase);
        var app = await session.Query<App>()
            .Where(x => x.Slug == slug)
            .FirstOrDefaultAsync(ct);

        return app is null ? Results.NotFound() : Results.Ok(AppDto.From(app));
    }

    private sealed record AppDto(
        string Id,
        string Name,
        string Database,
        string CdcTaskName,
        string CreatedAt)
    {
        public static AppDto From(App app) => new(
            app.Slug,
            app.AppName,
            app.Database,
            app.CdcTaskName,
            app.CreatedAt.ToString("O"));
    }
}
