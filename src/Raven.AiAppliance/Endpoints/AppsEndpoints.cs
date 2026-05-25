using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Raven;
using Raven.AiAppliance.Schema;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;

namespace Raven.AiAppliance.Endpoints;

public static class AppsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps");
        group.MapGet("/", ListAsync);
        group.MapGet("/{slug}", GetAsync);
        group.MapPost("/{slug}/setup/agent", ProvisionAgentAsync);
        group.MapPost("/{slug}/setup/channel", ProvisionChannelAsync);
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

    private static async Task<IResult> ProvisionAgentAsync(
        string slug,
        ProvisionAgentRequest body,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        IAgentSchema schema,
        ILogger<AppsLogger> logger,
        CancellationToken ct)
    {
        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        App? app;
        using (var session = store.OpenAsyncSession(opts.ConfigDatabase))
        {
            app = await session.Query<App>()
                .Where(x => x.Slug == slug)
                .FirstOrDefaultAsync(ct);
        }

        if (app is null)
            return Results.NotFound(new { error = $"no app with slug '{slug}'" });

        // Framing is recorded for now but not yet wired to schema selection
        // (design §1.3 step 9 — AI-suggest paths are a follow-up). The 8-week
        // demo always registers the DI-injected schema.
        logger.LogInformation(
            "Provisioning agent for app slug={Slug} framing={Framing} schema={SchemaId}",
            app.Slug, body?.Framing ?? "(none)", schema.Identifier);

        var result = await AiAgentRegistrar.RegisterAsync(
            store: store,
            schema: schema,
            options: opts,
            targetDatabase: app.Database,
            ct: ct);

        return Results.Ok(new { agentId = result.AgentIdentifier });
    }

    private static async Task<IResult> ProvisionChannelAsync(
        string slug,
        ProvisionChannelRequest body,
        IDocumentStore store,
        IOptions<ApplianceOptions> options,
        IAgentSchemaRegistry schemas,
        ILogger<AppsLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.Type) || string.IsNullOrWhiteSpace(body.AgentId))
            return Results.BadRequest(new { error = "type and agentId are required" });

        // 8-week demo: iFrame only. Telegram + WhatsApp are deferred per
        // design §3.6 / §3.7. Case-insensitive — the design spells it "IFrame"
        // but tests / curl will commonly send "iframe".
        if (!string.Equals(body.Type, "iframe", StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest(new { error = $"unsupported channel type '{body.Type}'. Supported: iframe." });

        if (!schemas.TryGet(body.AgentId, out _))
            return Results.BadRequest(new { error = $"unknown agentId '{body.AgentId}'" });

        var opts = options.Value;
        await RavenStoreFactory.EnsureDatabaseAsync(store, opts.ConfigDatabase, ct);

        App? app;
        using (var session = store.OpenAsyncSession(opts.ConfigDatabase))
        {
            app = await session.Query<App>()
                .Where(x => x.Slug == slug)
                .FirstOrDefaultAsync(ct);
        }

        if (app is null)
            return Results.NotFound(new { error = $"no app with slug '{slug}'" });

        // widgetId is the stable customer-facing identifier baked into embed
        // snippets — see design §3.4 + the plan's TDD discussion. Generated
        // (not user-supplied) so rename of DisplayName never breaks the URL.
        // 32-bit space ≈ 4B values is plenty per-app; empty-change-vector
        // store gives atomic uniqueness; 5-attempt retry absorbs the
        // statistically impossible collision.
        for (var attempt = 0; attempt < 5; attempt++)
        {
            var widgetId = "wgt_" + Guid.NewGuid().ToString("N").Substring(0, 8);
            var docId = $"@channels/{widgetId}";
            var channel = new ChannelInstance
            {
                Id = docId,
                Type = "IFrame",
                DisplayName = body.DisplayName ?? "iframe",
                AgentId = body.AgentId,
                AllowedOrigins = body.AllowedOrigins ?? [],
                CreatedAt = DateTime.UtcNow,
            };

            try
            {
                using var session = store.OpenAsyncSession(app.Database);
                await session.StoreAsync(channel, changeVector: string.Empty, id: docId, ct);
                await session.SaveChangesAsync(ct);

                logger.LogInformation(
                    "Provisioned channel slug={Slug} widgetId={WidgetId} agentId={AgentId} type={Type}",
                    app.Slug, widgetId, body.AgentId, channel.Type);

                return Results.Ok(new { widgetId });
            }
            catch (ConcurrencyException)
            {
                // GUID collision in a 4-billion-value space — astronomically
                // unlikely; loop generates a new ID. Reaching attempt 5
                // signals broken RNG, not normal operation.
            }
        }

        throw new InvalidOperationException(
            $"Could not generate a unique widgetId after 5 attempts. " +
            $"This usually means the RNG is broken; investigate before retrying.");
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class AppsLogger;

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
