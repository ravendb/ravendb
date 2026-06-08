using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Schema;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;

namespace Raven.AiAppliance.Endpoints;

/// <summary>
/// All channel HTTP operations for an app (mirrors
/// <see cref="AiConnectionStringsEndpoints"/>): create (the wizard's
/// <c>/setup/channel</c> step), list, edit, and delete. Provision / edit /
/// delete each <b>dispatch on the channel <see cref="ChannelType"/></b> to a
/// per-type method — iFrame is implemented; Telegram / WhatsApp are 501 stubs
/// (the seam RavenDB-26631 fills). Embed rendering (the public consume-side)
/// lives in <see cref="EmbedEndpoints"/>.
/// </summary>
public static class ChannelsEndpoints
{
    // M2/M4 caps prevent unbounded doc growth + give the embed page a
    // trustworthy AllowedOrigins list to consume.
    private const int MaxAllowedOrigins = 32;
    private const int MaxOriginLength = 256;
    private const int MaxDisplayNameLength = 200;

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("channels");

        group.MapPost("/setup/channel", ProvisionChannelAsync)
            .WithName("channels.create")
            .WithDescription(
                "Registers a channel for the app. allowedOrigins is required; entries are " +
                "normalized to scheme://authority (max 32). An explicit empty list is the " +
                "opt-in contract: the embed page emits no CSP frame-ancestors header and is " +
                "embeddable from any site. Provision is create-only: when the (type, agent) " +
                "channel already exists the response carries existing=true and the request's " +
                "allowedOrigins/displayName are NOT applied — edit via PUT /channels/{id}.")
            .Accepts<ProvisionChannelRequest>("application/json")
            .Produces<ProvisionChannelResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status501NotImplemented);

        group.MapGet("/channels", ListChannelsAsync)
            .WithName("channels.list")
            .Produces<ChannelSummaryResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/channels/{channelId}", UpdateChannelAsync)
            .WithName("channels.update")
            .Accepts<UpdateChannelRequest>("application/json")
            .Produces<ChannelSummaryResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status501NotImplemented);

        group.MapDelete("/channels/{channelId}", DeleteChannelAsync)
            .WithName("channels.delete")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status501NotImplemented);
    }

    // ---- create (dispatch on requested type) ----

    private static async Task<IResult> ProvisionChannelAsync(
        string slug,
        ProvisionChannelRequest body,
        IDocumentStore store,
        IAgentSchemaRegistry schemas,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.AgentId))
            return Results.BadRequest(new ApiErrorResponse("agentId is required"));

        // L1: load App first so unknown-slug always returns 404 regardless of
        // type/agentId — the 400-vs-404 differential otherwise leaks which
        // agentIds are registered to unauthenticated probers.
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        return body.Type switch
        {
            ChannelType.IFrame => await ProvisionIFrameAsync(app, body, schemas, store, logger, ct),
            ChannelType.Telegram => ProvisionTelegramAsync(),
            ChannelType.WhatsApp => ProvisionWhatsAppAsync(),
            null => Results.BadRequest(new ApiErrorResponse("type is required")),
            _ => Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{body.Type}'")),
        };
    }

    private static async Task<IResult> ProvisionIFrameAsync(
        App app,
        ProvisionChannelRequest body,
        IAgentSchemaRegistry schemas,
        IDocumentStore store,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        // L3: validate against the registry AND adopt its canonical casing for
        // storage (TryGet is case-insensitive but the caller's casing was being
        // persisted, which would trip later case-sensitive queries on AgentId).
        if (!schemas.TryGet(body.AgentId, out var schema))
            return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{body.AgentId}'"));

        // "Embeddable from anywhere" must be an explicit opt-in
        // (allowedOrigins: []) — an omitted property is rejected rather than
        // silently provisioning an open embed.
        if (body.AllowedOrigins is null)
            return Results.BadRequest(new ApiErrorResponse(
                "allowedOrigins is required; pass an empty array to make the embed page embeddable from anywhere"));

        var origins = body.AllowedOrigins;
        if (TryNormalizeOrigins(origins, out var originError) == false)
            return Results.BadRequest(new ApiErrorResponse(originError!));

        if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
            return Results.BadRequest(new ApiErrorResponse(nameError!));

        // C2 (Copilot review #4362803113): idempotency on (slug, type, agentId)
        // via an atomic guard on a deterministic binding doc id. Write the
        // binding doc AND the channel doc in one TransactionMode.ClusterWide
        // session — RavenDB auto-creates an atomic guard at
        // "rvn-atomic/{bindingId}". Concurrent writers Raft-serialize; the
        // loser reads the winner's binding and returns the same widgetId.
        var bindingId = $"channel-bindings/{app.Slug}/{ChannelType.IFrame}/{schema.Identifier}";

        // Fast path: operator double-click / client retry skips the cluster-wide round trip.
        using (var session = store.OpenAsyncSession(app.Database))
        {
            var existing = await session.LoadAsync<ChannelBinding>(bindingId, ct);
            if (existing is not null)
            {
                await UpsertWidgetIndexAsync(store, existing.WidgetId, app.Slug, ct);
                logger.LogInformation(
                    "Channel binding already exists for slug={Slug} agentId={AgentId}; returning existing widgetId={WidgetId}",
                    app.Slug, schema.Identifier, existing.WidgetId);
                return Results.Ok(new ProvisionChannelResponse(existing.WidgetId, Existing: true));
            }
        }

        // Slow path. H1 (security review 2026-05-25): widgetId is the public,
        // bearer-style identifier baked into embed snippets and the
        // /embed/{widgetId} path. A random GUID keeps it unguessable — NOT
        // derived from the binding tuple (slug + type + agentId are public
        // inputs).
        var widgetId = "wgt_" + Guid.NewGuid().ToString("N");
        var channelDocId = Channel.IdPrefix + widgetId;

        try
        {
            using var session = store.OpenAsyncSession(new global::Raven.Client.Documents.Session.SessionOptions
            {
                Database = app.Database,
                TransactionMode = TransactionMode.ClusterWide,
            });

            await session.StoreAsync(new ChannelBinding
            {
                Id = bindingId,
                WidgetId = widgetId,
                CreatedAt = DateTime.UtcNow,
            }, ct);

            await session.StoreAsync(new Channel
            {
                Id = channelDocId,
                Type = ChannelType.IFrame,
                DisplayName = body.DisplayName ?? ChannelType.IFrame.ToString(),
                AgentId = schema.Identifier,
                AllowedOrigins = origins,
                Enabled = true,
                CreatedAt = DateTime.UtcNow,
                BindingId = bindingId,
            }, ct);

            await session.SaveChangesAsync(ct);

            await UpsertWidgetIndexAsync(store, widgetId, app.Slug, ct);

            logger.LogInformation(
                "Provisioned iFrame channel slug={Slug} widgetId={WidgetId} agentId={AgentId}",
                app.Slug, widgetId, schema.Identifier);

            return Results.Ok(new ProvisionChannelResponse(widgetId));
        }
        catch (ClusterTransactionConcurrencyException)
        {
            // Lost the race. The winner's binding is committed through Raft, but
            // the document apply lags the commit on this node, so an immediate
            // read-back can miss it — retry briefly until it appears (~500ms
            // budget). NOTE (ayende PR review 2026-06-07 challenged this): a
            // single post-conflict read SHOULD see the winner, but empirically
            // both a plain and a cluster-wide read flake ~20-30% here, so the
            // bounded retry stays until the correct wait-for-index read is
            // confirmed — see the review thread.
            var winner = await LoadBindingWithRetryAsync(store, app.Database, bindingId, ct);
            if (winner is null)
            {
                throw new InvalidOperationException(
                    $"ClusterTransactionConcurrencyException fired for '{bindingId}' but the binding doc never became visible after the conflict.");
            }

            await UpsertWidgetIndexAsync(store, winner.WidgetId, app.Slug, ct);
            logger.LogInformation(
                "Lost race for binding slug={Slug} agentId={AgentId}; returning winner's widgetId={WidgetId}",
                app.Slug, schema.Identifier, winner.WidgetId);
            return Results.Ok(new ProvisionChannelResponse(winner.WidgetId, Existing: true));
        }
    }

    private static IResult ProvisionTelegramAsync() => NotImplementedChannel(ChannelType.Telegram);

    private static IResult ProvisionWhatsAppAsync() => NotImplementedChannel(ChannelType.WhatsApp);

    // ---- list ----

    private static async Task<IResult> ListChannelsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);

        // LoadStartingWith on the shared "channels/" id prefix instead of a
        // collection Query: immediately consistent (no index-staleness wait
        // right after a create) and the natural fit since every channel doc
        // shares the prefix. Page until a short page returns so a large channel
        // set is never silently truncated. Order by CreatedAt in memory.
        const int pageSize = 1024;
        var channels = new List<Channel>();
        for (var start = 0; ; start += pageSize)
        {
            var page = (await session.Advanced.LoadStartingWithAsync<Channel>(
                Channel.IdPrefix, start: start, pageSize: pageSize, token: ct)).ToArray();
            channels.AddRange(page);
            if (page.Length < pageSize)
                break;
        }

        var items = channels
            .OrderByDescending(c => c.CreatedAt)
            .Select(ChannelSummaryResponse.From)
            .ToArray();

        return Results.Ok(items);
    }

    // ---- edit (dispatch on stored type) ----

    private static async Task<IResult> UpdateChannelAsync(
        string slug,
        string channelId,
        UpdateChannelRequest body,
        IDocumentStore store,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no channel '{channelId}' in app '{slug}'"));

        // The doc's stored Type is authoritative — the request carries no type.
        return channel.Type switch
        {
            ChannelType.IFrame => await UpdateIFrameChannelAsync(session, channel, body, app.Slug, channelId, logger, ct),
            ChannelType.Telegram => UpdateTelegramChannelAsync(),
            ChannelType.WhatsApp => UpdateWhatsAppChannelAsync(),
            _ => Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{channel.Type}'")),
        };
    }

    private static async Task<IResult> UpdateIFrameChannelAsync(
        IAsyncDocumentSession session,
        Channel channel,
        UpdateChannelRequest body,
        string slug,
        string channelId,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (body.AllowedOrigins is not null)
        {
            var origins = body.AllowedOrigins;
            if (TryNormalizeOrigins(origins, out var originError) == false)
                return Results.BadRequest(new ApiErrorResponse(originError!));
            channel.AllowedOrigins = origins;
        }

        if (body.DisplayName is not null)
        {
            if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
                return Results.BadRequest(new ApiErrorResponse(nameError!));
            channel.DisplayName = body.DisplayName;
        }

        if (body.Enabled is not null)
            channel.Enabled = body.Enabled.Value;

        await session.SaveChangesAsync(ct);

        logger.LogInformation(
            "Updated iFrame channel slug={Slug} channelId={ChannelId} enabled={Enabled}",
            slug, channelId, channel.Enabled);

        return Results.Ok(ChannelSummaryResponse.From(channel));
    }

    private static IResult UpdateTelegramChannelAsync() => NotImplementedChannel(ChannelType.Telegram);

    private static IResult UpdateWhatsAppChannelAsync() => NotImplementedChannel(ChannelType.WhatsApp);

    // ---- delete (dispatch on stored type) ----

    private static async Task<IResult> DeleteChannelAsync(
        string slug,
        string channelId,
        IDocumentStore store,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        Channel? channel;
        using (var session = store.OpenAsyncSession(app.Database))
            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);

        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no channel '{channelId}' in app '{slug}'"));

        return channel.Type switch
        {
            ChannelType.IFrame => await DeleteIFrameChannelAsync(store, app, channelId, logger, ct),
            ChannelType.Telegram => DeleteTelegramChannelAsync(),
            ChannelType.WhatsApp => DeleteWhatsAppChannelAsync(),
            _ => Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{channel.Type}'")),
        };
    }

    private static async Task<IResult> DeleteIFrameChannelAsync(
        IDocumentStore store,
        App app,
        string channelId,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        var channelDocId = Channel.IdPrefix + channelId;

        // Delete the channel AND its binding in one cluster-wide session.
        // Removing the binding doc clears the atomic guard at
        // "rvn-atomic/{bindingId}", so the same (slug, type, agentId) tuple can
        // be re-provisioned afterwards.
        using (var session = store.OpenAsyncSession(new global::Raven.Client.Documents.Session.SessionOptions
        {
            Database = app.Database,
            TransactionMode = TransactionMode.ClusterWide,
        }))
        {
            var channel = await session.LoadAsync<Channel>(channelDocId, ct);
            if (channel is not null)
                session.Delete(channel);

            // Use the BindingId from the channel loaded inside THIS cluster-wide
            // tx (not a value captured by the earlier non-transactional read) so
            // the binding doc — and its atomic guard — is always cleared.
            if (channel is not null && string.IsNullOrEmpty(channel.BindingId) == false)
            {
                var binding = await session.LoadAsync<ChannelBinding>(channel.BindingId, ct);
                if (binding is not null)
                    session.Delete(binding);
            }

            await session.SaveChangesAsync(ct);
        }

        // The widget-index pointer lives in the config DB and can't join the
        // per-app cluster-wide tx above. Delete it separately; a brief orphan
        // (crash between the two) is harmless — re-provision overwrites it and
        // the embed page re-validates the channel doc exists.
        using (var cfg = store.OpenAsyncSession())
        {
            cfg.Delete($"widget-index/{channelId}");
            await cfg.SaveChangesAsync(ct);
        }

        logger.LogInformation("Deleted iFrame channel slug={Slug} channelId={ChannelId}", app.Slug, channelId);
        return Results.NoContent();
    }

    private static IResult DeleteTelegramChannelAsync() => NotImplementedChannel(ChannelType.Telegram);

    private static IResult DeleteWhatsAppChannelAsync() => NotImplementedChannel(ChannelType.WhatsApp);

    // ---- shared helpers ----

    private static IResult NotImplementedChannel(ChannelType type) =>
        Results.Problem(
            detail: $"{type} channels are not yet supported.",
            statusCode: StatusCodes.Status501NotImplemented);

    private static async Task UpsertWidgetIndexAsync(IDocumentStore store, string widgetId, string slug, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new WidgetIndex { Id = $"widget-index/{widgetId}", Slug = slug }, ct);
        await session.SaveChangesAsync(ct);
    }

    /// <summary>Loads the winning binding after a cluster-tx conflict, retrying
    /// until the Raft-committed doc becomes visible on this node (~500ms budget).
    /// Single reads (plain or cluster-wide) flake here because the document
    /// apply lags the commit; this poll is the working safety net pending the
    /// correct wait-for-index read (ayende PR review thread, 2026-06-07).</summary>
    private static async Task<ChannelBinding?> LoadBindingWithRetryAsync(
        IDocumentStore store, string database, string bindingId, CancellationToken ct)
    {
        const int maxAttempts = 10;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            using (var session = store.OpenAsyncSession(database))
            {
                var binding = await session.LoadAsync<ChannelBinding>(bindingId, ct);
                if (binding is not null)
                    return binding;
            }

            await Task.Delay(50, ct);
        }

        return null;
    }

    /// <summary>Validates + normalizes <paramref name="origins"/> in place to the
    /// canonical <c>scheme://authority</c> form. Returns false with an error on
    /// the first invalid entry.</summary>
    private static bool TryNormalizeOrigins(string[] origins, out string? error)
    {
        error = null;

        if (origins.Length > MaxAllowedOrigins)
        {
            error = $"allowedOrigins exceeds limit of {MaxAllowedOrigins} entries";
            return false;
        }

        for (var i = 0; i < origins.Length; i++)
        {
            var origin = origins[i];
            if (string.IsNullOrWhiteSpace(origin) || origin.Length > MaxOriginLength)
            {
                error = $"allowedOrigins entry is empty or exceeds {MaxOriginLength} chars";
                return false;
            }

            if (origin == "*")
            {
                error = "wildcard '*' is not an allowed origin; list explicit http(s) origins";
                return false;
            }

            // C3 (Copilot review #4362803113): the browser Origin header is
            // scheme+host[:port] only — reject anything past the authority,
            // except a bare "/" path which Uri normalizes onto origin-only URLs.
            // Also reject userinfo (e.g. https://user:pass@host): a real Origin
            // never carries it, so such an entry would never match at runtime.
            if (!Uri.TryCreate(origin, UriKind.Absolute, out var uri) ||
                (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps) ||
                (uri.AbsolutePath != "" && uri.AbsolutePath != "/") ||
                string.IsNullOrEmpty(uri.Query) == false ||
                string.IsNullOrEmpty(uri.Fragment) == false ||
                string.IsNullOrEmpty(uri.UserInfo) == false)
            {
                error = $"allowedOrigins entry '{origin}' is not an origin (scheme+host[:port] only)";
                return false;
            }

            // C2 (Copilot review #4365219160): normalize on persist so runtime
            // matching at the embed page doesn't have to strip trailing slashes.
            origins[i] = $"{uri.Scheme}://{uri.Authority}";
        }

        return true;
    }

    /// <summary>M4: cap DisplayName length and forbid control chars at intake
    /// (defends against operator-on-operator stored XSS if the dashboard ever
    /// renders DisplayName unescaped).</summary>
    private static bool TryValidateDisplayName(string? displayName, out string? error)
    {
        error = null;
        if (displayName is null)
            return true;

        if (displayName.Length > MaxDisplayNameLength)
        {
            error = $"displayName exceeds {MaxDisplayNameLength} chars";
            return false;
        }

        if (displayName.Any(char.IsControl))
        {
            error = "displayName contains control characters";
            return false;
        }

        return true;
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class ChannelsLogger;
}
