using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

public static class EmbedLinksEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("embed-links").RequireAuthorization();

        group.MapGet("/embed-links", ListAsync)
            .WithName("embedLinks.list")
            .WithDescription(
                "Lists the app's active embed links (non-expired, non-revoked), most recent first. " +
                "Each item carries its token, the channel + agent it targets, the bound parameters, " +
                "the TTL/cap, and how many turns it has consumed — so the operator can audit and revoke links.")
            .Produces<EmbedLinkSummaryResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPost("/embed-links", MintAsync)
            .WithName("embedLinks.mint")
            .WithDescription(
                "Mints a per-user embed link for an agent's iFrame channel. Parameters are " +
                "validated against the agent and bound into the link server-side (never " +
                "client-supplied). ttlSeconds and maxInvocations are bounded; both default " +
                "when omitted. Returns the opaque token + an absolute, paste-ready /embed/{token} URL.")
            .Accepts<MintEmbedLinkRequest>("application/json")
            .Produces<MintEmbedLinkResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapDelete("/embed-links/{token}", RevokeAsync)
            .WithName("embedLinks.revoke")
            .WithDescription("Revokes a minted link; the public embed surface then returns 410 Gone. Idempotent.")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> ListAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);

        const int pageSize = 1024;
        var now = DateTime.UtcNow;
        var links = new List<EmbedLink>();
        for (var start = 0;; start += pageSize)
        {
            var page = (await session.Advanced.LoadStartingWithAsync<EmbedLink>(
                EmbedLink.IdPrefix, start: start, pageSize: pageSize, token: ct)).ToArray();
            links.AddRange(page);
            if (page.Length < pageSize)
                break;
        }

        var items = links
            .Where(l => l.Revoked == false && l.ExpiresAt > now)
            .OrderByDescending(l => l.CreatedAt)
            .Select(EmbedLinkSummaryResponse.From)
            .ToArray();

        return Results.Ok(items);
    }

    private static async Task<IResult> MintAsync(
        string slug,
        MintEmbedLinkRequest body,
        IDocumentStore store,
        ILogger<EmbedLinksLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.AgentId))
            return Results.BadRequest(new ApiErrorResponse("agentId is required"));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var config = await AgentLookup.FindAsync(store, app.Database, body.AgentId, ct);
        if (config is null)
            return Results.NotFound(new ApiErrorResponse($"no agent '{body.AgentId}' in app '{slug}'"));

        var bindingId = $"channel-bindings/{app.Slug}/{ChannelType.IFrame}/{config.Identifier}";
        Channel? channel;
        using (var session = store.OpenAsyncSession(app.Database))
        {
            var binding = await session.LoadAsync<ChannelBinding>(bindingId, ct);
            if (binding is null)
                return Results.NotFound(new ApiErrorResponse(
                    $"no iframe channel for agent '{config.Identifier}' in app '{slug}'"));

            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + binding.WidgetId, ct);
        }

        if (channel is null || channel.Type != ChannelType.IFrame)
            return Results.NotFound(new ApiErrorResponse(
                $"no iframe channel for agent '{config.Identifier}' in app '{slug}'"));

        if (channel.Enabled == false)
            return Results.BadRequest(new ApiErrorResponse(
                $"the iframe channel for agent '{config.Identifier}' is disabled", Code: "channel_disabled"));

        if (AgentParameters.TryResolve(config, body.Parameters, out var parameters, out var missing) == false)
            return Results.BadRequest(new ApiErrorResponse(
                $"missing agent parameter(s): {string.Join(", ", missing)}", Code: "missing_parameters"));

        var ttlSeconds = body.TtlSeconds ?? EmbedLinkLimits.DefaultTtlSeconds;
        if (ttlSeconds < EmbedLinkLimits.MinTtlSeconds || ttlSeconds > EmbedLinkLimits.MaxTtlSeconds)
            return Results.BadRequest(new ApiErrorResponse(
                $"ttlSeconds must be between {EmbedLinkLimits.MinTtlSeconds} and {EmbedLinkLimits.MaxTtlSeconds}"));

        var maxInvocations = body.MaxInvocations ?? EmbedLinkLimits.DefaultMaxInvocations;
        if (maxInvocations < 1 || maxInvocations > EmbedLinkLimits.MaxMaxInvocations)
            return Results.BadRequest(new ApiErrorResponse(
                $"maxInvocations must be between 1 and {EmbedLinkLimits.MaxMaxInvocations}"));

        var token = Guid.NewGuid().ToString("N");
        var expiresAt = DateTime.UtcNow.AddSeconds(ttlSeconds);

        using (var session = store.OpenAsyncSession(app.Database))
        {
            var link = new EmbedLink
            {
                Id = EmbedLink.IdPrefix + token,
                WidgetId = channel.Id!.Substring(Channel.IdPrefix.Length),
                AgentId = config.Identifier,
                Parameters = parameters,
                ExpiresAt = expiresAt,
                MaxInvocations = maxInvocations,
                InvocationCount = 0,
                Revoked = false,
                CreatedAt = DateTime.UtcNow,
            };
            await session.StoreAsync(link, ct);
            session.Advanced.GetMetadataFor(link)[global::Raven.Client.Constants.Documents.Metadata.Expires] = expiresAt;
            await session.SaveChangesAsync(ct);
        }

        using (var cfg = store.OpenAsyncSession())
        {
            var index = new LinkIndex { Id = LinkIndex.IdPrefix + token, Slug = app.Slug };
            await cfg.StoreAsync(index, ct);
            cfg.Advanced.GetMetadataFor(index)[global::Raven.Client.Constants.Documents.Metadata.Expires] = expiresAt;
            await cfg.SaveChangesAsync(ct);
        }

        var publicHost = ApplianceHost.WithSubdomain(ctx.Request.Host, "public");
        var url = $"{ctx.Request.Scheme}://{publicHost.ToUriComponent()}{ctx.Request.PathBase}/embed/{token}";
        logger.LogInformation(
            "Minted embed link slug={Slug} agentId={AgentId} ttlSeconds={Ttl} maxInvocations={Max}",
            app.Slug, config.Identifier, ttlSeconds, maxInvocations);

        return Results.Ok(new MintEmbedLinkResponse(token, url, expiresAt, maxInvocations));
    }

    private static async Task<IResult> RevokeAsync(
        string slug,
        string token,
        IDocumentStore store,
        ILogger<EmbedLinksLogger> logger,
        CancellationToken ct)
    {
        if (EmbedLink.IsWellFormedToken(token) == false)
            return Results.NotFound(new ApiErrorResponse("no such link"));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using (var session = store.OpenAsyncSession(app.Database))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token, ct);
            if (link is null)
                return Results.NoContent();

            if (link.Revoked == false)
            {
                link.Revoked = true;
                await session.SaveChangesAsync(ct);
                logger.LogInformation("Revoked embed link slug={Slug} tokenPrefix={TokenPrefix}", app.Slug, EmbedLink.RedactToken(token));
            }
        }

        return Results.NoContent();
    }

    internal sealed class EmbedLinksLogger;
}
