using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Wizard;

namespace Raven.Quill.Endpoints;

public static class ChannelsEndpoints
{
    private const int MaxAllowedOrigins = 32;
    private const int MaxOriginLength = 256;
    private const int MaxDisplayNameLength = 200;

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("channels").RequireAuthorization();

        group.MapPost("/setup/channel", ProvisionChannelAsync)
            .WithName("channels.create")
            .WithDescription(
                "Registers a channel for the app. allowedOrigins is required; entries are " +
                "normalized to scheme://authority (max 32). An explicit empty list is the " +
                "opt-in contract: the embed page emits no CSP frame-ancestors header and is " +
                "embeddable from any site. Each POST creates a new channel; multiple channels " +
                "may target the same agent (e.g. different sites, origins, or themes). " +
                "Edit via PUT /channels/{id}, remove via DELETE /channels/{id}.")
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


    private static async Task<IResult> ProvisionChannelAsync(
        string slug,
        ProvisionChannelRequest body,
        IDocumentStore store,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.AgentId))
            return Results.BadRequest(new ApiErrorResponse("agentId is required"));

        // load App first: unknown slug => 404, don't leak which agentIds exist
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        return body.Type switch
        {
            ChannelType.IFrame => await ProvisionIFrameAsync(app, body, store, logger, ct),
            ChannelType.Telegram => ProvisionTelegramAsync(),
            ChannelType.WhatsApp => ProvisionWhatsAppAsync(),
            null => Results.BadRequest(new ApiErrorResponse("type is required")),
            _ => Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{body.Type}'")),
        };
    }

    private static async Task<IResult> ProvisionIFrameAsync(
        App app,
        ProvisionChannelRequest body,
        IDocumentStore store,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        var config = await AgentLookup.FindAsync(store, app.Database, body.AgentId, ct);
        if (config is null)
            return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{body.AgentId}'"));

        // open embed must be explicit (allowedOrigins: []); an omitted list => 400
        if (body.AllowedOrigins is null)
            return Results.BadRequest(new ApiErrorResponse(
                "allowedOrigins is required; pass an empty array to make the embed page embeddable from anywhere"));

        var origins = body.AllowedOrigins;
        if (TryNormalizeOrigins(origins, out var originError) == false)
            return Results.BadRequest(new ApiErrorResponse(originError!));

        if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
            return Results.BadRequest(new ApiErrorResponse(nameError!));

        using var session = store.OpenAsyncSession(app.Database);

        var widgetId = "wgt_" + Guid.NewGuid().ToString("N");

        await session.StoreAsync(new Channel
        {
            Id = Channel.IdPrefix + widgetId,
            Type = ChannelType.IFrame,
            DisplayName = body.DisplayName ?? ChannelType.IFrame.ToString(),
            AgentId = config.Identifier,
            AllowedOrigins = origins,
            Enabled = true,
            CreatedAt = DateTime.UtcNow,
        }, ct);

        await session.SaveChangesAsync(ct);

        logger.LogInformation(
            "Provisioned iFrame channel slug={Slug} widgetId={WidgetId} agentId={AgentId}",
            app.Slug, widgetId, config.Identifier);

        return Results.Ok(new ProvisionChannelResponse(widgetId));
    }

    private static IResult ProvisionTelegramAsync() => NotImplementedChannel(ChannelType.Telegram);

    private static IResult ProvisionWhatsAppAsync() => NotImplementedChannel(ChannelType.WhatsApp);


    private static async Task<IResult> ListChannelsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channels = await LoadAllChannelsAsync(session, ct);

        var items = channels
            .OrderByDescending(c => c.CreatedAt)
            .Select(ChannelSummaryResponse.From)
            .ToArray();

        return Results.Ok(items);
    }


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
        using (var session = store.OpenAsyncSession(app.Database))
        {
            session.Delete(Channel.IdPrefix + channelId);
            await session.SaveChangesAsync(ct);
        }

        logger.LogInformation("Deleted iFrame channel slug={Slug} channelId={ChannelId}", app.Slug, channelId);
        return Results.NoContent();
    }

    private static IResult DeleteTelegramChannelAsync() => NotImplementedChannel(ChannelType.Telegram);

    private static IResult DeleteWhatsAppChannelAsync() => NotImplementedChannel(ChannelType.WhatsApp);


    // LoadStartingWith: immediately consistent, no post-create index wait; page fully
    private static async Task<List<Channel>> LoadAllChannelsAsync(IAsyncDocumentSession session, CancellationToken ct)
    {
        const int pageSize = 1024;
        var channels = new List<Channel>();
        for (var start = 0;; start += pageSize)
        {
            var page = (await session.Advanced.LoadStartingWithAsync<Channel>(
                Channel.IdPrefix, start: start, pageSize: pageSize, token: ct)).ToArray();
            channels.AddRange(page);
            if (page.Length < pageSize)
                break;
        }

        return channels;
    }

    private static IResult NotImplementedChannel(ChannelType type) =>
        Results.Problem(
            detail: $"{type} channels are not yet supported.",
            statusCode: StatusCodes.Status501NotImplemented);

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

            origins[i] = $"{uri.Scheme}://{uri.Authority}";
        }

        return true;
    }

    // cap length + forbid control chars: operator-on-operator stored-XSS guard
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

    internal sealed class ChannelsLogger;
}
