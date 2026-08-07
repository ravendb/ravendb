using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Session;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Raven;
using Raven.Quill.Telegram;
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
        ITelegramChannelManager telegramManager,
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
            ChannelType.Telegram => await ProvisionTelegramAsync(app, body, store, telegramManager, logger, ct),
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

        var channelId = Guid.NewGuid().ToString("N");

        await session.StoreAsync(new Channel
        {
            Id = Channel.IdPrefix + channelId,
            Type = ChannelType.IFrame,
            DisplayName = body.DisplayName ?? ChannelType.IFrame.ToString(),
            AgentId = config.Identifier,
            AllowedOrigins = origins,
            Enabled = true,
            CreatedAt = DateTime.UtcNow,
        }, ct);

        await session.SaveChangesAsync(ct);

        logger.LogInformation(
            "Provisioned iFrame channel slug={Slug} channelId={ChannelId} agentId={AgentId}",
            app.Slug, channelId, config.Identifier);

        return Results.Ok(new ProvisionChannelResponse(channelId));
    }

    private static async Task<IResult> ProvisionTelegramAsync(
        App app,
        ProvisionChannelRequest body,
        IDocumentStore store,
        ITelegramChannelManager telegramManager,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        var config = await AgentLookup.FindAsync(store, app.Database, body.AgentId, ct);
        if (config is null)
            return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{body.AgentId}'"));

        if (string.IsNullOrWhiteSpace(body.BotToken))
            return Results.BadRequest(new ApiErrorResponse("botToken is required for a Telegram channel"));

        if (body.AllowedOrigins is { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("allowedOrigins does not apply to Telegram channels"));

        if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
            return Results.BadRequest(new ApiErrorResponse(nameError!));

        // bind params at provision (mint-time-binding analogue); the Telegram user id is injected per message
        if (TryResolveTelegramParameters(config, body.Parameters, out var parameters, out var paramError) == false)
            return Results.BadRequest(new ApiErrorResponse(paramError!, Code: "missing_parameters"));

        var botToken = body.BotToken.Trim();
        var (bot, botError) = await telegramManager.ValidateBotTokenAsync(botToken, ct);
        if (bot is null)
            return Results.BadRequest(new ApiErrorResponse(botError!));

        using var session = store.OpenAsyncSession(app.Database);

        var existing = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);
        if (existing.Any(c => c.Type == ChannelType.Telegram && c.Telegram?.BotId == bot.Id))
            return Results.BadRequest(new ApiErrorResponse($"bot @{bot.Username} is already connected in this app"));

        var channelId = Guid.NewGuid().ToString("N");
        var channel = new Channel
        {
            Id = Channel.IdPrefix + channelId,
            Type = ChannelType.Telegram,
            DisplayName = body.DisplayName ?? (string.IsNullOrEmpty(bot.Username) ? "Telegram" : "@" + bot.Username),
            AgentId = config.Identifier,
            AllowedOrigins = [],
            Enabled = true,
            CreatedAt = DateTime.UtcNow,
            Telegram = new TelegramSettings
            {
                BotToken = botToken,
                BotId = bot.Id,
                BotUsername = bot.Username ?? "",
                Parameters = parameters,
            },
        };

        await session.StoreAsync(channel, ct);
        await session.SaveChangesAsync(ct);

        await telegramManager.StartOrRestartAsync(app.Database, channel);

        logger.LogInformation(
            "Provisioned Telegram channel slug={Slug} channelId={ChannelId} agentId={AgentId} bot={Bot}",
            app.Slug, channelId, config.Identifier, TelegramSettings.RedactToken(botToken));

        return Results.Ok(new ProvisionChannelResponse(channelId));
    }

    private static IResult ProvisionWhatsAppAsync() => NotImplementedChannel(ChannelType.WhatsApp);

    /// The agent's declared parameters must be operator-bound now, except the auto-bound ones (user
    /// identifier and Telegram username); the poller fills those from the sender on every message.
    private static bool TryResolveTelegramParameters(
        AiAgentConfiguration config,
        Dictionary<string, string>? supplied,
        out Dictionary<string, string> resolved,
        out string? error)
    {
        error = null;
        if (AgentParameters.TryResolve(config, supplied, out resolved, out var missing))
            return true;

        missing.RemoveAll(TelegramSettings.IsAutoBoundParameter);
        if (missing.Count == 0)
            return true;

        error = $"missing agent parameter(s): {string.Join(", ", missing)}";
        return false;
    }

    private static async Task<IResult> ListChannelsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

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
        ITelegramChannelManager telegramManager,
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
            ChannelType.Telegram => await UpdateTelegramChannelAsync(session, channel, body, app, channelId, telegramManager, logger, ct),
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

    private static async Task<IResult> UpdateTelegramChannelAsync(
        IAsyncDocumentSession session,
        Channel channel,
        UpdateChannelRequest body,
        App app,
        string channelId,
        ITelegramChannelManager telegramManager,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (body.AllowedOrigins is not null)
            return Results.BadRequest(new ApiErrorResponse("allowedOrigins does not apply to Telegram channels"));

        if (body.DisplayName is not null)
        {
            if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
                return Results.BadRequest(new ApiErrorResponse(nameError!));
            channel.DisplayName = body.DisplayName;
        }

        var tokenRotated = false;
        if (string.IsNullOrWhiteSpace(body.BotToken) == false)
        {
            var botToken = body.BotToken.Trim();
            var (bot, botError) = await telegramManager.ValidateBotTokenAsync(botToken, ct);
            if (bot is null)
                return Results.BadRequest(new ApiErrorResponse(botError!));

            var existing = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);
            if (existing.Any(c => c.Id != channel.Id && c.Type == ChannelType.Telegram && c.Telegram?.BotId == bot.Id))
                return Results.BadRequest(new ApiErrorResponse($"bot @{bot.Username} is already connected in this app"));

            channel.Telegram ??= new TelegramSettings();
            channel.Telegram.BotToken = botToken;
            channel.Telegram.BotId = bot.Id;
            channel.Telegram.BotUsername = bot.Username ?? "";
            tokenRotated = true;
        }

        var wasEnabled = channel.Enabled;
        if (body.Enabled is not null)
            channel.Enabled = body.Enabled.Value;

        await session.SaveChangesAsync(ct);

        var runnable = channel is { Enabled: true, Telegram.BotToken.Length: > 0 };
        if (runnable && (tokenRotated || wasEnabled == false))
            await telegramManager.StartOrRestartAsync(app.Database, channel);
        else if (channel.Enabled == false)
            await telegramManager.StopAsync(app.Database, channelId);

        logger.LogInformation(
            "Updated Telegram channel slug={Slug} channelId={ChannelId} enabled={Enabled} tokenRotated={TokenRotated}",
            app.Slug, channelId, channel.Enabled, tokenRotated);

        return Results.Ok(ChannelSummaryResponse.From(channel));
    }

    private static IResult UpdateWhatsAppChannelAsync() => NotImplementedChannel(ChannelType.WhatsApp);


    private static async Task<IResult> DeleteChannelAsync(
        string slug,
        string channelId,
        IDocumentStore store,
        ITelegramChannelManager telegramManager,
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
            ChannelType.Telegram => await DeleteTelegramChannelAsync(store, app, channelId, telegramManager, logger, ct),
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

    private static async Task<IResult> DeleteTelegramChannelAsync(
        IDocumentStore store,
        App app,
        string channelId,
        ITelegramChannelManager telegramManager,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        using (var session = store.OpenAsyncSession(app.Database))
        {
            session.Delete(Channel.IdPrefix + channelId);
            await session.SaveChangesAsync(ct);
        }

        await telegramManager.StopAsync(app.Database, channelId);

        logger.LogInformation("Deleted Telegram channel slug={Slug} channelId={ChannelId}", app.Slug, channelId);
        return Results.NoContent();
    }

    private static IResult DeleteWhatsAppChannelAsync() => NotImplementedChannel(ChannelType.WhatsApp);

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
