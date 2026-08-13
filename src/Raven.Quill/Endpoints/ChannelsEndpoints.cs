using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Raven;
using Raven.Quill.Telegram;
using Raven.Quill.Wizard;
using TelegramUser = Telegram.Bot.Types.User;

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

        if (body.Telegram is not null)
            return Results.BadRequest(new ApiErrorResponse("telegram settings apply to Telegram channels only"));

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

        if (string.IsNullOrWhiteSpace(body.Telegram?.BotToken))
            return Results.BadRequest(new ApiErrorResponse("telegram.botToken is required for a Telegram channel"));

        if (body.AllowedOrigins is { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("allowedOrigins does not apply to Telegram channels"));

        if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
            return Results.BadRequest(new ApiErrorResponse(nameError!));

        if (TelegramParameterBindings.TryResolve(config, body.Telegram.ParameterBindings, out var bindings, out var paramError) == false)
            return Results.BadRequest(new ApiErrorResponse(paramError!, Code: "missing_parameters"));

        var botToken = body.Telegram.BotToken.Trim();
        var (bot, botError) = await telegramManager.ValidateBotTokenAsync(botToken, ct);
        if (bot is null)
            return Results.BadRequest(new ApiErrorResponse(botError!));

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
                ParameterBindings = bindings,
            },
        };

        var (reserved, owner) = await TryReserveBotAsync(store, bot.Id, app.Database, channel.Id!, ct);
        if (reserved == false)
            return Results.BadRequest(new ApiErrorResponse(AlreadyConnected(bot.Username, owner)));

        // reserved before the channel exists: a crash here leaves an orphan the next attempt reclaims
        try
        {
            using var session = store.OpenAsyncSession(app.Database);
            await session.StoreAsync(channel, ct);
            await session.SaveChangesAsync(ct);
        }
        catch
        {
            await TryReleaseBotAsync(store, bot.Id, app.Database, channel.Id!, logger);
            throw;
        }

        telegramManager.Wake();

        logger.LogInformation(
            "Provisioned Telegram channel slug={Slug} channelId={ChannelId} agentId={AgentId} bot=@{Bot}",
            app.Slug, channelId, config.Identifier, bot.Username);

        return Results.Ok(new ProvisionChannelResponse(channelId));
    }

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
            ChannelType.Telegram => await UpdateTelegramChannelAsync(session, channel, body, app, channelId, store, telegramManager, logger, ct),
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
        if (body.Telegram is not null)
            return Results.BadRequest(new ApiErrorResponse("telegram settings apply to Telegram channels only"));

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
        IDocumentStore store,
        ITelegramChannelManager telegramManager,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (body.AllowedOrigins is { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("allowedOrigins does not apply to Telegram channels"));

        if (body.DisplayName is not null)
        {
            if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
                return Results.BadRequest(new ApiErrorResponse(nameError!));
            channel.DisplayName = body.DisplayName;
        }

        var tokenRotated = false;
        TelegramUser? rotatedBot = null;
        var previousBotId = 0L;
        if (string.IsNullOrWhiteSpace(body.Telegram?.BotToken) == false)
        {
            var botToken = body.Telegram.BotToken.Trim();
            var (bot, botError) = await telegramManager.ValidateBotTokenAsync(botToken, ct);
            if (bot is null)
                return Results.BadRequest(new ApiErrorResponse(botError!));

            if (bot.Id != channel.Telegram?.BotId)
            {
                rotatedBot = bot;
                previousBotId = channel.Telegram?.BotId ?? 0;
            }

            channel.Telegram ??= new TelegramSettings();
            channel.Telegram.BotToken = botToken;
            channel.Telegram.BotId = bot.Id;
            channel.Telegram.BotUsername = bot.Username ?? "";
            tokenRotated = true;
        }

        if (body.Telegram?.Messages is { } messages)
        {
            if (messages.TryNormalize(out var messagesError) == false)
                return Results.BadRequest(new ApiErrorResponse(messagesError!));

            channel.Telegram ??= new TelegramSettings();
            channel.Telegram.Messages = messages.HasAnyOverride ? messages : null;
        }

        if (body.Telegram?.ParameterBindings is { } suppliedBindings)
        {
            var config = await AgentLookup.FindAsync(store, app.Database, channel.AgentId, ct);
            if (config is null)
                return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{channel.AgentId}'"));

            if (TelegramParameterBindings.TryResolve(config, suppliedBindings, out var bindings, out var paramError) == false)
                return Results.BadRequest(new ApiErrorResponse(paramError!, Code: "missing_parameters"));

            channel.Telegram ??= new TelegramSettings();
            channel.Telegram.ParameterBindings = bindings;
        }

        if (body.Enabled is not null)
            channel.Enabled = body.Enabled.Value;

        if (rotatedBot is not null)
        {
            var (reserved, owner) = await TryReserveBotAsync(store, rotatedBot.Id, app.Database, channel.Id!, ct);
            if (reserved == false)
                return Results.BadRequest(new ApiErrorResponse(AlreadyConnected(rotatedBot.Username, owner)));
        }

        try
        {
            await session.SaveChangesAsync(ct);
        }
        catch
        {
            if (rotatedBot is not null)
                await TryReleaseBotAsync(store, rotatedBot.Id, app.Database, channel.Id!, logger);
            throw;
        }

        // the old token stays reserved until the rotate is durable
        if (rotatedBot is not null && previousBotId > 0)
            await TryReleaseBotAsync(store, previousBotId, app.Database, channel.Id!, logger);

        telegramManager.Wake();

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

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no channel '{channelId}' in app '{slug}'"));

        return channel.Type switch
        {
            ChannelType.IFrame => await DeleteIFrameChannelAsync(session, channel, app.Slug, channelId, logger, ct),
            ChannelType.Telegram => await DeleteTelegramChannelAsync(session, channel, app, channelId, store, telegramManager, logger, ct),
            ChannelType.WhatsApp => DeleteWhatsAppChannelAsync(),
            _ => Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{channel.Type}'")),
        };
    }

    private static async Task<IResult> DeleteIFrameChannelAsync(
        IAsyncDocumentSession session,
        Channel channel,
        string slug,
        string channelId,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        session.Delete(channel);
        await session.SaveChangesAsync(ct);

        logger.LogInformation("Deleted iFrame channel slug={Slug} channelId={ChannelId}", slug, channelId);
        return Results.NoContent();
    }

    private static async Task<IResult> DeleteTelegramChannelAsync(
        IAsyncDocumentSession session,
        Channel channel,
        App app,
        string channelId,
        IDocumentStore store,
        ITelegramChannelManager telegramManager,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        session.Delete(channel);
        await session.SaveChangesAsync(ct);

        if (channel.Telegram?.BotId is > 0)
            await TryReleaseBotAsync(store, channel.Telegram.BotId, app.Database, channel.Id!, logger);

        telegramManager.Wake();

        logger.LogInformation("Deleted Telegram channel slug={Slug} channelId={ChannelId}", app.Slug, channelId);
        return Results.NoContent();
    }

    private static IResult DeleteWhatsAppChannelAsync() => NotImplementedChannel(ChannelType.WhatsApp);

    private static string AlreadyConnected(string? botUsername, string? ownerDatabase) =>
        ownerDatabase is null
            ? $"bot @{botUsername} is already connected"
            : $"bot @{botUsername} is already connected in app '{ownerDatabase}'";

    private static async Task<(bool Reserved, string? OwnerDatabase)> TryReserveBotAsync(
        IDocumentStore store, long botId, string database, string channelId, CancellationToken ct)
    {
        using var configSession = store.OpenAsyncSession();

        var reservationId = TelegramBotReservation.IdFor(botId);
        var reservation = await configSession.LoadAsync<TelegramBotReservation>(reservationId, ct);
        if (reservation is not null && reservation.Database == database && reservation.ChannelId == channelId)
            return (true, null);

        if (reservation is null)
        {
            await configSession.StoreAsync(
                new TelegramBotReservation { Database = database, ChannelId = channelId },
                string.Empty, reservationId, ct);
        }
        else
        {
            if (await IsReservationLiveAsync(store, reservation, botId, ct))
                return (false, reservation.Database);

            // a reservation without a live matching channel is an orphan; reclaim it under its change vector
            reservation.Database = database;
            reservation.ChannelId = channelId;
            await configSession.StoreAsync(
                reservation, configSession.Advanced.GetChangeVectorFor(reservation), reservationId, ct);
        }

        try
        {
            await configSession.SaveChangesAsync(ct);
            return (true, null);
        }
        catch (ConcurrencyException)
        {
            return (false, null);
        }
    }

    // live = the reserved channel still exists and still uses this bot
    private static async Task<bool> IsReservationLiveAsync(
        IDocumentStore store, TelegramBotReservation reservation, long botId, CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession(reservation.Database);
            var channel = await session.LoadAsync<Channel>(reservation.ChannelId, ct);
            return channel?.Telegram?.BotId == botId;
        }
        catch (DatabaseDoesNotExistException)
        {
            return false;
        }
    }

    private static async Task TryReleaseBotAsync(
        IDocumentStore store, long botId, string database, string channelId, ILogger<ChannelsLogger> logger)
    {
        var reservationId = TelegramBotReservation.IdFor(botId);
        try
        {
            using var configSession = store.OpenAsyncSession();
            var reservation = await configSession.LoadAsync<TelegramBotReservation>(reservationId);
            if (reservation is null || reservation.Database != database || reservation.ChannelId != channelId)
                return;

            configSession.Delete(reservation);
            await configSession.SaveChangesAsync();
        }
        catch (Exception e)
        {
            // an unreleased reservation is an orphan the next reserve attempt reclaims
            logger.LogWarning("Telegram bot reservation {ReservationId} was not released: {Error}", reservationId, e.Message);
        }
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
