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
using Raven.Quill.Slack;
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
        ISlackClient slackClient,
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
            ChannelType.Slack => await ProvisionSlackAsync(app, body, store, slackClient, logger, ct),
            null => Results.BadRequest(new ApiErrorResponse("type is required")),
            _ => Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{body.Type}'")),
        };
    }

    private static IResult? RejectForeignSettings(ChannelType type, bool hasTelegram, bool hasSlack)
    {
        if (type != ChannelType.Telegram && hasTelegram)
            return Results.BadRequest(new ApiErrorResponse("telegram settings apply to Telegram channels only"));

        if (type != ChannelType.Slack && hasSlack)
            return Results.BadRequest(new ApiErrorResponse("slack settings apply to Slack channels only"));

        return null;
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

        if (RejectForeignSettings(ChannelType.IFrame, body.Telegram is not null, body.Slack is not null) is { } foreignSettings)
            return foreignSettings;

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

        if (RejectForeignSettings(ChannelType.Telegram, body.Telegram is not null, body.Slack is not null) is { } foreignSettings)
            return foreignSettings;

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

    private static async Task<IResult> ProvisionSlackAsync(
        App app,
        ProvisionChannelRequest body,
        IDocumentStore store,
        ISlackClient slackClient,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        var config = await AgentLookup.FindAsync(store, app.Database, body.AgentId, ct);
        if (config is null)
            return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{body.AgentId}'"));

        if (RejectForeignSettings(ChannelType.Slack, body.Telegram is not null, body.Slack is not null) is { } foreignSettings)
            return foreignSettings;

        if (body.AllowedOrigins is { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("allowedOrigins does not apply to Slack channels"));

        var botToken = body.Slack?.BotToken?.Trim();
        if (string.IsNullOrEmpty(botToken))
            return Results.BadRequest(new ApiErrorResponse("slack.botToken is required for a Slack channel"));

        if (botToken.StartsWith("xoxb-", StringComparison.Ordinal) == false)
            return Results.BadRequest(new ApiErrorResponse(
                "slack.botToken must be the bot token (xoxb-) from the Slack app's OAuth page"));

        var signingSecret = body.Slack!.SigningSecret?.Trim();
        if (string.IsNullOrEmpty(signingSecret))
            return Results.BadRequest(new ApiErrorResponse("slack.signingSecret is required for a Slack channel"));

        if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
            return Results.BadRequest(new ApiErrorResponse(nameError!));

        if (SlackParameterBindings.TryResolve(config, body.Slack.ParameterBindings, out var bindings, out var paramError) == false)
            return Results.BadRequest(new ApiErrorResponse(paramError!, Code: "missing_parameters"));

        var (auth, authError, _) = await slackClient.AuthTestAsync(botToken, ct);
        if (auth is null)
            return Results.BadRequest(new ApiErrorResponse(authError!));

        var channelId = Guid.NewGuid().ToString("N");
        var channel = new Channel
        {
            Id = Channel.IdPrefix + channelId,
            Type = ChannelType.Slack,
            DisplayName = body.DisplayName
                          ?? (string.IsNullOrEmpty(auth.BotName) ? auth.TeamName : auth.BotName),
            AgentId = config.Identifier,
            AllowedOrigins = [],
            Enabled = true,
            CreatedAt = DateTime.UtcNow,
            Slack = new SlackSettings
            {
                TeamId = auth.TeamId,
                TeamName = auth.TeamName,
                BotUserId = auth.BotUserId,
                BotId = auth.BotId,
                BotToken = botToken,
                SigningSecret = signingSecret,
                WebhookToken = Guid.NewGuid().ToString("N"),
                ConnectedAt = DateTime.UtcNow,
                ParameterBindings = bindings,
            },
        };

        var (reserved, owner) = await TryReserveSlackBotAsync(
            store, auth.TeamId, auth.BotUserId, app.Database, channel.Id!, channel.Slack.WebhookToken, ct);
        if (reserved == false)
            return Results.BadRequest(new ApiErrorResponse(SlackBotAlreadyConnected(auth.TeamName, auth.BotUserId, owner)));

        try
        {
            using var session = store.OpenAsyncSession(app.Database);
            await session.StoreAsync(channel, ct);
            await session.SaveChangesAsync(ct);
        }
        catch
        {
            await TryReleaseSlackAsync(
                store, auth.TeamId, auth.BotUserId, channel.Slack.WebhookToken, app.Database, channel.Id!, logger);
            throw;
        }

        logger.LogInformation(
            "Provisioned Slack channel slug={Slug} channelId={ChannelId} agentId={AgentId} teamId={TeamId} botUserId={BotUserId}",
            app.Slug, channelId, config.Identifier, auth.TeamId, auth.BotUserId);

        return Results.Ok(new ProvisionChannelResponse(channelId));
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
        ISlackClient slackClient,
        SlackHealthRegistry slackHealth,
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
            ChannelType.Slack => await UpdateSlackChannelAsync(session, channel, body, app, channelId, store, slackClient, slackHealth, logger, ct),
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
        if (RejectForeignSettings(ChannelType.IFrame, body.Telegram is not null, body.Slack is not null) is { } foreignSettings)
            return foreignSettings;

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
        if (RejectForeignSettings(ChannelType.Telegram, body.Telegram is not null, body.Slack is not null) is { } foreignSettings)
            return foreignSettings;

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

    private static async Task<IResult> UpdateSlackChannelAsync(
        IAsyncDocumentSession session,
        Channel channel,
        UpdateChannelRequest body,
        App app,
        string channelId,
        IDocumentStore store,
        ISlackClient slackClient,
        SlackHealthRegistry health,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        if (RejectForeignSettings(ChannelType.Slack, body.Telegram is not null, body.Slack is not null) is { } foreignSettings)
            return foreignSettings;

        if (body.AllowedOrigins is { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("allowedOrigins does not apply to Slack channels"));

        if (body.DisplayName is not null)
        {
            if (TryValidateDisplayName(body.DisplayName, out var nameError) == false)
                return Results.BadRequest(new ApiErrorResponse(nameError!));
            channel.DisplayName = body.DisplayName;
        }

        var settings = channel.Slack ??= new SlackSettings();

        var tokenRotated = false;
        if (string.IsNullOrWhiteSpace(body.Slack?.BotToken) == false)
        {
            var botToken = body.Slack.BotToken.Trim();
            if (botToken.StartsWith("xoxb-", StringComparison.Ordinal) == false)
                return Results.BadRequest(new ApiErrorResponse(
                    "slack.botToken must be the bot token (xoxb-) from the Slack app's OAuth page"));

            var (auth, authError, _) = await slackClient.AuthTestAsync(botToken, ct);
            if (auth is null)
                return Results.BadRequest(new ApiErrorResponse(authError!));

            if (auth.TeamId != settings.TeamId || auth.BotUserId != settings.BotUserId)
                return Results.BadRequest(new ApiErrorResponse(
                    "the token belongs to a different workspace or bot; connect it as a new channel instead"));

            settings.BotToken = botToken;
            settings.TeamName = auth.TeamName;
            settings.BotId = auth.BotId;
            tokenRotated = true;
        }

        var secretRotated = false;
        if (string.IsNullOrWhiteSpace(body.Slack?.SigningSecret) == false)
        {
            settings.SigningSecret = body.Slack.SigningSecret.Trim();
            secretRotated = true;
        }

        if (body.Slack?.ParameterBindings is { } suppliedBindings)
        {
            var config = await AgentLookup.FindAsync(store, app.Database, channel.AgentId, ct);
            if (config is null)
                return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{channel.AgentId}'"));

            if (SlackParameterBindings.TryResolve(config, suppliedBindings, out var bindings, out var paramError) == false)
                return Results.BadRequest(new ApiErrorResponse(paramError!, Code: "missing_parameters"));

            settings.ParameterBindings = bindings;
        }

        if (body.Enabled is not null)
            channel.Enabled = body.Enabled.Value;

        await session.SaveChangesAsync(ct);

        if (tokenRotated)
            health.InvalidateTokenCheck(app.Database, channel.ShortId);

        logger.LogInformation(
            "Updated Slack channel slug={Slug} channelId={ChannelId} enabled={Enabled} tokenRotated={TokenRotated} secretRotated={SecretRotated}",
            app.Slug, channelId, channel.Enabled, tokenRotated, secretRotated);

        return Results.Ok(ChannelSummaryResponse.From(channel));
    }


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
            ChannelType.Slack => await DeleteSlackChannelAsync(session, channel, app, channelId, store, logger, ct),
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

    private static async Task<IResult> DeleteSlackChannelAsync(
        IAsyncDocumentSession session,
        Channel channel,
        App app,
        string channelId,
        IDocumentStore store,
        ILogger<ChannelsLogger> logger,
        CancellationToken ct)
    {
        session.Delete(channel);
        await session.SaveChangesAsync(ct);

        if (channel.Slack is { TeamId.Length: > 0, BotUserId.Length: > 0 } settings)
            await TryReleaseSlackAsync(
                store, settings.TeamId, settings.BotUserId, settings.WebhookToken, app.Database, channel.Id!, logger);

        logger.LogInformation("Deleted Slack channel slug={Slug} channelId={ChannelId}", app.Slug, channelId);
        return Results.NoContent();
    }

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

    private static string SlackBotAlreadyConnected(string teamName, string botUserId, string? ownerDatabase)
    {
        var bot = string.IsNullOrEmpty(teamName) ? botUserId : $"{botUserId} in workspace '{teamName}'";
        return ownerDatabase is null
            ? $"Slack bot {bot} is already connected"
            : $"Slack bot {bot} is already connected in app '{ownerDatabase}'";
    }

    private static async Task<(bool Reserved, string? OwnerDatabase)> TryReserveSlackBotAsync(
        IDocumentStore store, string teamId, string botUserId, string database, string channelId, string webhookToken, CancellationToken ct)
    {
        using var configSession = store.OpenAsyncSession();

        var reservationId = SlackBotReservation.IdFor(teamId, botUserId);
        var reservation = await configSession.LoadAsync<SlackBotReservation>(reservationId, ct);
        if (reservation is not null && reservation.Database == database && reservation.ChannelId == channelId)
            return (true, null);

        if (reservation is null)
        {
            await configSession.StoreAsync(
                new SlackBotReservation { Database = database, ChannelId = channelId },
                string.Empty, reservationId, ct);
        }
        else
        {
            if (await IsSlackReservationLiveAsync(store, reservation, teamId, botUserId, ct))
                return (false, reservation.Database);

            reservation.Database = database;
            reservation.ChannelId = channelId;
            await configSession.StoreAsync(
                reservation, configSession.Advanced.GetChangeVectorFor(reservation), reservationId, ct);
        }

        await configSession.StoreAsync(
            new SlackWebhookRoute { Database = database, ChannelId = channelId },
            SlackWebhookRoute.IdFor(webhookToken), ct);

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

    private static async Task<bool> IsSlackReservationLiveAsync(
        IDocumentStore store, SlackBotReservation reservation, string teamId, string botUserId, CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession(reservation.Database);
            var channel = await session.LoadAsync<Channel>(reservation.ChannelId, ct);
            return channel?.Slack is { } settings && settings.TeamId == teamId && settings.BotUserId == botUserId;
        }
        catch (DatabaseDoesNotExistException)
        {
            return false;
        }
    }

    private static async Task TryReleaseSlackAsync(
        IDocumentStore store, string teamId, string botUserId, string webhookToken, string database, string channelId, ILogger<ChannelsLogger> logger)
    {
        try
        {
            using var configSession = store.OpenAsyncSession();

            var reservation = await configSession.LoadAsync<SlackBotReservation>(
                SlackBotReservation.IdFor(teamId, botUserId));
            if (reservation is not null && reservation.Database == database && reservation.ChannelId == channelId)
                configSession.Delete(reservation);

            var route = await configSession.LoadAsync<SlackWebhookRoute>(
                SlackWebhookRoute.IdFor(webhookToken));
            if (route is not null && route.Database == database && route.ChannelId == channelId)
                configSession.Delete(route);

            await configSession.SaveChangesAsync();
        }
        catch (Exception e)
        {
            logger.LogWarning(
                "Slack bot reservation for {TeamId}/{BotUserId} was not released: {Error}",
                teamId, botUserId, e.Message);
        }
    }

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
