using Raven.Client.Documents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Discord;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Raven;

namespace Raven.Quill.Endpoints;

public static class DiscordEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("discord").RequireAuthorization();

        group.MapGet("/discord/health", GetHealthAsync)
            .WithName("discord.health")
            .WithDescription(
                "Per-channel connection health for the app's Discord channels: bot token validity plus the live " +
                "gateway connection state and the inbound and send activity seen since the channel's gateway " +
                "last started.")
            .Produces<DiscordChannelHealthResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetHealthAsync(
        string slug,
        IDocumentStore store,
        IDiscordClient discordClient,
        IDiscordChannelManager discordManager,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

        var discordChannels = channels
            .Where(c => c is { Type: ChannelType.Discord, Discord: not null })
            .OrderByDescending(c => c.CreatedAt)
            .ToArray();

        var checks = new (bool? Valid, string? Error)[discordChannels.Length];
        await Task.WhenAll(discordChannels.Select(async (channel, i) =>
        {
            var (identity, error, discordResponded) =
                await discordClient.GetBotIdentityAsync(channel.Discord!.BotToken, ct);
            checks[i] = (identity is not null ? true : discordResponded ? false : null, identity is null ? error : null);
        }));

        var rows = new DiscordChannelHealthResponse[discordChannels.Length];
        for (var i = 0; i < discordChannels.Length; i++)
        {
            var channel = discordChannels[i];
            var settings = channel.Discord!;
            var health = discordManager.HealthFor(app.Database, channel.ShortId);
            rows[i] = new DiscordChannelHealthResponse(
                channel.ShortId,
                settings.ApplicationId,
                settings.BotUserId,
                settings.BotUsername,
                channel.Enabled,
                checks[i].Valid,
                checks[i].Error,
                health?.IsConnected ?? false,
                health?.LastConnectedAt,
                health?.LastConnectionError,
                health?.LastInboundAt,
                health?.LastSendErrorAt,
                health?.LastSendError);
        }

        return Results.Ok(rows);
    }
}
