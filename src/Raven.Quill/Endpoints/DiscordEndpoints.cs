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
                "Per-channel connection health for the app's Discord channels: bot token validity " +
                "(cached a few minutes) plus the live gateway connection state and the inbound and send " +
                "activity seen since the last restart.")
            .Produces<DiscordChannelHealthResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetHealthAsync(
        string slug,
        IDocumentStore store,
        IDiscordClient discordClient,
        DiscordHealthRegistry health,
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
            if (health.TryGetFreshTokenCheck(app.Database, channel.ShortId, out var tokenValid, out var tokenError) == false)
            {
                var (identity, error, discordResponded) =
                    await discordClient.GetBotIdentityAsync(channel.Discord!.BotToken, ct);
                tokenValid = identity is not null ? true : discordResponded ? false : null;
                tokenError = identity is null ? error : null;
                health.StoreTokenCheck(app.Database, channel.ShortId, tokenValid, tokenError);
            }

            checks[i] = (tokenValid, tokenError);
        }));

        var rows = new DiscordChannelHealthResponse[discordChannels.Length];
        for (var i = 0; i < discordChannels.Length; i++)
        {
            var channel = discordChannels[i];
            var settings = channel.Discord!;
            var snapshot = health.SnapshotFor(app.Database, channel.ShortId);
            rows[i] = new DiscordChannelHealthResponse(
                channel.ShortId,
                settings.ApplicationId,
                settings.BotUserId,
                settings.BotUsername,
                channel.Enabled,
                checks[i].Valid,
                checks[i].Error,
                snapshot.GatewayConnected,
                snapshot.LastConnectedAt,
                snapshot.LastGatewayError,
                snapshot.LastInboundAt,
                snapshot.LastSendErrorAt,
                snapshot.LastSendError);
        }

        return Results.Ok(rows);
    }
}
