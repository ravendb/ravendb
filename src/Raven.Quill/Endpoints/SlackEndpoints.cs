using Raven.Client.Documents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Raven;
using Raven.Quill.Slack;

namespace Raven.Quill.Endpoints;

public static class SlackEndpoints
{
    internal const string AppTokenMissingError =
        "this channel predates Socket Mode; rotate its credentials and paste the app-level (xapp-) token to connect it";

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("slack").RequireAuthorization();

        group.MapGet("/slack/health", GetHealthAsync)
            .WithName("slack.health")
            .WithDescription(
                "Per-channel connection health for the app's Slack channels: bot token validity " +
                "(cached a few minutes) plus the live Socket Mode connection state and the inbound and send " +
                "activity seen since the last restart.")
            .Produces<SlackChannelHealthResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetHealthAsync(
        string slug,
        IDocumentStore store,
        ISlackClient slackClient,
        SlackHealthRegistry health,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

        var slackChannels = channels
            .Where(c => c is { Type: ChannelType.Slack, Slack: not null })
            .OrderByDescending(c => c.CreatedAt)
            .ToArray();

        var checks = new (bool? Valid, string? Error)[slackChannels.Length];
        await Task.WhenAll(slackChannels.Select(async (channel, i) =>
        {
            if (health.TryGetFreshTokenCheck(app.Database, channel.ShortId, out var tokenValid, out var tokenError) == false)
            {
                var (info, error, slackResponded) = await slackClient.AuthTestAsync(channel.Slack!.BotToken, ct);
                tokenValid = info is not null ? true : slackResponded ? false : null;
                tokenError = info is null ? error : null;
                health.StoreTokenCheck(app.Database, channel.ShortId, tokenValid, tokenError);
            }

            checks[i] = (tokenValid, tokenError);
        }));

        var rows = new SlackChannelHealthResponse[slackChannels.Length];
        for (var i = 0; i < slackChannels.Length; i++)
        {
            var channel = slackChannels[i];
            var settings = channel.Slack!;
            var snapshot = health.SnapshotFor(app.Database, channel.ShortId);
            rows[i] = new SlackChannelHealthResponse(
                channel.ShortId,
                settings.TeamId,
                settings.TeamName,
                settings.BotUserId,
                channel.Enabled,
                checks[i].Valid,
                checks[i].Error,
                snapshot.SocketConnected,
                snapshot.LastConnectedAt,
                settings.AppToken.Length == 0 ? AppTokenMissingError : snapshot.LastSocketError,
                snapshot.LastInboundAt,
                snapshot.LastSendErrorAt,
                snapshot.LastSendError);
        }

        return Results.Ok(rows);
    }
}
