using System.Text.Json;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Hosting;
using Raven.Quill.Raven;
using Raven.Quill.Slack;

namespace Raven.Quill.Endpoints;

public static class SlackEndpoints
{
    public const string WebhookRateLimitPolicy = "slack-webhook";

    public static void Map(WebApplication app)
    {
        app.MapPost("/webhooks/slack/{token}", HandleDeliveryAsync)
            .AllowAnonymous()
            .ExcludeFromDescription()
            .RequireRateLimiting(WebhookRateLimitPolicy);

        var group = app.MapGroup("/api/apps/{slug}").WithTags("slack").RequireAuthorization();

        group.MapGet("/channels/{channelId}/slack/webhook", GetWebhookInfoAsync)
            .WithName("slack.webhookInfo")
            .WithDescription(
                "The event subscription configuration for this channel: the public request URL, " +
                "ready to paste into the Slack app's Event Subscriptions page.")
            .Produces<SlackWebhookInfoResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/slack/health", GetHealthAsync)
            .WithName("slack.health")
            .WithDescription(
                "Per-channel connection health for the app's Slack channels: bot token validity " +
                "(cached a few minutes) plus in-memory webhook and send activity since the last restart.")
            .Produces<SlackChannelHealthResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> HandleDeliveryAsync(
        string token,
        HttpContext ctx,
        IDocumentStore store,
        SlackInboundProcessor processor,
        SlackHealthRegistry health,
        IOptions<ApplianceOptions> options,
        ILogger<SlackLogger> logger,
        CancellationToken ct)
    {
        var limit = options.Value.Slack.MaxWebhookBodyBytes;
        if (ctx.Request.ContentLength is { } declared && declared > limit)
            return Results.StatusCode(StatusCodes.Status413PayloadTooLarge);

        var resolved = await ResolveTokenChannelAsync(store, token, ct);
        if (resolved is null)
        {
            logger.LogDebug("Dropped Slack delivery for unknown webhook token {Token}",
                EmbedLink.RedactToken(token));
            return Results.Ok();
        }

        var (database, channel, settings) = resolved.Value;

        var rawBody = await ReadCappedAsync(ctx.Request.Body, limit, ct);
        if (rawBody is null)
            return Results.StatusCode(StatusCodes.Status413PayloadTooLarge);

        var signature = ctx.Request.Headers[SlackSignature.SignatureHeaderName].ToString();
        var timestamp = ctx.Request.Headers[SlackSignature.TimestampHeaderName].ToString();
        if (SlackSignature.IsValid(rawBody, settings.SigningSecret, signature, timestamp,
                options.Value.Slack.SignatureTolerance, DateTime.UtcNow) == false)
        {
            health.RecordSignatureFailure(database, channel.ShortId);
            logger.LogWarning(
                "Slack delivery for channel {ChannelId} failed signature verification (wrong signing secret or stale timestamp?)",
                channel.ShortId);
            return Results.Unauthorized();
        }

        SlackEventPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<SlackEventPayload>(rawBody);
        }
        catch (JsonException e)
        {
            logger.LogWarning("Dropped unparseable Slack delivery for channel {ChannelId}: {Error}",
                channel.ShortId, e.Message);
            return Results.Ok();
        }

        if (payload?.Type == "url_verification")
            return Results.Text(payload.Challenge ?? "");

        if (payload?.Type != "event_callback" || payload.Event is not { } message)
            return Results.Ok();

        if (payload.TeamId != settings.TeamId)
        {
            logger.LogDebug("Dropped Slack delivery for channel {ChannelId} from foreign team {TeamId}",
                channel.ShortId, payload.TeamId);
            return Results.Ok();
        }

        if (message.Type != "message" || message.ChannelType != "im")
            return Results.Ok();

        if (string.IsNullOrEmpty(message.BotId) == false ||
            string.IsNullOrEmpty(message.User) || message.User == settings.BotUserId)
            return Results.Ok();

        var kind = message.Subtype switch
        {
            null or "" => "text",
            "file_share" => "unsupported",
            _ => null,
        };
        if (kind is null || string.IsNullOrEmpty(message.Channel) || channel.Enabled == false)
            return Results.Ok();

        health.RecordInbound(database, channel.ShortId);
        processor.Enqueue(database, channel.Id!, message.User, message.Channel, payload.EventId ?? "", kind, message.Text);

        return Results.Ok();
    }

    private static async Task<IResult> GetWebhookInfoAsync(
        string slug,
        string channelId,
        HttpContext ctx,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);
        if (channel is not { Type: ChannelType.Slack, Slack: { } settings })
            return Results.NotFound(new ApiErrorResponse($"no Slack channel '{channelId}' in app '{slug}'"));

        var publicHost = ApplianceHost.WithSubdomain(ctx.Request.Host, "public");
        var url = $"{ctx.Request.Scheme}://{publicHost.ToUriComponent()}{ctx.Request.PathBase}" +
                  $"/webhooks/slack/{settings.WebhookToken}";

        return Results.Ok(new SlackWebhookInfoResponse(url));
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
        var channels = await session.Query<Channel>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(c => c.Type == ChannelType.Slack)
            .ToArrayAsync(ct);

        var slackChannels = channels
            .Where(c => c.Slack is not null)
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
                snapshot.LastInboundAt,
                snapshot.LastSignatureFailureAt,
                snapshot.LastSendErrorAt,
                snapshot.LastSendError);
        }

        return Results.Ok(rows);
    }

    private static async Task<(string Database, Channel Channel, SlackSettings Settings)?> ResolveTokenChannelAsync(
        IDocumentStore store, string token, CancellationToken ct)
    {
        if (EmbedLink.IsWellFormedToken(token) == false)
            return null;

        SlackWebhookRoute? route;
        using (var configSession = store.OpenAsyncSession())
            route = await configSession.LoadAsync<SlackWebhookRoute>(SlackWebhookRoute.IdFor(token), ct);
        if (route is null)
            return null;

        try
        {
            using var session = store.OpenAsyncSession(route.Database);
            var channel = await session.LoadAsync<Channel>(route.ChannelId, ct);
            if (channel is not { Type: ChannelType.Slack, Slack: { } settings } ||
                settings.WebhookToken != token)
                return null;

            return (route.Database, channel, settings);
        }
        catch (DatabaseDoesNotExistException)
        {
            return null;
        }
    }

    private static async Task<byte[]?> ReadCappedAsync(Stream body, int limit, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];
        while (true)
        {
            var read = await body.ReadAsync(chunk, ct);
            if (read == 0)
                return buffer.ToArray();

            if (buffer.Length + read > limit)
                return null;

            buffer.Write(chunk, 0, read);
        }
    }

    internal sealed class SlackLogger;
}
