using Raven.Client.Documents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Telegram;

namespace Raven.Quill.Endpoints;

public static class TelegramEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}/telegram").WithTags("telegram").RequireAuthorization();

        group.MapGet("/health", GetHealthAsync)
            .WithName("telegram.health")
            .WithDescription(
                "Per-bot polling health for the app's Telegram channels: last successful poll, last error " +
                "(token-scrubbed) and error count. Counters live in the polling service, so they reset on restart.")
            .Produces<TelegramChannelHealthResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetHealthAsync(
        string slug,
        IDocumentStore store,
        ITelegramChannelManager telegramManager,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        List<Channel> channels;
        using (var session = store.OpenAsyncSession(app.Database))
        {
            const int pageSize = 1024;
            channels = [];
            for (var start = 0;; start += pageSize)
            {
                var page = (await session.Advanced.LoadStartingWithAsync<Channel>(
                    Channel.IdPrefix, start: start, pageSize: pageSize, token: ct)).ToArray();
                channels.AddRange(page);
                if (page.Length < pageSize)
                    break;
            }
        }

        var health = telegramManager.GetHealth(app.Database);

        var items = channels
            .Where(c => c.Type == ChannelType.Telegram)
            .OrderByDescending(c => c.CreatedAt)
            .Select(c =>
            {
                var channelId = StripPrefix(c.Id);
                var snapshot = health.GetValueOrDefault(channelId);
                return new TelegramChannelHealthResponse(
                    channelId,
                    c.Telegram?.BotUsername,
                    c.Enabled,
                    snapshot?.IsPolling ?? false,
                    snapshot?.LastSuccessfulPoll,
                    snapshot?.LastErrorAt,
                    snapshot?.ErrorCount ?? 0,
                    snapshot?.LastError);
            })
            .ToArray();

        return Results.Ok(items);
    }

    private static string StripPrefix(string? id) =>
        id is not null && id.StartsWith(Channel.IdPrefix, StringComparison.Ordinal)
            ? id[Channel.IdPrefix.Length..]
            : id ?? "";
}
