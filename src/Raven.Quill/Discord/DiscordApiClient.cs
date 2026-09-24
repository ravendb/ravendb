using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using Discord;
using Discord.Net;
using Discord.Rest;

namespace Raven.Quill.Discord;

internal sealed class DiscordApiClient(DiscordSdk sdk) : IDiscordClient, IAsyncDisposable
{
    private const string TokenRejected =
        "discord rejected the bot token; reset it on the app's Bot page and try again";

    private const int MaxCachedBots = 256;

    private static readonly AllowedMentions SuppressMentions = new(AllowedMentionTypes.None);

    private readonly ConcurrentDictionary<string, Lazy<Task<BotSession>>> _bots = new();

    public async Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
        string botToken, CancellationToken ct)
    {
        try
        {
            await using var client = sdk.NewRestClient();
            await client.LoginAsync(TokenType.Bot, botToken, validateToken: false).WaitAsync(ct);

            var user = client.CurrentUser;
            if (user is null || string.IsNullOrEmpty(user.Username))
                return (null, "discord returned an unrecognized users/@me payload", true);

            if (user.IsBot == false)
                return (null, "that token belongs to a user account, not a bot; copy the token from the app's Bot page", true);

            var application = await client.GetApplicationInfoAsync(sdk.NewRequestOptions(ct));
            if (application is null)
                return (null, "discord returned an unrecognized oauth2/applications/@me payload", true);

            return (new DiscordBotIdentity(Snowflake(application.Id), Snowflake(user.Id), user.Username), null, true);
        }
        catch (RateLimitedException)
        {
            return (null, "discord is rate-limiting the token check; try again shortly", false);
        }
        catch (HttpException e) when ((int)e.HttpCode >= 500)
        {
            return (null, $"the Discord API is unavailable (status {(int)e.HttpCode})", false);
        }
        catch (HttpException e) when (e.HttpCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            return (null, TokenRejected, true);
        }
        catch (HttpException e)
        {
            return (null, $"discord refused the token check: {ErrorTextOf(e)}", true);
        }
        catch (TimeoutException)
        {
            return (null, "the Discord API did not respond while validating the bot token", false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            return (null, "the Discord API did not respond while validating the bot token", false);
        }
        catch (HttpRequestException e)
        {
            return (null, $"could not reach the Discord API: {e.Message}", false);
        }
    }

    public async Task<string> CreateMessageAsync(
        string botToken, string channelId, string content, bool suppressEmbeds, CancellationToken ct)
    {
        var message = await CallAsync(botToken, channelId, "create message", ct,
            (channel, options) => channel.SendMessageAsync(
                content, allowedMentions: SuppressMentions, options: options, flags: FlagsFor(suppressEmbeds)));

        return Snowflake(message.Id);
    }

    public Task EditMessageAsync(
        string botToken, string channelId, string messageId, string content, bool suppressEmbeds,
        CancellationToken ct)
    {
        var id = ParseSnowflake(messageId, "message id");
        return CallAsync(botToken, channelId, "edit message", ct,
            (channel, options) => channel.ModifyMessageAsync(id, message =>
            {
                message.Content = content;
                message.AllowedMentions = SuppressMentions;
                message.Flags = FlagsFor(suppressEmbeds);
            }, options));
    }

    public Task<IDisposable> BeginTypingAsync(string botToken, string channelId, CancellationToken ct) =>
        CallAsync(botToken, channelId, "typing indicator", ct,
            (channel, options) => Task.FromResult(channel.EnterTypingState(options)));

    private static MessageFlags FlagsFor(bool suppressEmbeds) =>
        suppressEmbeds ? MessageFlags.SuppressEmbeds : MessageFlags.None;

    public async ValueTask DisposeAsync()
    {
        foreach (var token in _bots.Keys)
        {
            if (_bots.TryRemove(token, out var bot))
                await DisposeAsync(bot);
        }
    }

    private async Task<TResult> CallAsync<TResult>(
        string botToken, string channelId, string what, CancellationToken ct,
        Func<IMessageChannel, RequestOptions, Task<TResult>> call)
    {
        var id = ParseSnowflake(channelId, "channel id");
        try
        {
            var bot = await BotAsync(botToken, ct);
            var channel = await bot.ChannelAsync(id, sdk.NewRequestOptions(ct))
                          ?? throw new DiscordApiException(
                              $"discord refused {what}: unknown channel {channelId}", HttpStatusCode.NotFound);

            return await call(channel, sdk.NewRequestOptions(ct));
        }
        catch (HttpException e) when (e.HttpCode == HttpStatusCode.Unauthorized)
        {
            if (_bots.TryRemove(botToken, out var bot))
                await DisposeAsync(bot);

            throw new DiscordApiException($"discord refused {what}: {ErrorTextOf(e)}", e.HttpCode, e);
        }
        catch (RateLimitedException e)
        {
            throw new DiscordApiException($"discord rate-limited {what}", HttpStatusCode.TooManyRequests, e);
        }
        catch (HttpException e)
        {
            throw new DiscordApiException($"discord refused {what}: {ErrorTextOf(e)}", e.HttpCode, e);
        }
        catch (TimeoutException e)
        {
            throw new DiscordApiException($"the Discord API did not respond to {what}", inner: e);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            throw new DiscordApiException($"the Discord API did not respond to {what}");
        }
        catch (HttpRequestException e)
        {
            throw new DiscordApiException($"could not reach the Discord API: {e.Message}", inner: e);
        }
    }

    private async Task<BotSession> BotAsync(string botToken, CancellationToken ct)
    {
        if (_bots.Count >= MaxCachedBots)
        {
            foreach (var token in _bots.Keys)
            {
                if (_bots.TryRemove(token, out var stale))
                    await DisposeAsync(stale);
            }
        }

        var bot = _bots.GetOrAdd(botToken, token => new Lazy<Task<BotSession>>(() => BotSession.OpenAsync(sdk, token)));
        try
        {
            return await bot.Value.WaitAsync(ct);
        }
        catch (Exception) when (bot.Value.IsCompletedSuccessfully == false)
        {
            if (_bots.TryRemove(new KeyValuePair<string, Lazy<Task<BotSession>>>(botToken, bot)))
                _ = DisposeAsync(bot);
            throw;
        }
    }

    private static async Task DisposeAsync(Lazy<Task<BotSession>> bot)
    {
        try
        {
            await (await bot.Value).DisposeAsync();
        }
        catch (Exception)
        {
        }
    }

    private static string ErrorTextOf(HttpException e) =>
        string.IsNullOrWhiteSpace(e.Reason) ? $"status {(int)e.HttpCode}" : e.Reason;

    private static string Snowflake(ulong id) => id.ToString(CultureInfo.InvariantCulture);

    private static ulong ParseSnowflake(string value, string what) =>
        ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            ? id
            : throw new DiscordApiException($"'{value}' is not a Discord {what}");

    private sealed class BotSession(DiscordRestClient client) : IAsyncDisposable
    {
        private const int MaxCachedChannels = 1024;

        private readonly ConcurrentDictionary<ulong, Lazy<Task<IMessageChannel?>>> _channels = new();

        public static async Task<BotSession> OpenAsync(DiscordSdk sdk, string botToken)
        {
            var client = sdk.NewRestClient();
            try
            {
                await client.LoginAsync(TokenType.Bot, botToken, validateToken: false);
            }
            catch (Exception)
            {
                await client.DisposeAsync();
                throw;
            }

            return new BotSession(client);
        }

        public async Task<IMessageChannel?> ChannelAsync(ulong channelId, RequestOptions options)
        {
            if (_channels.Count >= MaxCachedChannels)
                _channels.Clear();

            var channel = _channels.GetOrAdd(channelId, id => new Lazy<Task<IMessageChannel?>>(
                async () => await client.GetChannelAsync(id, options) as IMessageChannel));

            try
            {
                var resolved = await channel.Value;
                if (resolved is null)
                    _channels.TryRemove(new KeyValuePair<ulong, Lazy<Task<IMessageChannel?>>>(channelId, channel));
                return resolved;
            }
            catch (Exception)
            {
                _channels.TryRemove(new KeyValuePair<ulong, Lazy<Task<IMessageChannel?>>>(channelId, channel));
                throw;
            }
        }

        public ValueTask DisposeAsync() => client.DisposeAsync();
    }
}
