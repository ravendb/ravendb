using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using Discord;
using Discord.Net;
using Discord.Rest;

namespace Raven.Quill.Discord;

internal sealed class DiscordBotClient(DiscordSdk sdk, string botToken) : IAsyncDisposable
{
    private const int MaxCachedChannels = 1024;

    private static readonly AllowedMentions SuppressMentions = new(AllowedMentionTypes.None);

    private readonly object _sync = new();
    private readonly ConcurrentDictionary<ulong, Lazy<Task<IMessageChannel?>>> _channels = new();
    private Lazy<Task<DiscordRestClient>>? _login;
    private bool _disposed;

    public async Task<string> CreateMessageAsync(
        string channelId, string content, bool suppressEmbeds, CancellationToken ct)
    {
        var message = await CallAsync(channelId, "create message", ct,
            (channel, options) => channel.SendMessageAsync(
                content, allowedMentions: SuppressMentions, options: options, flags: FlagsFor(suppressEmbeds)));

        return Snowflake(message.Id);
    }

    public Task EditMessageAsync(
        string channelId, string messageId, string content, bool suppressEmbeds, CancellationToken ct)
    {
        var id = ParseSnowflake(messageId, "message id");
        return CallAsync(channelId, "edit message", ct,
            (channel, options) => channel.ModifyMessageAsync(id, message =>
            {
                message.Content = content;
                message.AllowedMentions = SuppressMentions;
                message.Flags = FlagsFor(suppressEmbeds);
            }, options));
    }

    public Task TriggerTypingAsync(string channelId, CancellationToken ct) =>
        CallAsync(channelId, "typing indicator", ct,
            async (channel, options) =>
            {
                await channel.TriggerTypingAsync(options);
                return true;
            });

    public ValueTask DisposeAsync()
    {
        Lazy<Task<DiscordRestClient>>? login;
        lock (_sync)
        {
            _disposed = true;
            login = _login;
            _login = null;
        }

        if (login is not null)
            DisposeWhenOpened(login);
        return ValueTask.CompletedTask;
    }

    private async Task<TResult> CallAsync<TResult>(
        string channelId, string what, CancellationToken ct,
        Func<IMessageChannel, RequestOptions, Task<TResult>> call)
    {
        var id = ParseSnowflake(channelId, "channel id");
        try
        {
            var client = await ClientAsync(ct);
            var channel = await ChannelAsync(client, id, sdk.NewRequestOptions(ct))
                          ?? throw new DiscordApiException(
                              $"discord refused {what}: unknown channel {channelId}", HttpStatusCode.NotFound);

            return await call(channel, sdk.NewRequestOptions(ct));
        }
        catch (HttpException e) when (e.HttpCode == HttpStatusCode.Unauthorized)
        {
            ForgetLogin();
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
        catch (ObjectDisposedException e)
        {
            throw new DiscordApiException($"the bot session was reset during {what}", inner: e);
        }
    }

    private async Task<DiscordRestClient> ClientAsync(CancellationToken ct)
    {
        Lazy<Task<DiscordRestClient>> login;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            login = _login ??= new Lazy<Task<DiscordRestClient>>(OpenAsync);
        }

        try
        {
            return await login.Value.WaitAsync(ct);
        }
        catch (Exception)
        {
            ForgetLogin(login);
            throw;
        }
    }

    private async Task<DiscordRestClient> OpenAsync()
    {
        var client = sdk.NewRestClient();
        try
        {
            await client.LoginAsync(TokenType.Bot, botToken, validateToken: false).WaitAsync(sdk.RequestTimeout);
        }
        catch (Exception)
        {
            await client.DisposeAsync();
            throw;
        }

        return client;
    }

    private void ForgetLogin(Lazy<Task<DiscordRestClient>>? login = null)
    {
        lock (_sync)
        {
            if (login is not null && ReferenceEquals(_login, login) == false)
                return;

            login = _login;
            _login = null;
        }

        _channels.Clear();
        if (login is not null)
            DisposeWhenOpened(login);
    }

    private static void DisposeWhenOpened(Lazy<Task<DiscordRestClient>> login) =>
        login.Value.ContinueWith(
            static opened => opened.Result.DisposeAsync().AsTask(),
            CancellationToken.None, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);

    private async Task<IMessageChannel?> ChannelAsync(DiscordRestClient client, ulong channelId, RequestOptions options)
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

    private static MessageFlags FlagsFor(bool suppressEmbeds) =>
        suppressEmbeds ? MessageFlags.SuppressEmbeds : MessageFlags.None;

    internal static string ErrorTextOf(HttpException e) =>
        string.IsNullOrWhiteSpace(e.Reason) ? $"status {(int)e.HttpCode}" : e.Reason;

    private static string Snowflake(ulong id) => id.ToString(CultureInfo.InvariantCulture);

    private static ulong ParseSnowflake(string value, string what) =>
        ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            ? id
            : throw new DiscordApiException($"'{value}' is not a Discord {what}");
}
