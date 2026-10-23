using System.Globalization;
using System.Net;
using Discord;
using Discord.Rest;

namespace Raven.Quill.Discord;

internal sealed class DiscordBotClient(DiscordSdk sdk, string botToken) : IAsyncDisposable
{
    private readonly DiscordRestClient _client = sdk.NewRestClient();

    public Task LoginAsync(CancellationToken ct) =>
        _client.LoginState == LoginState.LoggedIn
            ? Task.CompletedTask
            : GuardAsync("login", ct, _client.LoginAsync(TokenType.Bot, botToken, validateToken: false).WaitAsync(sdk.RequestTimeout, ct));

    public async Task<DiscordDmChannel> ChannelAsync(string channelId, CancellationToken ct)
    {
        var id = ParseSnowflake(channelId, "channel id");
        var channel = await GuardAsync("resolve channel", ct, _client.GetChannelAsync(id, sdk.NewRequestOptions(ct)));
        if (channel is not IMessageChannel messages)
            throw new DiscordApiException($"discord refused resolve channel: unknown channel {channelId}", HttpStatusCode.NotFound);

        return new DiscordDmChannel(sdk, messages);
    }

    public ValueTask DisposeAsync() => _client.DisposeAsync();

    internal static async Task<TResult> GuardAsync<TResult>(string what, CancellationToken ct, Task<TResult> call)
    {
        try
        {
            return await call;
        }
        catch (Exception e) when (ct.IsCancellationRequested == false && DiscordApiErrors.IsApiFailure(e))
        {
            throw new DiscordApiException(DiscordApiErrors.Describe(e, what), DiscordApiErrors.StatusOf(e), e);
        }
    }

    internal static async Task GuardAsync(string what, CancellationToken ct, Task call)
    {
        await GuardAsync(what, ct, Completed(call));

        static async Task<bool> Completed(Task call)
        {
            await call;
            return true;
        }
    }

    internal static string Snowflake(ulong id) => id.ToString(CultureInfo.InvariantCulture);

    internal static ulong ParseSnowflake(string value, string what) =>
        ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            ? id
            : throw new DiscordApiException($"'{value}' is not a Discord {what}");
}

internal sealed class DiscordDmChannel(DiscordSdk sdk, IMessageChannel channel)
{
    private static readonly AllowedMentions SuppressMentions = new(AllowedMentionTypes.None);

    public async Task<string> CreateMessageAsync(string content, bool suppressEmbeds, CancellationToken ct)
    {
        var message = await DiscordBotClient.GuardAsync("create message", ct, channel.SendMessageAsync(
            content, allowedMentions: SuppressMentions, options: sdk.NewRequestOptions(ct), flags: FlagsFor(suppressEmbeds)));

        return DiscordBotClient.Snowflake(message.Id);
    }

    public Task EditMessageAsync(string messageId, string content, bool suppressEmbeds, CancellationToken ct)
    {
        var id = DiscordBotClient.ParseSnowflake(messageId, "message id");
        return DiscordBotClient.GuardAsync("edit message", ct, channel.ModifyMessageAsync(id, message =>
        {
            message.Content = content;
            message.AllowedMentions = SuppressMentions;
            message.Flags = FlagsFor(suppressEmbeds);
        }, sdk.NewRequestOptions(ct)));
    }

    public Task TriggerTypingAsync(CancellationToken ct) =>
        DiscordBotClient.GuardAsync("typing indicator", ct, channel.TriggerTypingAsync(sdk.NewRequestOptions(ct)));

    private static MessageFlags FlagsFor(bool suppressEmbeds) =>
        suppressEmbeds ? MessageFlags.SuppressEmbeds : MessageFlags.None;
}
