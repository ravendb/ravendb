using System.Collections.Concurrent;
using Discord;
using Discord.WebSocket;

namespace Raven.Quill.Discord;

internal sealed class DiscordConnection(DiscordSdk sdk)
{
    private readonly ConcurrentDictionary<ulong, (DiscordSocketClient Client, IMessageChannel Channel)> _fetched = new();
    private volatile DiscordSocketClient? _client;

    public DiscordSocketClient? Client
    {
        get => _client;
        set => _client = value;
    }

    public async Task<string> CreateAsync(string dmChannelId, string text, bool suppressEmbeds, CancellationToken ct)
    {
        var channel = await DmChannelAsync(dmChannelId, ct);
        return await channel.CreateAsync(sdk, text, suppressEmbeds, ct);
    }

    public async Task EditAsync(string dmChannelId, string messageId, string text, bool suppressEmbeds, CancellationToken ct)
    {
        var channel = await DmChannelAsync(dmChannelId, ct);
        await channel.EditAsync(sdk, messageId, text, suppressEmbeds, ct);
    }

    public async Task TypingAsync(string dmChannelId, CancellationToken ct)
    {
        var channel = await DmChannelAsync(dmChannelId, ct);
        await channel.TypingAsync(sdk, ct);
    }

    private async Task<IMessageChannel> DmChannelAsync(string dmChannelId, CancellationToken ct)
    {
        var client = _client ?? throw new DiscordApiException("the Discord gateway is not connected");
        var id = DiscordMessages.ParseSnowflake(dmChannelId, "channel id");

        if (client.GetChannel(id) is IMessageChannel cached)
            return cached;

        if (_fetched.TryGetValue(id, out var known) && ReferenceEquals(known.Client, client))
            return known.Channel;

        var fetched = await DiscordMessages.GuardAsync("fetch channel", ct, client.GetChannelAsync(id, sdk.NewRequestOptions(ct)).AsTask());
        var channel = fetched as IMessageChannel
                      ?? throw new DiscordApiException($"Discord channel {dmChannelId} is not a message channel");

        _fetched[id] = (client, channel);
        return channel;
    }
}
