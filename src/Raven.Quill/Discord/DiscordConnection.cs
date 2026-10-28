using System.Collections.Concurrent;
using Discord;
using Discord.WebSocket;

namespace Raven.Quill.Discord;

internal sealed class DiscordConnection(DiscordSdk sdk)
{
    private readonly ConcurrentDictionary<ulong, IMessageChannel> _fetched = new();
    private volatile DiscordSocketClient? _client;

    public DiscordSocketClient? Client
    {
        get => _client;
        set
        {
            _client = value;
            _fetched.Clear();
        }
    }

    public async Task<IMessageChannel> DmChannelAsync(string dmChannelId, CancellationToken ct)
    {
        var client = _client ?? throw new DiscordApiException("the Discord gateway is not connected");
        var id = DiscordMessages.ParseSnowflake(dmChannelId, "channel id");

        if (client.GetChannel(id) is IMessageChannel cached)
            return cached;

        if (_fetched.TryGetValue(id, out var known))
            return known;

        var fetched = await DiscordMessages.GuardAsync("fetch channel", ct, client.GetChannelAsync(id, sdk.NewRequestOptions(ct)).AsTask());
        var channel = fetched as IMessageChannel
                      ?? throw new DiscordApiException($"Discord channel {dmChannelId} is not a message channel");

        if (ReferenceEquals(_client, client))
            _fetched[id] = channel;

        return channel;
    }
}
