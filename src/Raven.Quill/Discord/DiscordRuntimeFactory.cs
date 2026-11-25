using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed class DiscordRuntimeFactory(
    DiscordSdk sdk,
    ChannelChatsFactory chats,
    IOptions<ApplianceOptions> options,
    QuillLogger<DiscordRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Discord;

    public bool CanStart(Channel channel) => channel.Discord is { BotToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string fingerprint, ChannelConnectionHealth? previous)
    {
        var connection = new DiscordConnection(sdk);
        var turns = new DiscordTurns(channel.Discord!, connection, options.Value.Discord, logger);

        return DiscordRuntime.Start(database, channel, fingerprint, connection, turns, chats,
            sdk, options.Value.Discord, logger, previous);
    }
}
