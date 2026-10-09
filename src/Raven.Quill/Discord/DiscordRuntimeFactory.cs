using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed class DiscordRuntimeFactory(
    IHttpClientFactory httpFactory,
    ChannelChatsFactory chats,
    IOptions<ApplianceOptions> options,
    QuillLogger<DiscordRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Discord;

    public bool CanStart(Channel channel) => channel.Discord is { BotToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector)
    {
        var turns = new DiscordTurns(channel.Discord!, httpFactory, options.Value.Discord, logger);

        return DiscordRuntime.Start(database, channel, changeVector, turns, chats,
            httpFactory, options.Value.Discord, logger);
    }
}
