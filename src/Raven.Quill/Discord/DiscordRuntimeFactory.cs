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
        var bot = new DiscordBotClient(sdk, channel.Discord!.BotToken);
        var turns = new DiscordTurns(channel.Discord, bot, options.Value.Discord, logger);

        return DiscordRuntime.Start(database, channel, fingerprint, turns, bot, chats,
            sdk, options.Value.Discord, logger, previous);
    }
}
