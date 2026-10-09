using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed class DiscordRuntimeFactory(
    IChatTurns<DiscordMessage> turns,
    IHttpClientFactory httpFactory,
    IOptions<ApplianceOptions> options,
    QuillLogger<DiscordRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Discord;

    public bool CanStart(Channel channel) => channel.Discord is { BotToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector) =>
        DiscordRuntime.Start(database, channel, changeVector,
            new ChannelChats<DiscordMessage>(turns, options.Value, logger.RavenLogger),
            httpFactory, options.Value.Discord, logger);
}
