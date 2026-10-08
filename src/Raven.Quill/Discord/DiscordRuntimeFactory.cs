using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed class DiscordRuntimeFactory(
    ChannelChats<DiscordMessage> chats,
    IServiceScopeFactory scopes,
    IOptions<ApplianceOptions> options,
    QuillLogger<DiscordRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Discord;

    public bool CanStart(Channel channel) => channel.Discord is { BotToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector) =>
        DiscordRuntime.Start(database, channel, changeVector, chats, scopes, options.Value.Discord, logger);
}
