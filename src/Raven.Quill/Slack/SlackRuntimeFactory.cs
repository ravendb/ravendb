using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Slack;

internal sealed class SlackRuntimeFactory(
    SlackSdk sdk,
    ISlackClient slack,
    SlackUserDirectory users,
    ChannelChatsFactory chats,
    IOptions<ApplianceOptions> options,
    QuillLogger<SlackRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Slack;

    public bool CanStart(Channel channel) => channel.Slack is { AppToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector, ChannelConnectionHealth? previous)
    {
        var turns = new SlackTurns(channel.Slack!, slack, users, options.Value.Slack, logger);

        return SlackRuntime.Start(
            database, channel, changeVector, sdk, turns, chats, options.Value.Slack, logger, previous);
    }
}
