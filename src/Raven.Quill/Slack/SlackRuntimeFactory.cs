using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Slack;

internal sealed class SlackRuntimeFactory(
    SlackSdk sdk,
    IChatTurns<SlackMessage> turns,
    IOptions<ApplianceOptions> options,
    QuillLogger<SlackRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Slack;

    public bool CanStart(Channel channel) => channel.Slack is { AppToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector) =>
        SlackRuntime.Start(database, channel, changeVector, sdk,
            new ChannelChats<SlackMessage>(turns, options.Value, logger.RavenLogger), options.Value.Slack, logger);
}
