using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Slack;

internal sealed class SlackBot(
    AsyncServiceScope scope,
    ISlackClient slack,
    SlackSettings settings,
    ChannelConnectionHealth health,
    string dmChannel,
    SlackOptions options,
    QuillLogger<SlackTurns> logger) : IChannelBot
{
    public ChannelReplies Replies => ChannelReplies.Default;

    public ChannelConnectionHealth? Health => health;

    public ISlackClient Client => slack;

    public SlackSettings Settings => settings;

    public Task SendAsync(string text, CancellationToken ct) =>
        slack.PostMessageAsync(settings.BotToken, dmChannel, text, ct);

    public ChannelStreamingReply CreateReply() =>
        new SlackStreamingReply(slack, settings.BotToken, dmChannel, options, logger);

    public string? DescribeApiFailure(Exception e) =>
        SlackApiErrors.IsApiFailure(e) ? SlackApiErrors.Describe(e) : null;

    public ValueTask DisposeAsync() => scope.DisposeAsync();
}
