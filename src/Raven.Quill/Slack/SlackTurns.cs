using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Slack;

internal sealed record SlackMessage(
    string SenderId, string DmChannel, ChannelMessageKind Kind, string? Text) : IChannelMessage;

internal sealed class SlackTurns(
    SlackSettings settings,
    ISlackClient slack,
    SlackUserDirectory users,
    SlackOptions options,
    QuillLogger<SlackRuntime> logger) : IPlatformTurns<SlackMessage>
{
    public Task SendAsync(SlackMessage replyTo, string text, CancellationToken ct) =>
        slack.PostMessageAsync(settings.BotToken, replyTo.DmChannel, text, ct);

    public ChannelStreamingReply CreateReply(SlackMessage replyTo) =>
        new SlackStreamingReply(slack, settings.BotToken, replyTo.DmChannel, options, logger);

    public string? DescribeApiFailure(Exception e) =>
        SlackApiErrors.IsApiFailure(e) ? SlackApiErrors.Describe(e) : null;

    public async Task<Dictionary<string, string>?> BindAsync(
        AiAgentConfiguration config, SlackMessage message, CancellationToken ct)
    {
        var (parameters, bindError) = await SlackParameterBindings.BindAsync(
            config, settings.ParameterBindings, message.SenderId,
            () => users.GetAsync(slack, settings.BotToken, settings.TeamId, message.SenderId, ct));
        return parameters ?? throw new InvalidOperationException(bindError);
    }
}
