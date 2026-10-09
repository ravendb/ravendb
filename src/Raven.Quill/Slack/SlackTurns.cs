using Microsoft.Extensions.Options;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Slack;

internal sealed record SlackMessage(
    string Database, Channel Channel, ChannelConnectionHealth Health, string SenderId, string DmChannel,
    bool IsUnsupported, string? Text) : IChannelMessage
{
    public bool RunsAlone => IsUnsupported;
}

internal sealed class SlackTurns(
    ISlackClient slack,
    SlackUserDirectory users,
    IOptions<ApplianceOptions> options,
    QuillLogger<SlackTurns> logger) : IPlatformTurns<SlackMessage, SlackBot>
{
    public ChannelType Type => ChannelType.Slack;

    public SlackBot OpenBot(Channel channel, SlackMessage message) =>
        new(slack, channel.Slack!, message.Health, message.DmChannel, options.Value.Slack, logger);

    public async Task<Dictionary<string, string>?> BindAsync(
        SlackBot bot, Channel channel, AiAgentConfiguration config, SlackMessage message, CancellationToken ct)
    {
        var settings = bot.Settings;

        var (parameters, bindError) = await SlackParameterBindings.BindAsync(
            config, settings.ParameterBindings, message.SenderId,
            () => users.GetAsync(bot.Client, settings.BotToken, settings.TeamId, message.SenderId, ct));
        return parameters ?? throw new InvalidOperationException(bindError);
    }
}
