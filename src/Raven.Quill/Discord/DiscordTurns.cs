using Microsoft.Extensions.Options;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed record DiscordMessage(
    string Database, Channel Channel, ChannelConnectionHealth Health, string SenderId, string? SenderUsername,
    string DmChannel, bool IsUnsupported, string? Text) : IChannelMessage
{
    public bool RunsAlone => IsUnsupported;
}

internal sealed class DiscordTurns(
    IHttpClientFactory httpFactory,
    IOptions<ApplianceOptions> options,
    QuillLogger<DiscordTurns> logger) : IPlatformTurns<DiscordMessage, DiscordBot>
{
    public ChannelType Type => ChannelType.Discord;

    public DiscordBot OpenBot(Channel channel, DiscordMessage message) =>
        new(DiscordApiClient.Create(httpFactory), channel.Discord!, message.Health, message.DmChannel,
            options.Value.Discord, logger);

    public Task<Dictionary<string, string>?> BindAsync(
        DiscordBot bot, Channel channel, AiAgentConfiguration config, DiscordMessage message, CancellationToken ct)
    {
        var (parameters, bindError) = DiscordParameterBindings.Bind(
            config, channel.Discord!.ParameterBindings, message.SenderId, message.SenderUsername);
        return Task.FromResult(parameters ?? throw new InvalidOperationException(bindError));
    }
}
