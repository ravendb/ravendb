using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed record DiscordMessage(
    string SenderId, string? SenderUsername, string DmChannel, ChannelMessageKind Kind, string? Text) : IChannelMessage;

internal sealed class DiscordTurns(
    DiscordSettings settings,
    IDiscordClient discord,
    DiscordOptions options,
    QuillLogger<DiscordRuntime> logger) : IPlatformTurns<DiscordMessage>
{
    public Task SendAsync(DiscordMessage replyTo, string text, CancellationToken ct) =>
        discord.CreateMessageAsync(settings.BotToken, replyTo.DmChannel, text, ct);

    public ChannelStreamingReply CreateReply(DiscordMessage replyTo) =>
        new DiscordStreamingReply(discord, settings.BotToken, replyTo.DmChannel, options, logger);

    public string? DescribeApiFailure(Exception e) => e is DiscordApiException apiError ? apiError.Message : null;

    public Task<Dictionary<string, string>?> BindAsync(
        AiAgentConfiguration config, DiscordMessage message, CancellationToken ct)
    {
        var (parameters, bindError) = DiscordParameterBindings.Bind(
            config, settings.ParameterBindings, message.SenderId, message.SenderUsername);
        return Task.FromResult(parameters ?? throw new InvalidOperationException(bindError));
    }
}
