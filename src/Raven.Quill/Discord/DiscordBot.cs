using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed class DiscordBot(
    AsyncServiceScope scope,
    IDiscordClient discord,
    DiscordSettings settings,
    ChannelConnectionHealth health,
    string dmChannel,
    DiscordOptions options,
    QuillLogger<DiscordTurns> logger) : IChannelBot
{
    public ChannelReplies Replies => ChannelReplies.Default;

    public ChannelConnectionHealth? Health => health;

    public Task SendAsync(string text, CancellationToken ct) =>
        discord.CreateMessageAsync(settings.BotToken, dmChannel, text, ct);

    public ChannelStreamingReply CreateReply() =>
        new DiscordStreamingReply(discord, settings.BotToken, dmChannel, options, logger);

    public string? DescribeApiFailure(Exception e) => e is DiscordApiException apiError ? apiError.Message : null;

    public ValueTask DisposeAsync() => scope.DisposeAsync();
}
