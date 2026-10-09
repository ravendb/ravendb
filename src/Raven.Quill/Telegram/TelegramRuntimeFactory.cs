using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramRuntimeFactory(
    TelegramBotClientFactory botFactory,
    IChatTurns<TelegramMessage> turns,
    IOptions<ApplianceOptions> options,
    QuillLogger<TelegramRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Telegram;

    public bool CanStart(Channel channel) => channel.Telegram is { BotToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector) =>
        TelegramRuntime.Start(database, channel, changeVector, botFactory,
            new ChannelChats<TelegramMessage>(turns, options.Value, logger.RavenLogger), options.Value.Telegram, logger);
}
