using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramRuntimeFactory(
    TelegramBotClientFactory botFactory,
    IDocumentStore store,
    ChannelChatsFactory chats,
    IOptions<ApplianceOptions> options,
    QuillLogger<TelegramRuntime> logger) : IChannelRuntimeFactory
{
    public ChannelType Type => ChannelType.Telegram;

    public bool CanStart(Channel channel) => channel.Telegram is { BotToken.Length: > 0 };

    public IChannelRuntime Start(string database, Channel channel, string? changeVector, ChannelConnectionHealth? previous)
    {
        var client = botFactory.Create(channel.Telegram!.BotToken);
        var messages = ResolvedTelegramMessages.Resolve(channel.Telegram.Messages);
        var turns = new TelegramTurns(database, channel, client, messages, store, options.Value.Telegram, logger);

        return TelegramRuntime.Start(database, channel, changeVector, client, messages, turns, chats,
            options.Value.Telegram, logger);
    }
}
