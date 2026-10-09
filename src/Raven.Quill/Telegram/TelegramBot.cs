using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Telegram.Bot;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramBot(
    string database,
    Channel channel,
    TelegramSettings settings,
    ITelegramBotClient client,
    long chatId,
    TelegramOptions options,
    QuillLogger<TelegramTurns> logger) : IChannelBot
{
    public string Database => database;

    public Channel Channel => channel;

    public TelegramSettings Settings => settings;

    public ITelegramBotClient Client => client;

    public long ChatId => chatId;

    public ResolvedTelegramMessages Messages { get; } = ResolvedTelegramMessages.Resolve(settings.Messages);

    public ChannelReplies Replies =>
        new(Messages.SomethingWentWrong, Messages.ConversationExpired, Messages.Overloaded);

    public Task SendAsync(string text, CancellationToken ct) =>
        client.SendMessage(chatId, text, cancellationToken: ct);

    public Task RequestContactAsync(string text, CancellationToken ct) =>
        client.SendMessage(chatId, text,
            replyMarkup: new ReplyKeyboardMarkup(KeyboardButton.WithRequestContact(Messages.SharePhoneNumberButton))
            {
                ResizeKeyboard = true,
                OneTimeKeyboard = true,
            },
            cancellationToken: ct);

    public ChannelStreamingReply CreateReply() => new TelegramStreamingReply(client, chatId, options, logger);

    public async Task TypingAsync(CancellationToken ct)
    {
        try
        {
            await client.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
        }
    }
}
