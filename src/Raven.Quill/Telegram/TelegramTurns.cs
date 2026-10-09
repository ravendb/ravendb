using System.Globalization;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Raven.Quill.Metrics;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.ReplyMarkups;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed record TelegramMessage(string Database, Channel Channel, ITelegramBotClient Client, Message Message)
    : IChannelMessage
{
    public string SenderId => Message.Chat.Id.ToString(CultureInfo.InvariantCulture);

    public string? Text => Message.Text;

    public bool RunsAlone =>
        Message.Contact is not null || (Message.Text is { } text && text.TrimStart().StartsWith('/'));

    public bool IsUnsupported => false;
}

internal sealed class TelegramTurns(
    IDocumentStore store,
    IOptions<ApplianceOptions> options,
    QuillLogger<TelegramTurns> logger) : IPlatformTurns<TelegramMessage, TelegramBot>
{
    public ChannelType Type => ChannelType.Telegram;

    public TelegramBot OpenBot(Channel channel, TelegramMessage message) =>
        new(message.Database, channel, channel.Telegram!, message.Client, message.Message.Chat.Id,
            options.Value.Telegram, logger);

    public async Task<bool> TryHandleAsync(
        TelegramBot bot, Channel channel, TelegramMessage only, CancellationToken ct)
    {
        var message = only.Message;

        if (message.Contact is not null)
        {
            await HandleContactAsync(bot, message, ct);
            return true;
        }

        var prompt = message.Text!.Trim();

        if (IsCommand(bot, prompt, "clear"))
        {
            await ClearConversationsAsync(
                bot, ChannelConversationId.ChatPrefix(Type, channel.ShortId, only.SenderId), ct);
            await bot.SendAsync(bot.Messages.ConversationCleared, ct);
            return true;
        }

        if (IsCommand(bot, prompt, "start") == false)
            return false;

        var config = await AgentLookup.FindAsync(store, bot.Database, channel.AgentId, ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{channel.AgentId}' is no longer registered in this app");

        await bot.SendAsync(bot.Messages.Greeting, ct);
        await BindParametersAsync(bot, config, message, ct);
        return true;
    }

    public Task<Dictionary<string, string>?> BindAsync(
        TelegramBot bot, Channel channel, AiAgentConfiguration config, TelegramMessage message, CancellationToken ct) =>
        BindParametersAsync(bot, config, message.Message, ct);

    private async Task<Dictionary<string, string>?> BindParametersAsync(
        TelegramBot bot, AiAgentConfiguration config, Message message, CancellationToken ct)
    {
        var parameters = new Dictionary<string, string>();
        string? phoneNumber = null;

        foreach (var (name, binding) in bot.Settings.ParameterBindings)
        {
            if (binding is null)
                continue;

            switch (binding.Source)
            {
                case ChannelParameterSource.Constant:
                    parameters[name] = binding.Value ?? "";
                    break;

                case ChannelParameterSource.UserId:
                    var userId = message.From?.Id ?? message.Chat.Id;
                    parameters[name] = userId.ToString(CultureInfo.InvariantCulture);
                    break;

                case ChannelParameterSource.Username:
                    var username = message.From?.Username;
                    if (string.IsNullOrEmpty(username))
                    {
                        await bot.SendAsync(bot.Messages.UsernameMissing, ct);
                        return null;
                    }

                    parameters[name] = username;
                    break;

                case ChannelParameterSource.PhoneNumber:
                    phoneNumber ??= await LoadPhoneNumberAsync(bot, message, ct);
                    if (phoneNumber is null)
                    {
                        await bot.RequestContactAsync(bot.Messages.PhoneNumberRequest, ct);
                        return null;
                    }

                    parameters[name] = phoneNumber;
                    break;
            }
        }

        var unbound = (config.Parameters ?? [])
            .Select(p => p.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false && parameters.ContainsKey(name) == false)
            .ToArray();
        if (unbound.Length > 0)
        {
            await bot.SendAsync(bot.Messages.NotConfigured, ct);
            if (logger.IsWarnEnabled)
                logger.Warn($"Telegram agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}");
            return null;
        }

        return parameters;
    }

    private static bool IsCommand(TelegramBot bot, string text, string name)
    {
        var separator = text.IndexOfAny([' ', '\t', '\r', '\n']);
        var command = separator < 0 ? text : text[..separator];

        if (command.Equals($"/{name}", StringComparison.OrdinalIgnoreCase))
            return true;

        var username = bot.Settings.BotUsername;
        return string.IsNullOrEmpty(username) == false &&
               command.Equals($"/{name}@{username}", StringComparison.OrdinalIgnoreCase);
    }

    private async Task HandleContactAsync(TelegramBot bot, Message message, CancellationToken ct)
    {
        var contact = message.Contact!;
        var senderId = message.From?.Id ?? message.Chat.Id;

        if (contact.UserId != senderId || string.IsNullOrEmpty(contact.PhoneNumber))
        {
            await bot.RequestContactAsync(bot.Messages.OwnContactRequired, ct);
            return;
        }

        using (var session = store.OpenAsyncSession(bot.Database))
        {
            await session.StoreAsync(new TelegramLink
            {
                Id = TelegramLink.IdFor(bot.Channel.ShortId, senderId),
                PhoneNumber = contact.PhoneNumber,
                SharedAt = DateTime.UtcNow,
            }, ct);
            await session.SaveChangesAsync(ct);
        }

        await bot.Client.SendMessage(bot.ChatId,
            bot.Messages.PhoneNumberReceived,
            replyMarkup: new ReplyKeyboardRemove(), cancellationToken: ct);
    }

    private async Task<string?> LoadPhoneNumberAsync(TelegramBot bot, Message message, CancellationToken ct)
    {
        var senderId = message.From?.Id ?? message.Chat.Id;
        using var session = store.OpenAsyncSession(bot.Database);
        var stored = await session.LoadAsync<TelegramLink>(
            TelegramLink.IdFor(bot.Channel.ShortId, senderId), ct);
        return string.IsNullOrEmpty(stored?.PhoneNumber) ? null : stored.PhoneNumber;
    }

    private async Task ClearConversationsAsync(TelegramBot bot, string conversationIdPrefix, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(bot.Database);
        var conversations = await session.Advanced.LoadStartingWithAsync<object>(
            conversationIdPrefix, pageSize: 1024, token: ct);
        var previews = await session.Advanced.LoadStartingWithAsync<object>(
            ConversationPreview.IdPrefix + conversationIdPrefix, pageSize: 1024, token: ct);

        foreach (var doc in conversations.Concat(previews))
            session.Delete(doc);

        await session.SaveChangesAsync(ct);
    }
}
