using System.Globalization;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Raven.Quill.Metrics;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed record TelegramMessage(Message Message) : IChannelMessage
{
    private static readonly string[] Commands = ["/start", "/clear"];

    public string SenderId => Message.Chat.Id.ToString(CultureInfo.InvariantCulture);

    public string? Text => Message.Text;

    public ChannelMessageKind Kind =>
        Message.Contact is not null || Commands.Any(IsCommand)
            ? ChannelMessageKind.Command
            : ChannelMessageKind.Prompt;

    public bool IsCommand(string command) =>
        Message.Text is { } text &&
        text.StartsWith(command, StringComparison.OrdinalIgnoreCase) &&
        (text.Length == command.Length || char.IsWhiteSpace(text[command.Length]) || text[command.Length] == '@');
}

internal sealed class TelegramTurns(
    string database,
    Channel channel,
    ITelegramBotClient client,
    ResolvedTelegramMessages messages,
    IDocumentStore store,
    TelegramOptions options,
    QuillLogger<TelegramRuntime> logger) : IPlatformTurns<TelegramMessage>
{
    private readonly Channel _channel = channel;
    private readonly TelegramSettings _settings = channel.Telegram!;
    private readonly ResolvedTelegramMessages _messages = messages;

    public ChannelReplies Replies { get; } =
        new(messages.SomethingWentWrong, messages.Overloaded);

    public Task SendAsync(TelegramMessage replyTo, string text, CancellationToken ct) =>
        client.SendMessage(replyTo.Message.Chat.Id, text, cancellationToken: ct);

    public ChannelStreamingReply CreateReply(TelegramMessage replyTo) =>
        new TelegramStreamingReply(client, replyTo.Message.Chat.Id, options, logger);

    public async Task TypingAsync(TelegramMessage replyTo, CancellationToken ct)
    {
        try
        {
            await client.SendChatAction(replyTo.Message.Chat.Id, ChatAction.Typing, cancellationToken: ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
        }
    }

    public async Task HandleCommandAsync(TelegramMessage only, CancellationToken ct)
    {
        if (only.Message.Contact is not null)
        {
            await HandleContactAsync(only, ct);
            return;
        }

        if (only.IsCommand("/clear"))
        {
            await ClearConversationsAsync(
                ChannelConversationId.ChatPrefix(_channel.Type, _channel.ShortId, only.SenderId), ct);
            await SendAsync(only, _messages.ConversationCleared, ct);
            return;
        }

        if (only.IsCommand("/start"))
        {
            var config = await AgentLookup.FindAsync(store, database, _channel.AgentId, ct)
                         ?? throw new InvalidOperationException(
                             $"agent '{_channel.AgentId}' is no longer registered in this app");

            await SendAsync(only, _messages.Greeting, ct);
            await BindAsync(config, only, ct);
            return;
        }

        throw new InvalidOperationException($"no handler for the Telegram command in '{only.Text}'");
    }

    public async Task<Dictionary<string, string>?> BindAsync(
        AiAgentConfiguration config, TelegramMessage last, CancellationToken ct)
    {
        var message = last.Message;
        var parameters = new Dictionary<string, string>();
        string? phoneNumber = null;

        foreach (var (name, binding) in _settings.ParameterBindings)
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
                        await SendAsync(last, _messages.UsernameMissing, ct);
                        return null;
                    }

                    parameters[name] = username;
                    break;

                case ChannelParameterSource.PhoneNumber:
                    phoneNumber ??= await LoadPhoneNumberAsync(message, ct);
                    if (phoneNumber is null)
                    {
                        await RequestContactAsync(last, _messages.PhoneNumberRequest, ct);
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
            await SendAsync(last, _messages.NotConfigured, ct);
            if (logger.IsWarnEnabled)
                logger.Warn($"Telegram agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}");
            return null;
        }

        return parameters;
    }

    private async Task HandleContactAsync(TelegramMessage only, CancellationToken ct)
    {
        var message = only.Message;
        var contact = message.Contact!;
        var senderId = message.From?.Id ?? message.Chat.Id;

        if (contact.UserId != senderId || string.IsNullOrEmpty(contact.PhoneNumber))
        {
            await RequestContactAsync(only, _messages.OwnContactRequired, ct);
            return;
        }

        using (var session = store.OpenAsyncSession(database))
        {
            await session.StoreAsync(new TelegramLink
            {
                Id = TelegramLink.IdFor(_channel.ShortId, senderId),
                PhoneNumber = contact.PhoneNumber,
                SharedAt = DateTime.UtcNow,
            }, ct);
            await session.SaveChangesAsync(ct);
        }

        await client.SendMessage(message.Chat.Id,
            _messages.PhoneNumberReceived,
            replyMarkup: new ReplyKeyboardRemove(), cancellationToken: ct);
    }

    private Task RequestContactAsync(TelegramMessage replyTo, string text, CancellationToken ct) =>
        client.SendMessage(replyTo.Message.Chat.Id, text,
            replyMarkup: new ReplyKeyboardMarkup(KeyboardButton.WithRequestContact(_messages.SharePhoneNumberButton))
            {
                ResizeKeyboard = true,
                OneTimeKeyboard = true,
            },
            cancellationToken: ct);

    private async Task<string?> LoadPhoneNumberAsync(Message message, CancellationToken ct)
    {
        var senderId = message.From?.Id ?? message.Chat.Id;
        using var session = store.OpenAsyncSession(database);
        var stored = await session.LoadAsync<TelegramLink>(
            TelegramLink.IdFor(_channel.ShortId, senderId), ct);
        return string.IsNullOrEmpty(stored?.PhoneNumber) ? null : stored.PhoneNumber;
    }

    private async Task ClearConversationsAsync(string conversationIdPrefix, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var conversations = await session.Advanced.LoadStartingWithAsync<object>(
            conversationIdPrefix, pageSize: 1024, token: ct);
        var previews = await session.Advanced.LoadStartingWithAsync<object>(
            ConversationPreview.IdPrefix + conversationIdPrefix, pageSize: 1024, token: ct);

        foreach (var doc in conversations.Concat(previews))
            session.Delete(doc);

        await session.SaveChangesAsync(ct);
    }
}
