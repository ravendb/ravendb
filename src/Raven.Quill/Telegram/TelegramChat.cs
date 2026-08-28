using System.Globalization;
using System.Threading.Channels;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramChat
{
    private readonly Channel<Message> _queue;
    private readonly long _chatId;
    private readonly TelegramBotRuntime _bot;
    private readonly TelegramChatContext _context;
    private readonly CancellationToken _ct;

    private int _overloadNotified;
    private int _retired;

    public TelegramChat(long chatId, TelegramBotRuntime bot, TelegramChatContext context, CancellationToken ct)
    {
        _chatId = chatId;
        _bot = bot;
        _context = context;
        _ct = ct;

        _queue = System.Threading.Channels.Channel.CreateBounded<Message>(
            new BoundedChannelOptions(context.Options.Telegram.ChatQueueCapacity)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            });

        Completion = Task.Run(RunAsync, CancellationToken.None);
    }

    public Task Completion { get; }

    /// After an idle timeout the chat retires: it leaves the bot's map and rejects posts, and the
    /// poster re-creates it. TryPost failing on a live chat still means the queue is full.
    public bool IsRetired => Volatile.Read(ref _retired) == 1;

    public bool TryPost(Message message) => _queue.Writer.TryWrite(message);

    public void NotifyOverloadOnce()
    {
        if (Interlocked.Exchange(ref _overloadNotified, 1) != 0)
            return;

        _ = _bot.TrySendPlainAsync(_chatId, _context.Messages.Overloaded);
    }

    private async Task RunAsync()
    {
        var batch = new List<Message>();

        try
        {
            while (await WaitForMessageAsync())
            {
                while (_queue.Reader.TryRead(out var message))
                {
                    if (RunsAlone(message) == false)
                    {
                        if (string.IsNullOrWhiteSpace(message.Text) == false)
                            batch.Add(message);
                        continue;
                    }

                    await FlushAsync(batch);
                    await HandleSafeAsync([message]);
                }

                await FlushAsync(batch);
            }
        }
        catch (OperationCanceledException) when (_ct.IsCancellationRequested)
        {
        }
    }

    private static bool RunsAlone(Message message) =>
        message.Contact is not null || (message.Text is { } text && IsCommand(text));

    private static bool IsCommand(string text) => text.TrimStart().StartsWith('/');

    private async Task FlushAsync(List<Message> batch)
    {
        if (batch.Count == 0)
            return;

        var pending = batch.ToArray();
        batch.Clear();
        await HandleSafeAsync(pending);
    }

    private async Task<bool> WaitForMessageAsync()
    {
        var wait = _queue.Reader.WaitToReadAsync(_ct).AsTask();
        try
        {
            return await wait.WaitAsync(_context.Options.Telegram.ChatIdleTimeout);
        }
        catch (TimeoutException)
        {
            Retire();
            // a message posted before the queue completed still drains below
            return await wait;
        }
    }

    private void Retire()
    {
        // publish the retired flag before completing the queue, so a TryPost that fails
        // against the completed queue always observes IsRetired and re-creates the chat
        Interlocked.Exchange(ref _retired, 1);
        _queue.Writer.TryComplete();
        _bot.OnChatRetired(_chatId, this);
    }

    private async Task HandleSafeAsync(IReadOnlyList<Message> batch)
    {
        try
        {
            await HandleBatchAsync(batch);
        }
        catch (OperationCanceledException) when (_ct.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            _context.Logger.Warn(
                $"Telegram message handling failed for channel {_context.ChannelDoc.Id} chat {_chatId}: " +
                $"{e.Message}");
        }

        Interlocked.Exchange(ref _overloadNotified, 0);
    }

    private async Task HandleBatchAsync(IReadOnlyList<Message> batch)
    {
        // the sender is the same across a private chat's batch, so the last message binds the parameters
        var message = batch[^1];

        if (message.Contact is not null)
        {
            await HandleContactAsync(message);
            return;
        }

        var channel = _context.ChannelDoc;
        var prompt = batch.Count == 1
            ? message.Text!.Trim()
            : string.Join('\n', batch.Select(m => m.Text!.Trim()));
        if (prompt.Length == 0)
            return;

        var conversationId = TelegramConversationId.ForUtcDay(_context.ChannelDoc.ShortId, _chatId, DateTime.UtcNow);

        if (IsCommand(prompt, "clear"))
        {
            await ClearConversationAsync(conversationId);
            await SendPlainAsync(_context.Messages.ConversationCleared);
            return;
        }

        var config = await AgentLookup.FindAsync(_context.Store, _context.Database, channel.AgentId, _ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{channel.AgentId}' is no longer registered in this app");

        if (IsCommand(prompt, "start"))
        {
            await SendPlainAsync(_context.Messages.Greeting);
            // ask for whatever the bindings need now, so the first real message can be answered
            await BindParametersAsync(config, message);
            return;
        }

        var parameters = await BindParametersAsync(config, message);
        if (parameters is null)
            return;

        try
        {
            await _bot.Client.SendChatAction(_chatId, ChatAction.Typing, cancellationToken: _ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {

        }

        await RunTurnAsync(prompt, conversationId, config, parameters);
    }

    private async Task<Dictionary<string, string>?> BindParametersAsync(
        AiAgentConfiguration config, Message message)
    {
        var parameters = new Dictionary<string, string>();
        string? phoneNumber = null;

        foreach (var (name, binding) in _context.ChannelDoc.Telegram!.ParameterBindings)
        {
            switch (binding.Source)
            {
                case ChannelParameterSource.Constant:
                    parameters[name] = binding.Value ?? "";
                    break;

                case ChannelParameterSource.UserId:
                    var userId = message.From?.Id ?? _chatId;
                    parameters[name] = userId.ToString(CultureInfo.InvariantCulture);
                    break;

                case ChannelParameterSource.Username:
                    var username = message.From?.Username;
                    if (string.IsNullOrEmpty(username))
                    {
                        await SendPlainAsync(_context.Messages.UsernameMissing);
                        return null;
                    }

                    parameters[name] = username;
                    break;

                case ChannelParameterSource.PhoneNumber:
                    phoneNumber ??= await LoadPhoneNumberAsync(message);
                    if (phoneNumber is null)
                    {
                        await RequestContactAsync(_context.Messages.PhoneNumberRequest);
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
            await SendPlainAsync(_context.Messages.NotConfigured);
            throw new InvalidOperationException(
                $"agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}");
        }

        return parameters;
    }

    private async Task RunTurnAsync(
        string prompt, string conversationId, AiAgentConfiguration config, Dictionary<string, string> parameters)
    {
        var reply = new TelegramStreamingReply(
            _bot.Client, _chatId, _context.Options.Telegram, _context.Logger, _ct);

        try
        {
            await _context.Router.RunAsync(
                new AgentRequest(
                    _context.Database, config.Identifier, conversationId, prompt, _context.ChannelDoc.Id!,
                    parameters.ToDictionary(
                        parameter => parameter.Key,
                        parameter => AgentParameterValue.FromString(parameter.Value))),
                reply.OnChunkAsync, config, _ct);

            await reply.FinalizeAsync();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            await _bot.TrySendPlainAsync(_chatId, _context.Messages.SomethingWentWrong);
            throw;
        }
    }

    private bool IsCommand(string text, string name)
    {
        var separator = text.IndexOfAny([' ', '\t', '\r', '\n']);
        var command = separator < 0 ? text : text[..separator];

        if (command.Equals($"/{name}", StringComparison.OrdinalIgnoreCase))
            return true;

        var username = _context.ChannelDoc.Telegram?.BotUsername;
        return string.IsNullOrEmpty(username) == false &&
               command.Equals($"/{name}@{username}", StringComparison.OrdinalIgnoreCase);
    }

    private async Task HandleContactAsync(Message message)
    {
        var contact = message.Contact!;
        var senderId = message.From?.Id ?? _chatId;

        if (contact.UserId != senderId || string.IsNullOrEmpty(contact.PhoneNumber))
        {
            await RequestContactAsync(_context.Messages.OwnContactRequired);
            return;
        }

        using (var session = _context.Store.OpenAsyncSession(_context.Database))
        {
            await session.StoreAsync(new TelegramLink
            {
                Id = TelegramLink.IdFor(_context.ChannelDoc.ShortId, senderId),
                PhoneNumber = contact.PhoneNumber,
                SharedAt = DateTime.UtcNow,
            }, _ct);
            await session.SaveChangesAsync(_ct);
        }

        await _bot.Client.SendMessage(_chatId,
            _context.Messages.PhoneNumberReceived,
            replyMarkup: new ReplyKeyboardRemove(), cancellationToken: _ct);
    }

    private async Task<string?> LoadPhoneNumberAsync(Message message)
    {
        var senderId = message.From?.Id ?? _chatId;
        using var session = _context.Store.OpenAsyncSession(_context.Database);
        var stored = await session.LoadAsync<TelegramLink>(
            TelegramLink.IdFor(_context.ChannelDoc.ShortId, senderId), _ct);
        return string.IsNullOrEmpty(stored?.PhoneNumber) ? null : stored.PhoneNumber;
    }

    private Task RequestContactAsync(string text) =>
        _bot.Client.SendMessage(_chatId, text,
            replyMarkup: new ReplyKeyboardMarkup(
                KeyboardButton.WithRequestContact(_context.Messages.SharePhoneNumberButton))
            {
                ResizeKeyboard = true,
                OneTimeKeyboard = true,
            },
            cancellationToken: _ct);

    private async Task ClearConversationAsync(string conversationId)
    {
        using var session = _context.Store.OpenAsyncSession(_context.Database);
        session.Delete(conversationId);
        session.Delete(ConversationPreview.IdFor(conversationId));
        await session.SaveChangesAsync(_ct);
    }

    private Task SendPlainAsync(string text) => _bot.Client.SendMessage(_chatId, text, cancellationToken: _ct);
}
