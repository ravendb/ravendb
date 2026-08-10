using System.Globalization;
using System.Threading.Channels;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

internal sealed class TelegramChat
{
    private readonly Channel<Message> _queue;
    private readonly long _chatId;
    private readonly TelegramBotRuntime _bot;
    private readonly TelegramChatContext _context;
    private readonly CancellationToken _ct;

    private int _overloadNotified;

    public TelegramChat(long chatId, TelegramBotRuntime bot, TelegramChatContext context, CancellationToken ct)
    {
        _chatId = chatId;
        _bot = bot;
        _context = context;
        _ct = ct;

        _queue = System.Threading.Channels.Channel.CreateBounded<Message>(
            new BoundedChannelOptions(context.Options.TelegramChatQueueCapacity)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            });

        Completion = Task.Run(RunAsync, CancellationToken.None);
    }

    public Task Completion { get; }

    public bool TryPost(Message message) => _queue.Writer.TryWrite(message);

    public void NotifyOverloadOnce()
    {
        if (Interlocked.Exchange(ref _overloadNotified, 1) != 0)
            return;

        _ = _bot.TrySendPlainAsync(_chatId,
            "I'm still working through your earlier messages, so that one didn't make it. " +
            "Please resend it once I've replied.");
    }

    private async Task RunAsync()
    {
        try
        {
            await foreach (var message in _queue.Reader.ReadAllAsync(_ct))
            {
                await HandleSafeAsync(message);

                if (_queue.Reader.Count == 0)
                    Interlocked.Exchange(ref _overloadNotified, 0);
            }
        }
        catch (OperationCanceledException) when (_ct.IsCancellationRequested)
        {
        }
    }

    private async Task HandleSafeAsync(Message message)
    {
        try
        {
            await HandleMessageAsync(message);
        }
        catch (OperationCanceledException) when (_ct.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            var scrubbed = TelegramSettings.ScrubToken(e.Message, _bot.BotToken);
            _bot.Health.RecordError(DateTime.UtcNow, scrubbed);
            _context.Logger.LogWarning(
                "Telegram message handling failed for channel {ChannelId} chat {ChatId}: {Error}",
                _context.ChannelDoc.Id, _chatId, scrubbed);
        }
    }

    private async Task HandleMessageAsync(Message message)
    {
        var channel = _context.ChannelDoc;
        var prompt = message.Text!.Trim();
        if (prompt.Length == 0)
            return;

        var conversationId = TelegramConversationId.For(_context.ChannelId, _chatId, DateTime.UtcNow);

        if (IsCommand(prompt, "clear"))
        {
            await ClearConversationAsync(conversationId);
            await SendPlainAsync("Conversation cleared. The next message starts a fresh one.");
            return;
        }

        if (IsCommand(prompt, "start"))
        {
            await SendPlainAsync("Hi! Ask me anything and I'll answer. Send /clear anytime to start a fresh conversation.");
            return;
        }

        var config = await AgentLookup.FindAsync(_context.Store, _context.Database, channel.AgentId, _ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{channel.AgentId}' is no longer registered in this app");

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

        foreach (var (name, binding) in _context.ChannelDoc.Telegram!.ParameterBindings)
        {
            switch (binding.Source)
            {
                case TelegramParameterSource.Constant:
                    parameters[name] = binding.Value ?? "";
                    break;

                case TelegramParameterSource.UserId:
                    var userId = message.From?.Id ?? _chatId;
                    parameters[name] = userId.ToString(CultureInfo.InvariantCulture);
                    break;

                case TelegramParameterSource.Username:
                    var username = message.From?.Username;
                    if (string.IsNullOrEmpty(username))
                    {
                        await SendPlainAsync(
                            "This assistant needs your Telegram username. Set one in Telegram Settings and send your message again.");
                        return null;
                    }

                    parameters[name] = username;
                    break;
            }
        }

        var unbound = (config.Parameters ?? [])
            .Select(p => p.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false && parameters.ContainsKey(name) == false)
            .ToArray();
        if (unbound.Length > 0)
        {
            await SendPlainAsync("This assistant is not fully configured yet. Please contact whoever set up this bot.");
            throw new InvalidOperationException(
                $"agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}");
        }

        return parameters;
    }

    private async Task RunTurnAsync(
        string prompt, string conversationId, AiAgentConfiguration config, Dictionary<string, string> parameters)
    {
        var reply = new TelegramStreamingReply(
            _bot.Client, _chatId, _context.Options.TelegramEditDebounce, _bot.BotToken, _context.Logger, _ct);

        try
        {
            var result = await _context.Router.RunAsync(
                new AgentRequest(
                    _context.Database, config.Identifier, conversationId, prompt, _context.ChannelDoc.Id!, parameters),
                reply.OnChunkAsync, config, _ct);

            var fullReply = string.IsNullOrWhiteSpace(result.Reply) ? reply.AccumulatedText : result.Reply;
            await reply.FinalizeAsync(fullReply);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            await _bot.TrySendPlainAsync(_chatId, "Sorry - something went wrong handling that message. Please try again.");
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

    private async Task ClearConversationAsync(string conversationId)
    {
        using var session = _context.Store.OpenAsyncSession(_context.Database);
        session.Delete(conversationId);
        session.Delete(ConversationPreview.IdFor(conversationId));
        await session.SaveChangesAsync(_ct);
    }

    private Task SendPlainAsync(string text) => _bot.Client.SendMessage(_chatId, text, cancellationToken: _ct);
}
