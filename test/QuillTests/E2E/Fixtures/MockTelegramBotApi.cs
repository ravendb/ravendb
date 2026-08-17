using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace QuillTests.E2E.Fixtures;

public sealed class MockTelegramBotApi : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly object _lock = new();

    private readonly Dictionary<string, List<JsonObject>> _updateQueues = new();
    private readonly Dictionary<string, int> _getUpdatesCalls = new();
    private readonly Dictionary<string, long?> _lastOffsets = new();
    private readonly List<SentMessage> _sent = [];
    private readonly List<EditedMessage> _edited = [];
    private readonly List<ChatActionCall> _chatActions = [];
    private readonly Dictionary<(string Token, long ChatId, int MessageId), string> _messageTexts = new();
    private long _nextUpdateId = 1;
    private int _nextMessageId = 1000;

    public sealed record SentMessage(
        string Token, long ChatId, int MessageId, string Text, string? ParseMode, string? ReplyMarkup = null);

    public sealed record EditedMessage(string Token, long ChatId, int MessageId, string Text, string? ParseMode);

    public sealed record ChatActionCall(string Token, long ChatId, string Action);

    public string BaseAddress { get; }

    public string BotUsername { get; set; } = "quill_test_bot";

    public (int Status, string Body)? GetMeFailure { get; set; }

    public (int Status, string Body)? GetUpdatesFailure { get; set; }

    public bool FailParseModeRequests { get; set; }

    public static (int Status, string Body) Unauthorized { get; } =
        (401, """{"ok":false,"error_code":401,"description":"Unauthorized"}""");

    private MockTelegramBotApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public IReadOnlyList<SentMessage> SentMessages
    {
        get { lock (_lock) return _sent.ToArray(); }
    }

    public IReadOnlyList<EditedMessage> EditedMessages
    {
        get { lock (_lock) return _edited.ToArray(); }
    }

    public IReadOnlyList<ChatActionCall> ChatActions
    {
        get { lock (_lock) return _chatActions.ToArray(); }
    }

    public int GetUpdatesCallCount(string token)
    {
        lock (_lock) return _getUpdatesCalls.GetValueOrDefault(token);
    }

    public long? LastGetUpdatesOffset(string token)
    {
        lock (_lock) return _lastOffsets.GetValueOrDefault(token);
    }

    public int PendingUpdateCount(string token)
    {
        lock (_lock) return _updateQueues.TryGetValue(token, out var queue) ? queue.Count : 0;
    }

    public void EnqueueTextMessage(string token, long chatId, long fromUserId, string text,
        string? username = null, string chatType = "private")
    {
        lock (_lock)
        {
            var from = new JsonObject
            {
                ["id"] = fromUserId,
                ["is_bot"] = false,
                ["first_name"] = "Tester",
            };
            if (username is not null)
                from["username"] = username;

            var update = new JsonObject
            {
                ["update_id"] = _nextUpdateId++,
                ["message"] = new JsonObject
                {
                    ["message_id"] = _nextMessageId++,
                    ["from"] = from,
                    ["chat"] = new JsonObject
                    {
                        ["id"] = chatId,
                        ["type"] = chatType,
                    },
                    ["date"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    ["text"] = text,
                },
            };

            if (_updateQueues.TryGetValue(token, out var queue) == false)
                _updateQueues[token] = queue = [];
            queue.Add(update);
        }
    }

    public void EnqueueContactMessage(string token, long chatId, long fromUserId, string phoneNumber,
        long? contactUserId = null, string chatType = "private")
    {
        lock (_lock)
        {
            var update = new JsonObject
            {
                ["update_id"] = _nextUpdateId++,
                ["message"] = new JsonObject
                {
                    ["message_id"] = _nextMessageId++,
                    ["from"] = new JsonObject
                    {
                        ["id"] = fromUserId,
                        ["is_bot"] = false,
                        ["first_name"] = "Tester",
                    },
                    ["chat"] = new JsonObject
                    {
                        ["id"] = chatId,
                        ["type"] = chatType,
                    },
                    ["date"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    ["contact"] = new JsonObject
                    {
                        ["phone_number"] = phoneNumber,
                        ["first_name"] = "Tester",
                        ["user_id"] = contactUserId ?? fromUserId,
                    },
                },
            };

            if (_updateQueues.TryGetValue(token, out var queue) == false)
                _updateQueues[token] = queue = [];
            queue.Add(update);
        }
    }

    public async Task WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (condition() == false)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"MockTelegramBotApi: timed out waiting for {what}");
            await Task.Delay(25);
        }
    }

    public void Reset()
    {
        lock (_lock)
        {
            _updateQueues.Clear();
            _getUpdatesCalls.Clear();
            _lastOffsets.Clear();
            _sent.Clear();
            _edited.Clear();
            _chatActions.Clear();
            _messageTexts.Clear();
        }

        BotUsername = "quill_test_bot";
        GetMeFailure = null;
        GetUpdatesFailure = null;
        FailParseModeRequests = false;
    }

    public static async Task<MockTelegramBotApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        MockTelegramBotApi instance = null!;

        app.MapPost("/bot{token}/{method}", async (string token, string method, HttpContext ctx) =>
        {
            using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
            var raw = await reader.ReadToEndAsync();
            var body = raw.Length > 0 ? JsonNode.Parse(raw) : null;

            return method switch
            {
                "getMe" => instance.HandleGetMe(token),
                "getUpdates" => await instance.HandleGetUpdatesAsync(token, body, ctx.RequestAborted),
                "sendMessage" => instance.HandleSendMessage(token, body),
                "editMessageText" => instance.HandleEditMessageText(token, body),
                "sendChatAction" => instance.HandleSendChatAction(token, body),
                _ => Results.Content(
                    $$"""{"ok":false,"error_code":404,"description":"Not Found: method '{{method}}' is not mocked"}""",
                    "application/json", statusCode: 404),
            };
        });

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockTelegramBotApi failed to bind a port.");

        instance = new MockTelegramBotApi(app, url.TrimEnd('/'));
        return instance;
    }

    private IResult HandleGetMe(string token)
    {
        if (GetMeFailure is { } failure)
            return Results.Content(failure.Body, "application/json", statusCode: failure.Status);

        var result = new JsonObject
        {
            ["id"] = BotIdFor(token),
            ["is_bot"] = true,
            ["first_name"] = "Quill Test Bot",
            ["username"] = BotUsername,
        };

        return Ok(result);
    }

    private async Task<IResult> HandleGetUpdatesAsync(string token, JsonNode? body, CancellationToken aborted)
    {
        var offset = (long?)body?["offset"];
        lock (_lock)
        {
            _getUpdatesCalls[token] = _getUpdatesCalls.GetValueOrDefault(token) + 1;
            _lastOffsets[token] = offset;
        }

        if (GetUpdatesFailure is { } failure)
            return Results.Content(failure.Body, "application/json", statusCode: failure.Status);

        for (var attempt = 0; ; attempt++)
        {
            lock (_lock)
            {
                if (_updateQueues.TryGetValue(token, out var queue))
                {
                    // like real Telegram, an offset permanently discards every update below it
                    if (offset is { } confirmed)
                        queue.RemoveAll(u => (long)u["update_id"]! < confirmed);

                    var pending = queue.Take(100).ToArray();
                    if (pending.Length > 0)
                    {
                        var result = new JsonArray(pending.Select(u => (JsonNode)u.DeepClone()).ToArray());
                        return Ok(result);
                    }
                }
            }

            if (attempt >= 1)
                return Ok(new JsonArray());

            try
            {
                await Task.Delay(150, aborted);
            }
            catch (OperationCanceledException)
            {
                return Ok(new JsonArray());
            }
        }
    }

    private IResult HandleSendMessage(string token, JsonNode? body)
    {
        var chatId = (long)body!["chat_id"]!;
        var text = (string)body["text"]!;
        var parseMode = (string?)body["parse_mode"];
        var replyMarkup = body["reply_markup"]?.ToJsonString();

        if (FailParseModeRequests && parseMode is not null)
            return CantParseEntities();

        int messageId;
        lock (_lock)
        {
            messageId = _nextMessageId++;
            _sent.Add(new SentMessage(token, chatId, messageId, text, parseMode, replyMarkup));
            _messageTexts[(token, chatId, messageId)] = text;
        }

        return Ok(MessageJson(messageId, chatId, text));
    }

    private IResult HandleEditMessageText(string token, JsonNode? body)
    {
        var chatId = (long)body!["chat_id"]!;
        var messageId = (int)body["message_id"]!;
        var text = (string)body["text"]!;
        var parseMode = (string?)body["parse_mode"];

        if (FailParseModeRequests && parseMode is not null)
            return CantParseEntities();

        lock (_lock)
        {
            var key = (token, chatId, messageId);
            if (_messageTexts.TryGetValue(key, out var current) && current == text &&
                (parseMode is null || text.AsSpan().IndexOfAny("*_`[") < 0))
                return MessageNotModified();

            _messageTexts[key] = text;
            _edited.Add(new EditedMessage(token, chatId, messageId, text, parseMode));
        }

        return Ok(MessageJson(messageId, chatId, text));
    }

    private IResult HandleSendChatAction(string token, JsonNode? body)
    {
        var chatId = (long)body!["chat_id"]!;
        var action = (string)body["action"]!;

        lock (_lock)
            _chatActions.Add(new ChatActionCall(token, chatId, action));

        return Ok(JsonValue.Create(true));
    }

    private static IResult Ok(JsonNode result)
    {
        var envelope = new JsonObject { ["ok"] = true, ["result"] = result };
        return Results.Content(envelope.ToJsonString(), "application/json");
    }

    private static IResult CantParseEntities() => Results.Content(
        """{"ok":false,"error_code":400,"description":"Bad Request: can't parse entities"}""",
        "application/json", statusCode: 400);

    private static IResult MessageNotModified() => Results.Content(
        """{"ok":false,"error_code":400,"description":"Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message"}""",
        "application/json", statusCode: 400);

    private static JsonObject MessageJson(int messageId, long chatId, string text) => new()
    {
        ["message_id"] = messageId,
        ["date"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        ["chat"] = new JsonObject { ["id"] = chatId, ["type"] = "private" },
        ["text"] = text,
    };

    public static long BotIdFor(string token)
    {
        var colon = token.IndexOf(':');
        if (colon > 0 && long.TryParse(token[..colon], out var id))
            return id;
        return Math.Abs(token.GetHashCode());
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
