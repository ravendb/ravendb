using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace QuillTests.E2E.Fixtures;

public sealed class MockDiscordApi : IAsyncDisposable
{
    internal const int DirectMessagesIntent = 1 << 12;

    private readonly WebApplication _app;
    private readonly object _lock = new();

    private readonly Dictionary<string, BotEntry> _bots = new();
    private readonly List<SentMessage> _sent = [];
    private readonly List<EditedMessage> _edited = [];
    private readonly List<IdentifyCall> _identifies = [];
    private readonly List<ResumeCall> _resumes = [];
    private readonly List<string> _identityCalls = [];

    private int _heartbeats;
    private int _connects;
    private int _nextMessageId = 1;
    private int _nextSessionId = 1;
    private GatewaySession? _session;

    public sealed record SentMessage(
        string BotToken, string ChannelId, string Content, string MessageId, bool MentionsSuppressed);

    public sealed record EditedMessage(string BotToken, string ChannelId, string MessageId, string Content);

    public sealed record IdentifyCall(string Token, int Intents);

    public sealed record ResumeCall(string Token, string SessionId, long Seq);

    private sealed record BotEntry(string ApplicationId, string BotUserId, string Username);

    private sealed class GatewaySession(WebSocket socket, string sessionId)
    {
        internal readonly WebSocket Socket = socket;
        internal readonly string SessionId = sessionId;
        internal readonly SemaphoreSlim SendLock = new(1, 1);
        internal long Seq;
        internal volatile bool Ready;
    }

    public string BaseAddress { get; }

    public string GatewayUrl { get; }

    public bool Down { get; set; }

    public int? SendErrorStatus { get; set; }

    public bool NextEditRateLimit429 { get; set; }

    public bool SuppressHeartbeatAck { get; set; }

    public bool InvalidateResume { get; set; }

    public int? CloseOnConnect { get; set; }

    public bool StallBeforeHello { get; set; }

    public int? CloseAfterIdentify { get; set; }

    public int? CloseAfterResume { get; set; }

    public int HeartbeatIntervalMs { get; set; } = 30_000;

    private MockDiscordApi(WebApplication app, string baseAddress, string gatewayUrl)
    {
        _app = app;
        BaseAddress = baseAddress;
        GatewayUrl = gatewayUrl;
    }

    public IReadOnlyList<SentMessage> SentMessages
    {
        get { lock (_lock) return _sent.ToArray(); }
    }

    public IReadOnlyList<EditedMessage> EditedMessages
    {
        get { lock (_lock) return _edited.ToArray(); }
    }

    public IReadOnlyList<IdentifyCall> Identifies
    {
        get { lock (_lock) return _identifies.ToArray(); }
    }

    public IReadOnlyList<ResumeCall> Resumes
    {
        get { lock (_lock) return _resumes.ToArray(); }
    }

    public IReadOnlyList<string> IdentityCalls
    {
        get { lock (_lock) return _identityCalls.ToArray(); }
    }

    public int Heartbeats => Volatile.Read(ref _heartbeats);

    public int Connects => Volatile.Read(ref _connects);

    public bool IsConnected => ReadySession is not null;

    public string? CurrentSessionId => ReadySession?.SessionId;

    public void AddBot(string botToken, string applicationId, string botUserId, string username = "quill-bot")
    {
        lock (_lock)
            _bots[botToken] = new BotEntry(applicationId, botUserId, username);
    }

    public void Reset()
    {
        GatewaySession? session;
        lock (_lock)
        {
            _bots.Clear();
            _sent.Clear();
            _edited.Clear();
            _identifies.Clear();
            _resumes.Clear();
            _identityCalls.Clear();
            session = _session;
            _session = null;
        }

        session?.Socket.Abort();

        Volatile.Write(ref _heartbeats, 0);
        Volatile.Write(ref _connects, 0);
        Down = false;
        SendErrorStatus = null;
        NextEditRateLimit429 = false;
        SuppressHeartbeatAck = false;
        InvalidateResume = false;
        CloseOnConnect = null;
        StallBeforeHello = false;
        CloseAfterIdentify = null;
        CloseAfterResume = null;
        HeartbeatIntervalMs = 30_000;
    }

    public Task WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null) =>
        MockApiWait.UntilAsync(nameof(MockDiscordApi), condition, what, timeout);

    public Task WaitUntilAsync(Func<Task<bool>> condition, string what, TimeSpan? timeout = null) =>
        MockApiWait.UntilAsync(nameof(MockDiscordApi), condition, what, timeout);

    public Task WaitUntilConnectedAsync(TimeSpan? timeout = null) =>
        WaitUntilAsync(() => ReadySession is not null, "a ready gateway session", timeout);

    public async Task DispatchDmAsync(
        string messageId, string channelId, string authorId, string content,
        string authorUsername = "dana", bool fromBot = false, string? guildId = null, bool withAttachment = false,
        int messageType = 0)
    {
        await WaitUntilConnectedAsync();
        var session = ReadySession ?? throw new InvalidOperationException("MockDiscordApi: the session went away.");

        var author = new JsonObject { ["id"] = authorId, ["username"] = authorUsername };
        if (fromBot)
            author["bot"] = true;

        var message = new JsonObject
        {
            ["id"] = messageId,
            ["channel_id"] = channelId,
            ["type"] = messageType,
            ["content"] = content,
            ["author"] = author,
            ["attachments"] = withAttachment
                ? new JsonArray(new JsonObject { ["id"] = "attachment-1" })
                : new JsonArray(),
        };

        if (guildId is not null)
            message["guild_id"] = guildId;

        await DispatchAsync(session, "MESSAGE_CREATE", message);
    }

    public async Task CloseCurrentAsync(int code)
    {
        await WaitUntilConnectedAsync();
        var session = ReadySession ?? throw new InvalidOperationException("MockDiscordApi: the session went away.");
        await CloseAsync(session, code);
    }

    public async Task RequestReconnectAsync()
    {
        await WaitUntilConnectedAsync();
        var session = ReadySession ?? throw new InvalidOperationException("MockDiscordApi: the session went away.");
        await SendAsync(session, new JsonObject { ["op"] = 7, ["d"] = null });
    }

    private GatewaySession? ReadySession
    {
        get
        {
            lock (_lock)
                return _session is { Ready: true } session && session.Socket.State == WebSocketState.Open
                    ? session
                    : null;
        }
    }

    public static async Task<MockDiscordApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();
        app.UseWebSockets();

        MockDiscordApi instance = null!;

        app.Use(async (ctx, next) =>
        {
            if (instance.Down && ctx.WebSockets.IsWebSocketRequest == false)
            {
                ctx.Response.StatusCode = 503;
                return;
            }

            await next(ctx);
        });

        app.MapGet("/users/@me", (HttpContext ctx) => instance.HandleCurrentUser(ctx));
        app.MapGet("/oauth2/applications/@me", (HttpContext ctx) => instance.HandleCurrentApplication(ctx));
        app.MapGet("/gateway/bot", (HttpContext ctx) => instance.HandleGatewayBot(ctx));

        app.MapPost("/channels/{channelId}/messages", async (string channelId, HttpContext ctx) =>
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return instance.HandleCreateMessage(ctx, channelId, body);
        });

        app.MapPatch("/channels/{channelId}/messages/{messageId}",
            async (string channelId, string messageId, HttpContext ctx) =>
            {
                var body = await JsonNode.ParseAsync(ctx.Request.Body);
                return instance.HandleEditMessage(ctx, channelId, messageId, body);
            });

        app.Map("/gateway", (HttpContext ctx) => instance.HandleGatewayAsync(ctx));

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockDiscordApi failed to bind a port.");

        var httpBase = url.TrimEnd('/');
        instance = new MockDiscordApi(app, httpBase, "ws" + httpBase["http".Length..] + "/gateway");
        return instance;
    }

    private async Task HandleGatewayAsync(HttpContext ctx)
    {
        if (ctx.WebSockets.IsWebSocketRequest == false)
        {
            ctx.Response.StatusCode = 400;
            return;
        }

        using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
        Interlocked.Increment(ref _connects);

        string sessionId;
        lock (_lock)
            sessionId = string.Create(CultureInfo.InvariantCulture, $"session-{_nextSessionId++}");

        var session = new GatewaySession(socket, sessionId);
        lock (_lock)
            _session = session;

        try
        {
            await RunSessionAsync(session, ctx.RequestAborted);
        }
        catch (Exception)
        {
        }
        finally
        {
            lock (_lock)
            {
                if (ReferenceEquals(_session, session))
                    _session = null;
            }
        }
    }

    private async Task RunSessionAsync(GatewaySession session, CancellationToken ct)
    {
        if (CloseOnConnect is { } forced)
        {
            await CloseAsync(session, forced);
            return;
        }

        if (StallBeforeHello)
        {
            await Task.Delay(Timeout.Infinite, ct);
            return;
        }

        await SendAsync(session, new JsonObject
        {
            ["op"] = 10,
            ["d"] = new JsonObject { ["heartbeat_interval"] = HeartbeatIntervalMs },
        });

        var handshake = await ReceiveAsync(session, ct);
        if (handshake is null)
            return;

        var op = handshake["op"]?.GetValue<int>() ?? -1;
        var data = handshake["d"];

        if (op == 2)
        {
            var token = data?["token"]?.GetValue<string>() ?? "";
            var intents = data?["intents"]?.GetValue<int>() ?? 0;

            lock (_lock)
                _identifies.Add(new IdentifyCall(token, intents));

            if (CloseAfterIdentify is { } forcedAfterIdentify)
            {
                await CloseAsync(session, forcedAfterIdentify);
                return;
            }

            if (BotFor(token) is null)
            {
                await CloseAsync(session, 4004);
                return;
            }

            if ((intents & DirectMessagesIntent) == 0)
            {
                await CloseAsync(session, 4014);
                return;
            }

            await DispatchAsync(session, "READY", new JsonObject
            {
                ["session_id"] = session.SessionId,
                ["resume_gateway_url"] = GatewayUrl,
            });
        }
        else if (op == 6)
        {
            var token = data?["token"]?.GetValue<string>() ?? "";
            var resumedSession = data?["session_id"]?.GetValue<string>() ?? "";
            var seq = data?["seq"]?.GetValue<long>() ?? -1;

            lock (_lock)
                _resumes.Add(new ResumeCall(token, resumedSession, seq));

            if (CloseAfterResume is { } forcedAfterResume)
            {
                await CloseAsync(session, forcedAfterResume);
                return;
            }

            if (InvalidateResume)
            {
                await SendAsync(session, new JsonObject { ["op"] = 9, ["d"] = false });
                return;
            }

            await DispatchAsync(session, "RESUMED", new JsonObject());
        }
        else
        {
            return;
        }

        session.Ready = true;

        while (true)
        {
            var frame = await ReceiveAsync(session, ct);
            if (frame is null)
                return;

            if (frame["op"]?.GetValue<int>() != 1)
                continue;

            Interlocked.Increment(ref _heartbeats);

            if (SuppressHeartbeatAck == false)
                await SendAsync(session, new JsonObject { ["op"] = 11, ["d"] = null });
        }
    }

    private Task DispatchAsync(GatewaySession session, string name, JsonNode? data) =>
        SendAsync(session, new JsonObject
        {
            ["op"] = 0,
            ["t"] = name,
            ["s"] = Interlocked.Increment(ref session.Seq),
            ["d"] = data,
        });

    private static async Task SendAsync(GatewaySession session, JsonNode frame)
    {
        var bytes = Encoding.UTF8.GetBytes(frame.ToJsonString());

        await session.SendLock.WaitAsync();
        try
        {
            if (session.Socket.State == WebSocketState.Open)
                await session.Socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }
        finally
        {
            session.SendLock.Release();
        }
    }

    private static async Task CloseAsync(GatewaySession session, int code)
    {
        await session.SendLock.WaitAsync();
        try
        {
            if (session.Socket.State == WebSocketState.Open)
                await session.Socket.CloseOutputAsync(
                    (WebSocketCloseStatus)code, $"closed with {code}", CancellationToken.None);
        }
        catch (Exception)
        {
        }
        finally
        {
            session.SendLock.Release();
        }
    }

    private static async Task<JsonNode?> ReceiveAsync(GatewaySession session, CancellationToken ct)
    {
        var buffer = new byte[8 * 1024];
        using var frame = new MemoryStream();

        while (true)
        {
            WebSocketReceiveResult result;
            try
            {
                result = await session.Socket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
            }
            catch (Exception)
            {
                return null;
            }

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            frame.Write(buffer, 0, result.Count);
            if (result.EndOfMessage == false)
                continue;

            try
            {
                return JsonNode.Parse(Encoding.UTF8.GetString(frame.GetBuffer(), 0, (int)frame.Length));
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }

    private IResult HandleCurrentUser(HttpContext ctx)
    {
        var token = BotToken(ctx);

        BotEntry? entry;
        lock (_lock)
        {
            _identityCalls.Add(token);
            entry = _bots.GetValueOrDefault(token);
        }

        if (entry is null)
            return DiscordError(401, "401: Unauthorized");

        return Results.Json(new JsonObject
        {
            ["id"] = entry.BotUserId,
            ["username"] = entry.Username,
            ["bot"] = true,
        });
    }

    private IResult HandleCurrentApplication(HttpContext ctx)
    {
        var entry = BotFor(BotToken(ctx));
        if (entry is null)
            return DiscordError(401, "401: Unauthorized");

        return Results.Json(new JsonObject { ["id"] = entry.ApplicationId, ["name"] = "Quill" });
    }

    private IResult HandleGatewayBot(HttpContext ctx)
    {
        var entry = BotFor(BotToken(ctx));
        if (entry is null)
            return DiscordError(401, "401: Unauthorized");

        return Results.Json(new JsonObject
        {
            ["url"] = GatewayUrl,
            ["shards"] = 1,
        });
    }

    private IResult HandleCreateMessage(HttpContext ctx, string channelId, JsonNode? body)
    {
        var token = BotToken(ctx);
        if (BotFor(token) is null)
            return DiscordError(401, "401: Unauthorized");

        if (SendErrorStatus is { } status)
            return DiscordError(status, $"{status}: send refused by the test");

        var content = body?["content"]?.GetValue<string>() ?? "";
        var suppressed = body?["allowed_mentions"]?["parse"] is JsonArray { Count: 0 };

        string messageId;
        lock (_lock)
        {
            messageId = string.Create(CultureInfo.InvariantCulture, $"msg-{_nextMessageId++}");
            _sent.Add(new SentMessage(token, channelId, content, messageId, suppressed));
        }

        return Results.Json(new JsonObject
        {
            ["id"] = messageId,
            ["channel_id"] = channelId,
            ["content"] = content,
        });
    }

    private IResult HandleEditMessage(HttpContext ctx, string channelId, string messageId, JsonNode? body)
    {
        var token = BotToken(ctx);
        if (BotFor(token) is null)
            return DiscordError(401, "401: Unauthorized");

        if (NextEditRateLimit429)
        {
            NextEditRateLimit429 = false;
            return Results.Json(
                new JsonObject
                {
                    ["message"] = "You are being rate limited.",
                    ["retry_after"] = 0.05,
                    ["global"] = false,
                },
                statusCode: 429);
        }

        if (SendErrorStatus is { } status)
            return DiscordError(status, $"{status}: edit refused by the test");

        var content = body?["content"]?.GetValue<string>() ?? "";

        lock (_lock)
        {
            if (_sent.Any(m => m.MessageId == messageId) == false)
                return DiscordError(404, "10008: Unknown Message");

            _edited.Add(new EditedMessage(token, channelId, messageId, content));
        }

        return Results.Json(new JsonObject
        {
            ["id"] = messageId,
            ["channel_id"] = channelId,
            ["content"] = content,
        });
    }

    private BotEntry? BotFor(string botToken)
    {
        lock (_lock)
            return _bots.GetValueOrDefault(botToken);
    }

    private static string BotToken(HttpContext ctx)
    {
        var header = ctx.Request.Headers.Authorization.ToString();
        return header.StartsWith("Bot ", StringComparison.Ordinal) ? header["Bot ".Length..] : "";
    }

    private static IResult DiscordError(int status, string message) =>
        Results.Json(new JsonObject { ["message"] = message, ["code"] = 0 }, statusCode: status);

    public async ValueTask DisposeAsync() => await _app.DisposeAsync();
}
