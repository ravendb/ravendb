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

public sealed class MockSlackApi : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly object _lock = new();

    private readonly Dictionary<string, BotEntry> _bots = new();
    private readonly HashSet<string> _appTokens = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string?> _userEmails = new();
    private readonly List<string> _userInfoCalls = [];
    private readonly List<SentMessage> _sent = [];
    private readonly List<EditedMessage> _edited = [];
    private readonly List<string> _authTestCalls = [];
    private readonly List<string> _socketOpenCalls = [];
    private readonly List<string> _acks = [];
    private int _nextTs = 1;
    private int _nextEnvelope = 1;
    private int _connects;
    private SocketSession? _session;

    public sealed record SentMessage(string BotToken, string Channel, string Text, string Ts, string? Parse);

    public sealed record EditedMessage(string BotToken, string Channel, string Ts, string Text, string? Parse);

    private sealed record BotEntry(string TeamId, string TeamName, string BotUserId, string BotId);

    private sealed class SocketSession(WebSocket socket)
    {
        public WebSocket Socket { get; } = socket;
        public SemaphoreSlim SendLock { get; } = new(1, 1);
        public volatile bool Ready;
    }

    public string BaseAddress { get; }

    public string SocketUrl { get; }

    public bool Down { get; set; }

    public string? SendError { get; set; }

    public bool NextUpdateRateLimit429 { get; set; }

    public bool UsersReadScopeGranted { get; set; } = true;

    public string? SocketOpenError { get; set; }

    public TimeSpan? SocketOpenRetryAfter { get; set; }

    private MockSlackApi(WebApplication app, string baseAddress, string socketUrl)
    {
        _app = app;
        BaseAddress = baseAddress;
        SocketUrl = socketUrl;
    }

    public IReadOnlyList<SentMessage> SentMessages
    {
        get { lock (_lock) return _sent.ToArray(); }
    }

    public IReadOnlyList<EditedMessage> EditedMessages
    {
        get { lock (_lock) return _edited.ToArray(); }
    }

    public IReadOnlyList<string> AuthTestCalls
    {
        get { lock (_lock) return _authTestCalls.ToArray(); }
    }

    public IReadOnlyList<string> UserInfoCalls
    {
        get { lock (_lock) return _userInfoCalls.ToArray(); }
    }

    public IReadOnlyList<string> SocketOpenCalls
    {
        get { lock (_lock) return _socketOpenCalls.ToArray(); }
    }

    public IReadOnlyList<string> Acks
    {
        get { lock (_lock) return _acks.ToArray(); }
    }

    public int Connects => Volatile.Read(ref _connects);

    public bool IsConnected => ReadySession is not null;

    // a user known to Slack but with no email on their profile is registered with a null email
    public void AddUser(string userId, string? email)
    {
        lock (_lock)
            _userEmails[userId] = email;
    }

    public void AddBot(string botToken, string teamId, string teamName, string botUserId, string botId = "B0MOCK")
    {
        lock (_lock)
            _bots[botToken] = new BotEntry(teamId, teamName, botUserId, botId);
    }

    public void AddAppToken(string appToken)
    {
        lock (_lock)
            _appTokens.Add(appToken);
    }

    public void RemoveAppToken(string appToken)
    {
        lock (_lock)
            _appTokens.Remove(appToken);
    }

    public void Reset()
    {
        SocketSession? session;
        lock (_lock)
        {
            _bots.Clear();
            _appTokens.Clear();
            _sent.Clear();
            _edited.Clear();
            _authTestCalls.Clear();
            _userEmails.Clear();
            _userInfoCalls.Clear();
            _socketOpenCalls.Clear();
            _acks.Clear();
            session = _session;
            _session = null;
        }

        session?.Socket.Abort();

        Volatile.Write(ref _connects, 0);
        Down = false;
        SendError = null;
        NextUpdateRateLimit429 = false;
        UsersReadScopeGranted = true;
        SocketOpenError = null;
        SocketOpenRetryAfter = null;
    }

    public Task WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null) =>
        MockApiWait.UntilAsync(nameof(MockSlackApi), condition, what, timeout);

    public Task WaitUntilAsync(Func<Task<bool>> condition, string what, TimeSpan? timeout = null) =>
        MockApiWait.UntilAsync(nameof(MockSlackApi), condition, what, timeout);

    public Task WaitUntilConnectedAsync(TimeSpan? timeout = null) =>
        WaitUntilAsync(() => ReadySession is not null, "a ready Socket Mode session", timeout);

    public async Task<string> DispatchEventAsync(string teamId, string eventId, JsonNode message)
    {
        var envelopeId = await DispatchEnvelopeAsync(new JsonObject
        {
            ["type"] = "events_api",
            ["accepts_response_payload"] = false,
            ["retry_attempt"] = 0,
            ["retry_reason"] = "",
            ["payload"] = new JsonObject
            {
                ["type"] = "event_callback",
                ["team_id"] = teamId,
                ["event_id"] = eventId,
                ["event"] = message,
            },
        });
        return envelopeId;
    }

    public async Task<string> DispatchEnvelopeAsync(JsonObject envelope)
    {
        await WaitUntilConnectedAsync();
        var session = ReadySession ?? throw new InvalidOperationException("MockSlackApi: the session went away.");

        string envelopeId;
        lock (_lock)
            envelopeId = string.Create(CultureInfo.InvariantCulture, $"env-{_nextEnvelope++}");

        envelope["envelope_id"] = envelopeId;
        await SendAsync(session, envelope);
        return envelopeId;
    }

    public async Task SendDisconnectAsync(string reason)
    {
        await WaitUntilConnectedAsync();
        var session = ReadySession ?? throw new InvalidOperationException("MockSlackApi: the session went away.");
        await SendAsync(session, new JsonObject { ["type"] = "disconnect", ["reason"] = reason });
        await CloseAsync(session);
    }

    public async Task CloseCurrentAsync()
    {
        await WaitUntilConnectedAsync();
        var session = ReadySession ?? throw new InvalidOperationException("MockSlackApi: the session went away.");
        await CloseAsync(session);
    }

    private SocketSession? ReadySession
    {
        get
        {
            lock (_lock)
                return _session is { Ready: true } session && session.Socket.State == WebSocketState.Open
                    ? session
                    : null;
        }
    }

    public static async Task<MockSlackApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();
        app.UseWebSockets();

        MockSlackApi instance = null!;

        app.Use(async (ctx, next) =>
        {
            if (instance.Down && ctx.WebSockets.IsWebSocketRequest == false)
            {
                ctx.Response.StatusCode = 503;
                return;
            }
            await next(ctx);
        });

        app.MapPost("/auth.test", (HttpContext ctx) => instance.HandleAuthTest(ctx));

        app.MapPost("/apps.connections.open", (HttpContext ctx) => instance.HandleConnectionsOpen(ctx));

        app.MapGet("/users.info", (HttpContext ctx) => instance.HandleUserInfo(ctx));

        app.MapPost("/chat.postMessage", async (HttpContext ctx) =>
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return instance.HandlePostMessage(ctx, body);
        });

        app.MapPost("/chat.update", async (HttpContext ctx) =>
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return instance.HandleUpdate(ctx, body);
        });

        app.Map("/socket", (HttpContext ctx) => instance.HandleSocketAsync(ctx));

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockSlackApi failed to bind a port.");

        var httpBase = url.TrimEnd('/');
        instance = new MockSlackApi(app, httpBase, "ws" + httpBase["http".Length..] + "/socket");
        return instance;
    }

    private IResult HandleAuthTest(HttpContext ctx)
    {
        var token = BearerToken(ctx);
        BotEntry? entry;
        lock (_lock)
        {
            _authTestCalls.Add(token);
            entry = _bots.GetValueOrDefault(token);
        }

        if (entry is null)
            return SlackError("invalid_auth");

        return Results.Json(new JsonObject
        {
            ["ok"] = true,
            ["url"] = $"https://{entry.TeamName.ToLowerInvariant()}.slack.com/",
            ["team"] = entry.TeamName,
            ["user"] = "quill-bot",
            ["team_id"] = entry.TeamId,
            ["user_id"] = entry.BotUserId,
            ["bot_id"] = entry.BotId,
        });
    }

    private IResult HandleConnectionsOpen(HttpContext ctx)
    {
        var token = BearerToken(ctx);
        bool known;
        lock (_lock)
        {
            _socketOpenCalls.Add(token);
            known = _appTokens.Contains(token);
        }

        if (SocketOpenError is { } error)
        {
            if (SocketOpenRetryAfter is { } retryAfter)
                ctx.Response.Headers.RetryAfter = ((int)retryAfter.TotalSeconds).ToString(CultureInfo.InvariantCulture);
            return SlackError(error, error == "ratelimited" ? 429 : 200);
        }

        if (known == false)
            return SlackError("invalid_auth");

        return Results.Json(new JsonObject
        {
            ["ok"] = true,
            ["url"] = $"{SocketUrl}?ticket={Guid.NewGuid():N}&app_id=A0MOCK",
        });
    }

    private async Task HandleSocketAsync(HttpContext ctx)
    {
        if (ctx.WebSockets.IsWebSocketRequest == false)
        {
            ctx.Response.StatusCode = 400;
            return;
        }

        using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
        Interlocked.Increment(ref _connects);

        var session = new SocketSession(socket);
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

    private async Task RunSessionAsync(SocketSession session, CancellationToken ct)
    {
        await SendAsync(session, new JsonObject
        {
            ["type"] = "hello",
            ["num_connections"] = 1,
            ["connection_info"] = new JsonObject { ["app_id"] = "A0MOCK" },
        });

        session.Ready = true;

        while (true)
        {
            var frame = await ReceiveAsync(session, ct);
            if (frame is null)
                return;

            if (frame["envelope_id"]?.GetValue<string>() is { Length: > 0 } envelopeId)
            {
                lock (_lock)
                    _acks.Add(envelopeId);
            }
        }
    }

    private static async Task SendAsync(SocketSession session, JsonNode frame)
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

    private static async Task CloseAsync(SocketSession session)
    {
        await session.SendLock.WaitAsync();
        try
        {
            if (session.Socket.State == WebSocketState.Open)
                await session.Socket.CloseOutputAsync(
                    WebSocketCloseStatus.NormalClosure, "closed by the test", CancellationToken.None);
        }
        catch (Exception)
        {
        }
        finally
        {
            session.SendLock.Release();
        }
    }

    private static async Task<JsonNode?> ReceiveAsync(SocketSession session, CancellationToken ct)
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

    private IResult HandleUserInfo(HttpContext ctx)
    {
        BotEntry? entry;
        lock (_lock)
            entry = _bots.GetValueOrDefault(BearerToken(ctx));

        if (entry is null)
            return SlackError("invalid_auth");

        if (UsersReadScopeGranted == false)
            return SlackError("missing_scope");

        var userId = ctx.Request.Query["user"].ToString();

        bool known;
        string? email;
        lock (_lock)
        {
            _userInfoCalls.Add(userId);
            known = _userEmails.TryGetValue(userId, out email);
        }

        if (known == false)
            return SlackError("user_not_found");

        var profile = new JsonObject();
        if (email is not null)
            profile["email"] = email;

        return Results.Json(new JsonObject
        {
            ["ok"] = true,
            ["user"] = new JsonObject
            {
                ["id"] = userId,
                ["team_id"] = entry.TeamId,
                ["profile"] = profile,
            },
        });
    }

    private IResult HandlePostMessage(HttpContext ctx, JsonNode? body)
    {
        BotEntry? entry;
        lock (_lock)
            entry = _bots.GetValueOrDefault(BearerToken(ctx));

        if (entry is null)
            return SlackError("invalid_auth");

        if (SendError is { } error)
            return SlackError(error);

        var channel = body?["channel"]?.GetValue<string>() ?? "";
        var text = body?["text"]?.GetValue<string>() ?? "";
        var parse = body?["parse"]?.GetValue<string>();

        string ts;
        lock (_lock)
        {
            ts = string.Create(CultureInfo.InvariantCulture, $"1700000000.{_nextTs++:D6}");
            _sent.Add(new SentMessage(BearerToken(ctx), channel, text, ts, parse));
        }

        return Results.Json(new JsonObject { ["ok"] = true, ["channel"] = channel, ["ts"] = ts });
    }

    private IResult HandleUpdate(HttpContext ctx, JsonNode? body)
    {
        BotEntry? entry;
        lock (_lock)
            entry = _bots.GetValueOrDefault(BearerToken(ctx));

        if (entry is null)
            return SlackError("invalid_auth");

        if (NextUpdateRateLimit429)
        {
            NextUpdateRateLimit429 = false;
            ctx.Response.Headers.RetryAfter = "1";
            return Results.Json(new JsonObject { ["ok"] = false, ["error"] = "ratelimited" }, statusCode: 429);
        }

        if (SendError is { } error)
            return SlackError(error);

        var channel = body?["channel"]?.GetValue<string>() ?? "";
        var ts = body?["ts"]?.GetValue<string>() ?? "";
        var text = body?["text"]?.GetValue<string>() ?? "";
        var parse = body?["parse"]?.GetValue<string>();

        lock (_lock)
        {
            if (_sent.Any(m => m.Ts == ts) == false)
                return SlackError("message_not_found");

            _edited.Add(new EditedMessage(BearerToken(ctx), channel, ts, text, parse));
        }

        return Results.Json(new JsonObject { ["ok"] = true, ["channel"] = channel, ["ts"] = ts });
    }

    private static string BearerToken(HttpContext ctx)
    {
        var header = ctx.Request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.Ordinal) ? header["Bearer ".Length..] : "";
    }

    private static IResult SlackError(string error, int statusCode = 200) =>
        Results.Json(new JsonObject { ["ok"] = false, ["error"] = error }, statusCode: statusCode);

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
